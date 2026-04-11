import requests
import pytz
import pandas as pd
import numpy as np
import io
import re
import asyncio
import nest_asyncio
import os
import base64
import smtplib
import gspread
import json
from datetime import timedelta, datetime
from collections import Counter
from email.message import EmailMessage
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from urllib.parse import quote
from playwright.async_api import async_playwright

# تفعيل nest_asyncio
nest_asyncio.apply()

# --- إعدادات الحماية (GitHub Secrets) ---
API_KEY         = os.environ.get('ZENPUT_API_KEY')
APP_PASSWORD    = os.environ.get('GMAIL_APP_PASSWORD')
GOOGLE_JSON_STR = os.environ.get('GOOGLE_CREDENTIALS') 

# إعدادات الإيميل
SENDER_EMAIL    = "mohamed.hegazy010091@gmail.com"
RECIPIENTS_TO   = ["Mohamed.hegazy8555@gmail.com"]
RECIPIENTS_CC   = ["m.hejazi@aofgroup.com"]

# إعدادات المشروع الأساسية
TEMPLATE_ID     = 659312
TZ              = pytz.timezone("Asia/Baghdad")
GOOGLE_SHEET_ID = "1bestuz83Y-6o470OHF-J8dx6CxE4J9goJcj6jnOx5Ds"
MAX_RECORDS     = 5000

FIELDS = {
    "SHAWARMA_COLOR": 11183491, 
    "SHAWARMA_QUALITY": 11183493,
    "SHAWARMA_SIZE": 11183495,
    "TARABESH_COLOR": 11707959, 
    "TARABESH_QUALITY": 11707961,
    "TARABESH_SIZE": 11707963,
    "ARABI_COLOR": 11183499,
    "ARABI_QUALITY": 11183501,
    "ARABI_SIZE": 11183503
}

# --- HELPERS (الجزء الأول) ---
def zenput_headers():
    return {"X-API-TOKEN": API_KEY, "Accept": "application/json"}

def get_zenput_signed_url(s3_path):
    if not s3_path: return ""
    storage_api_url = f"https://www.zenput.com/api/v2/users/current/storage/?path={quote(s3_path)}"
    try:
        response = requests.get(storage_api_url, headers=zenput_headers(), timeout=10)
        if response.status_code == 200:
            return response.json().get('data', {}).get('location', "")
        else:
            return f"https://www.zenput.com/api/v3/files/download?s3_key={s3_path}"
    except: return ""

def format_milliseconds_to_hms(ms_value):
    try:
        if ms_value is None or ms_value == "" or float(ms_value) < 0: return "00:00:00"
        total_seconds = int(float(ms_value) / 1000)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except: return "00:00:00"

def parse_zenput_value(val):
    if isinstance(val, list) and len(val) > 0:
        if isinstance(val[0], dict) and "s3_key" in val[0]:
            signed_links = []
            for item in val:
                s3_key = item.get('s3_key')
                if s3_key:
                    real_link = get_zenput_signed_url(s3_key)
                    if real_link: signed_links.append(real_link)
            if not signed_links: return ""
            if len(signed_links) == 1: return f'=HYPERLINK("{signed_links[0]}", "View Photo")'
            return "\n".join(signed_links)
        return ", ".join(str(v) for v in val)
    val_str = str(val).strip().lower()
    if val_str == "true": return "Yes"
    if val_str == "false": return "No"
    return str(val).strip() if val is not None else ""

# --- FETCH & PROCESS (الجزء الأول) ---
def fetch_submissions_dynamic(template_id):
    all_submissions = []
    start = 0
    limit = 50
    today_str = datetime.now(TZ).strftime("%Y-%m-%d") 
    print(f"🚀 Starting Extraction Task (Target Date: {today_str})")
    while len(all_submissions) < MAX_RECORDS:
        params = {"form_template_id": template_id, "limit": limit, "offset": start, "date_submitted_start": today_str}
        resp = requests.get("https://www.zenput.com/api/v3/submissions/", headers=zenput_headers(), params=params)
        if resp.status_code != 200: break
        batch = resp.json().get("data", [])
        if not batch: break
        for s in batch:
            meta = s.get("smetadata") or {}
            date_raw = meta.get("date_submitted_local", "")
            if date_raw and date_raw.startswith(today_str): all_submissions.append(s)
        start += limit
        if len(batch) < limit: break
    print(f"✅ Total submissions retrieved: {len(all_submissions)}")
    return all_submissions

def process_quality_bread_submissions_to_df(submissions):
    if not submissions: return pd.DataFrame()
    rows = []
    for s in submissions:
        meta = s.get("smetadata") or {}
        answers = s.get("answers") or []
        ans_dict = {str(a.get("field_id")): a for a in answers if isinstance(a, dict)}
        location_obj = meta.get("location") or {}
        location_name = location_obj.get("name", "")
        external_key = location_obj.get("external_key", "")
        sub_id = s.get("id", "")
        legacy_sub_id = s.get("legacy_submission_id", "")
        lat, lon = meta.get("lat", ""), meta.get("lon", "")

        def get_val_with_quality_check(field_id):
            ans = ans_dict.get(str(field_id), {})
            raw_val = ans.get("value")
            display_val = parse_zenput_value(raw_val)
            if display_val == "Yes": return "Yes"
            for i, a in enumerate(answers):
                if str(a.get("field_id")) == str(field_id):
                    for j in range(i + 1, min(i + 4, len(answers))):
                        curr_field = answers[j]
                        if curr_field.get("field_type") in ["photo", "image"]:
                            p_data = curr_field.get("value") or curr_field.get("image_value")
                            if isinstance(p_data, list) and len(p_data) > 0:
                                img_meta = p_data[0]
                                var = img_meta.get('laplacian_variance', 999)
                                if var < 5 or img_meta.get('is_low_quality') is True: return "Yes"
                                else: return "No"
            return display_val

        def get_photo_after_id(field_id):
            for i, ans in enumerate(answers):
                if str(ans.get("field_id")) == str(field_id):
                    for j in range(i + 1, min(i + 4, len(answers))):
                        if answers[j].get("title") == "Photo" or answers[j].get("field_type") == "photo":
                            photo_val = answers[j].get("value")
                            if isinstance(photo_val, list) and len(photo_val) > 0:
                                s3_key = photo_val[0].get('s3_key')
                                if s3_key: return get_zenput_signed_url(s3_key)
            return ""

        def get_notes_by_id(field_id):
            for i, ans in enumerate(answers):
                if str(ans.get("field_id")) == str(field_id):
                    for j in range(i + 1, min(i + 4, len(answers))):
                        if answers[j].get("title") == "Photo" or answers[j].get("field_type") == "photo":
                            return answers[j].get("notes", "")
            return ""

        row_data = {
            "Location": location_name, "Location External Key": external_key,
            "Submitted By": meta.get("created_by", {}).get("display_name") if isinstance(meta.get("created_by"), dict) else meta.get("created_by", ""),
            "Date Submitted": meta.get("date_submitted_local", ""),
            "Is the bread color good?": get_val_with_quality_check(FIELDS["SHAWARMA_COLOR"]),
            "Photo": get_photo_after_id(FIELDS["SHAWARMA_COLOR"]),
            "Photo Notes": get_notes_by_id(FIELDS["SHAWARMA_COLOR"]),
            "Quality of shawarma bread good?": get_val_with_quality_check(FIELDS["SHAWARMA_QUALITY"]),
            "Photo2": get_photo_after_id(FIELDS["SHAWARMA_QUALITY"]),
            "Photo Notes3": get_notes_by_id(FIELDS["SHAWARMA_QUALITY"]),
            "Size of bread good? as our stander": get_val_with_quality_check(FIELDS["SHAWARMA_SIZE"]),
            "Photo4": get_photo_after_id(FIELDS["SHAWARMA_SIZE"]),
            "Photo Notes5": get_notes_by_id(FIELDS["SHAWARMA_SIZE"]),
            "Is the bread color good?6": get_val_with_quality_check(FIELDS["TARABESH_COLOR"]),
            "Photo7": get_photo_after_id(FIELDS["TARABESH_COLOR"]),
            "Photo Notes8": get_notes_by_id(FIELDS["TARABESH_COLOR"]),
            "Quality of tarabesh bread good?": get_val_with_quality_check(FIELDS["TARABESH_QUALITY"]),
            "Photo9": get_photo_after_id(FIELDS["TARABESH_QUALITY"]),
            "Photo Notes10": get_notes_by_id(FIELDS["TARABESH_QUALITY"]),
            "Size of bread good? as our stander11": get_val_with_quality_check(FIELDS["TARABESH_SIZE"]),
            "Photo12": get_photo_after_id(FIELDS["TARABESH_SIZE"]),
            "Photo Notes13": get_notes_by_id(FIELDS["TARABESH_SIZE"]),
            "Is the bread color good?14": get_val_with_quality_check(FIELDS["ARABI_COLOR"]),
            "Photo15": get_photo_after_id(FIELDS["ARABI_COLOR"]),
            "Photo Notes16": get_notes_by_id(FIELDS["ARABI_COLOR"]),
            "Quality of Arabi bread good?": get_val_with_quality_check(FIELDS["ARABI_QUALITY"]),
            "Photo17": get_photo_after_id(FIELDS["ARABI_QUALITY"]),
            "Photo Notes18": get_notes_by_id(FIELDS["ARABI_QUALITY"]),
            "Size of bread good?19": get_val_with_quality_check(FIELDS["ARABI_SIZE"]),
            "Photo20": get_photo_after_id(FIELDS["ARABI_SIZE"]),
            "Photo Notes21": get_notes_by_id(FIELDS["ARABI_SIZE"]),
            "Project": "Quality Bread", "Distance from Location": str(round(float(meta.get("distance_to_account") or 0), 2)),
            "Time to Complete": format_milliseconds_to_hms(meta.get("time_to_complete")),
            "Timezone": meta.get("time_zone", ""),
            "Location Map": f'=HYPERLINK("http://maps.google.com/?q={lat},{lon}", "View Map")' if lat and lon else "",
            "Submission Legacy Id": s.get("legacy_submission_id", ""), "Submission Id": s.get("id", ""),
            "Submission Link": f'=HYPERLINK("https://www.zenput.com/reports/#form_id/{legacy_sub_id}", "View Form")' if legacy_sub_id else "",
            "PDF": f'=HYPERLINK("https://www.zenput.com/submission/{sub_id}/pdf/", "Download PDF")' if sub_id else "",
            "Task Opened At": meta.get("date_created", ""), "Time Opened": meta.get("date_created", "")[11:19] if meta.get("date_created") else ""
        }
        rows.append(row_data)
    return pd.DataFrame(rows)

# --- إعدادات الجزء الثاني (HTML TEMPLATE) ---
html_template = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>لوحة تحكم مراقبة الجودة المتقدمة</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Tajawal:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #f8fafc; --card-color: #ffffff; --border-color: #e5e7eb;
            --text-primary: #1f2937; --text-secondary: #6b7280;
            --color-orange: #f97316; --color-orange-light: #fff7ed;
            --color-orange-dark: #c2410c; --color-red: #ef4444;
        }
        body { font-family: 'Tajawal', sans-serif; background-color: var(--bg-color); color: var(--text-primary); margin: 0; padding: 20px; }
        @media print {
            body { background-color: white; }
            .card { page-break-inside: avoid !important; break-inside: avoid !important; margin-bottom: 20px; box-shadow: none !important; border: 1px solid #eee !important; }
            tr { page-break-inside: avoid !important; break-inside: avoid !important; }
            thead { display: table-header-group; }
            #dashboard-to-export { width: 100% !important; padding: 0 !important; }
        }
        #dashboard-to-export { padding: 10px; background-color: var(--bg-color); width: 1100px; margin: auto; }
        .dashboard-container { display: grid; grid-template-columns: repeat(12, 1fr); gap: 20px; }
        .header { grid-column: 1 / -1; margin-bottom: 16px; }
        .header h1 { margin: 0; font-size: 2.25rem; font-weight: 700; }
        .header p { margin: 4px 0 0; font-size: 1.1rem; color: var(--text-secondary); }
        .card { background-color: var(--card-color); border: 1px solid var(--border-color); border-radius: 12px; padding: 24px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); }
        .card.kpi { grid-column: span 3; }
        .card.chart { grid-column: span 6; }
        .card.table-card { grid-column: span 12; }
        .card-title { font-size: 1.1rem; font-weight: 600; color: var(--text-secondary); margin: 0 0 16px 0; }
        .kpi .value { font-size: 2.5rem; font-weight: 700; margin: 0; }
        .kpi .value .icon { font-size: 1.5rem; vertical-align: middle; margin-right: 8px; }
        .value.orange { color: var(--color-orange); }
        .value.red { color: var(--color-red); }
        .bar-item { display: flex; align-items: center; margin-bottom: 12px; font-size: 0.9rem; }
        .bar-label { width: 35%; white-space: nowrap; color: var(--text-secondary); padding-left: 10px; }
        .bar-wrapper { flex-grow: 1; background-color: #f3f4f6; border-radius: 6px; height: 24px; }
        .bar { height: 100%; background: linear-gradient(90deg, var(--color-orange), #fdba74); border-radius: 6px; display: flex; align-items: center; justify-content: flex-start; color: #fff; font-weight: 700; font-size: 0.8rem; padding-right: 8px; box-sizing: border-box; }
        .table-wrapper { width: 100%; overflow: auto; border: 1px solid var(--border-color); border-radius: 8px; }
        table { width: 100%; border-collapse: collapse; text-align: right; }
        th, td { padding: 12px 16px; font-size: 0.9rem; border-bottom: 1px solid var(--border-color); vertical-align: middle; }
        thead { background-color: #f1f5f9; }
        th { font-weight: 700; color: var(--text-secondary); }
        td .status-badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 500; font-size: 0.8rem; }
        td .status-badge.quality { background-color: #fee2e2; color: #b91c1c; }
        td .status-badge.size { background-color: #ffedd5; color: #9a3412; }
        td .status-badge.color { background-color: #dbeafe; color: #1e40af; }
        .photo-container { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
        .issue-photo { width: 80px; height: 80px; object-fit: cover; border-radius: 8px; border: 2px solid var(--border-color); }
    </style>
</head>
<body>
    <div id="dashboard-to-export">
        <div class="dashboard-container">
            <header class="header">
                <h1>لوحة تحكم مراقبة الجودة</h1>
                <p>تحليل لـ {{total_reports}} تقرير جودة حديث</p>
            </header>
            <div class="card kpi">
                <h2>معدل الجودة العام</h2>
                <p class="value">{{quality_rate}}% <span class="icon">✅</span></p>
            </div>
            <div class="card kpi">
                <h2>إجمالي المشاكل</h2>
                <p class="value red">{{total_issues}} <span class="icon">🚩</span></p>
            </div>
            <div class="card kpi">
                <h2>الفروع المتأثرة</h2>
                <p class="value orange">{{branches_with_issues}} <span class="icon">🏢</span></p>
            </div>
            <div class="card kpi">
                <h2>إجمالي التقارير</h2>
                <p class="value">{{total_reports}} <span class="icon">📋</span></p>
            </div>
            <div class="card chart">
                <h2 class="card-title">المشاكل حسب الفرع</h2>
                {{branch_issues_bars}}
            </div>
            <div class="card chart">
                <h2 class="card-title">المشاكل حسب نوع المنتج</h2>
                {{product_issues_bars}}
            </div>
            <div class="card table-card">
                <h2 class="card-title">سجل المشاكل التفصيلي</h2>
                <div class="table-wrapper">
                    <table>
                        <thead><tr><th>الفرع</th><th>المنتج</th><th>فئة المشكلة</th><th>المشكلة / ملاحظة</th><th>صورة المشكلة</th></tr></thead>
                        <tbody>{{issues_table_rows}}</tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

# --- HELPERS (الجزء الثاني) ---
def get_image_as_base64(url):
    if not isinstance(url, str) or not url.startswith('http'): 
        match = re.search(r'HYPERLINK\("([^"]+)"', url)
        if match: url = match.group(1)
        else: return None
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, timeout=15, headers=headers)
        if response.status_code == 200:
            encoded_string = base64.b64encode(response.content).decode('utf-8')
            return f"data:{response.headers.get('Content-Type', 'image/jpeg')};base64,{encoded_string}"
    except: pass
    return None

def get_metric_from_question(question):
    q_lower = str(question).lower()
    if 'color' in q_lower: return 'اللون'
    if 'quality' in q_lower: return 'الجودة'
    if 'size' in q_lower: return 'الحجم'
    return 'غير محدد'

def create_bar_chart_html(data_counter, max_items=5):
    if not data_counter: return "<p>لا توجد بيانات لعرضها.</p>"
    top_items = data_counter.most_common(max_items)
    max_value = top_items[0][1] if top_items else 1
    html = ""
    for item, count in top_items:
        percentage = (count / max_value) * 100
        html += f'<div class="bar-item"><div class="bar-label">{item}</div><div class="bar-wrapper"><div class="bar" style="width: {percentage}%;">{count}</div></div></div>'
    return html

async def export_to_pdf(html_path, pdf_path):
    async with async_playwright() as p:
        browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
        page = await browser.new_page()
        await page.set_viewport_size({"width": 1200, "height": 800})
        abs_path = f"file://{os.path.abspath(html_path)}"
        await page.goto(abs_path, wait_until="networkidle", timeout=90000)
        await page.evaluate("document.fonts.ready")
        await page.pdf(
            path=pdf_path, format="A4", print_background=True, 
            landscape=True, margin={"top":"15mm","bottom":"15mm","left":"10mm","right":"10mm"}
        )
        await browser.close()

def send_final_email(pdf_path, stats, issues_list):
    msg = EmailMessage()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    
    # تحضير ملخص المشاكل لكل منتج بشكل ديناميكي
    # سيقوم الكود بتجميع المقاييس (لون، حجم، جودة) لكل منتج ظهرت فيه مشكلة
    summary_data = {}
    for i in issues_list:
        p = i['product']
        m = i['metric']
        if p not in summary_data:
            summary_data[p] = set()
        summary_data[p].add(m)
    
    # بناء نص المشاكل (مثال: خبز الشاورما: مشاكل في اللون والحجم)
    issues_text = ""
    for product, metrics in summary_data.items():
        metrics_str = " و ".join(list(metrics))
        issues_text += f"- {product}: مشاكل في {metrics_str}\n"
    
    if not issues_text:
        issues_text = "- لا توجد ملاحظات جوهرية لهذا اليوم."

    msg['Subject'] = f'📊 تقرير جودة الخبز بالفروع - {today}'
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENTS_TO)
    msg['Cc'] = ", ".join(RECIPIENTS_CC)

    # نص الإيميل الرسمي والمنسق بـ HTML بسيط جداً للحفاظ على الرسمية
    email_body = f"""
    <html>
    <body dir="rtl" style="font-family: Arial, sans-serif; line-height: 1.6; color: #000;">
        <p>السادة/ إدارة المشتريات،</p>
        
        <p>مرفق لسيادتكم تقرير جودة أنواع الخبز بالفروع ليوم {today}.</p>
        
        <p>نود الإشارة إلى ملاحظة تكرار بعض المشكلات المتعلقة بجودة أنواع الخبز خلال الفترة الأخيرة، وهو ما قد يؤثر على مستوى الخدمة المقدمة بالفروع.</p>
        
        <p><b>وتتمثل ملاحظات اليوم فيما يلي:</b></p>
        <div style="margin-right: 20px;">
            {issues_text.replace('\n', '<br>')}
        </div>
        
        <p>نأمل من سيادتكم التكرم بمراجعة هذه الملاحظات، والتفضل باتخاذ ما ترونه مناسبًا من إجراءات لضمان تحسين الجودة والحد من تكرار هذه المشكلات.</p>
        
        <p>وتفضلوا بقبول فائق الاحترام والتقدير،،</p>
        
        <p style="margin-top: 30px; font-size: 0.9em; color: #555;">
            مرسل آلياً | نظام مراقبة الجودة
        </p>
    </body>
    </html>
    """

    msg.add_alternative(email_body, subtype='html')

    # إرفاق ملف الـ PDF
    with open(pdf_path, 'rb') as f:
        msg.add_attachment(
            f.read(), 
            maintype='application', 
            subtype='pdf', 
            filename=os.path.basename(pdf_path)
        )

    # تنفيذ عملية الإرسال
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, APP_PASSWORD)
        smtp.send_message(msg)

# --- التنفيذ النهائي (MAIN EXECUTION) ---
async def main():
    try:
        # 1. جلب ومعالجة البيانات
        data_raw = fetch_submissions_dynamic(TEMPLATE_ID)
        final_df = process_quality_bread_submissions_to_df(data_raw)
        
        if final_df.empty:
            print("⚠️ No data found for the specified date.")
            return

        # تحديث شيت جوجل باستخدام المصادقة الآلية
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_JSON_STR), scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(GOOGLE_SHEET_ID).get_worksheet(0)
        sheet.clear()
        set_with_dataframe(sheet, final_df, row=1, col=1, include_index=False, include_column_header=True)
        print("🎉 Google Sheet updated successfully!")

        # 2. تحليل المشاكل
        print("⏳ جاري تحليل المشاكل وتجهيز التقرير الـ PDF...")
        product_groups = ['خبز شاورما', 'خبز طرابيش', 'خبز عربي']
        all_checks, issues = [], []
        col_list = list(final_df.columns)
        question_cols = [col for col in col_list if '?' in str(col)]

        for _, row in final_df.iterrows():
            branch = row['Location']
            for i, q_col_name in enumerate(question_cols):
                product = product_groups[(i // 3) % 3]
                metric = get_metric_from_question(q_col_name)
                answer = row.get(q_col_name)
                if pd.isna(answer): continue
                answer_str = str(answer).strip().lower()
                is_issue = (answer_str != 'yes')
                all_checks.append(is_issue)
                if is_issue:
                    q_idx = col_list.index(q_col_name)
                    raw_photo_val = str(row.get(col_list[q_idx + 1], ''))
                    photo_urls = re.findall(r'https?://[^\s"]+', raw_photo_val)
                    notes = row.get(col_list[q_idx + 2], '')
                    issues.append({
                        'branch': branch, 'product': product, 'metric': metric, 
                        'problem': str(answer) if answer_str != 'no' else f"{metric} غير جيد", 
                        'notes': str(notes) if pd.notna(notes) else '', 'photo_urls': photo_urls
                    })

        # بناء صفوف الجدول
        table_rows_html = ""
        for issue in issues:
            photo_html = '<div class="photo-container">'
            for url in issue['photo_urls']:
                b64 = get_image_as_base64(url)
                if b64: photo_html += f'<img src="{b64}" class="issue-photo">'
            photo_html += '</div>' if issue['photo_urls'] else 'لا توجد صورة'
            category_map = {'اللون': 'color', 'الجودة': 'quality', 'الحجم': 'size'}
            badge = category_map.get(issue['metric'], 'availability')
            table_rows_html += f"<tr><td>{issue['branch']}</td><td>{issue['product']}</td><td><span class='status-badge {badge}'>{issue['metric']}</span></td><td>{issue['problem']}<br><small>{issue['notes']}</small></td><td>{photo_html}</td></tr>"

        # حساب النسب وحقن البيانات
        quality_rate = ((len(all_checks) - len(issues)) / len(all_checks) * 100) if all_checks else 100
        final_html = html_template.replace('{{total_reports}}', str(len(final_df)))\
                                  .replace('{{quality_rate}}', f"{quality_rate:.1f}")\
                                  .replace('{{total_issues}}', str(len(issues)))\
                                  .replace('{{branches_with_issues}}', str(len(set(i['branch'] for i in issues))))\
                                  .replace('{{branch_issues_bars}}', create_bar_chart_html(Counter(i['branch'] for i in issues)))\
                                  .replace('{{product_issues_bars}}', create_bar_chart_html(Counter(i['product'] for i in issues)))\
                                  .replace('{{issues_table_rows}}', table_rows_html)

        # حفظ ومعالجة PDF
        with open('report.html', 'w', encoding='utf-8') as f: f.write(final_html)
        print("📡 جاري تحويل HTML إلى PDF...")
        current_date = datetime.now(TZ).strftime("%Y-%m-%d")
        pdf_name = f'Bread_Quality_Report_{current_date}.pdf'
        await export_to_pdf('report.html', pdf_name)
        # Send email
        send_final_email(pdf_name, {'rate': round(quality_rate, 1), 'issues': len(issues)}, issues)
        print("✅ تم استخراج التقرير بنجاح وتحديث الشيت وإرسال الإيميل!")

    except Exception as e:
        print(f"❌ حدث خطأ أثناء التشغيل: {e}")

if __name__ == "__main__":
    asyncio.run(main())
