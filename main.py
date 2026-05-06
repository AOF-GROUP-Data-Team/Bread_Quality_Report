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
import time
from datetime import timedelta, datetime
from collections import Counter
from email.message import EmailMessage
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from urllib.parse import quote
from playwright.async_api import async_playwright
from PIL import Image

# Set nest_asyncio for GitHub Actions
nest_asyncio.apply()

# --- GitHub Secrets ---
API_KEY         = os.environ.get('ZENPUT_API_KEY')
APP_PASSWORD    = os.environ.get('GMAIL_APP_PASSWORD')
GOOGLE_JSON_STR = os.environ.get('GOOGLE_CREDENTIALS') 

# SENDER_EMAIL 
SENDER_EMAIL    = "aof.group.auto@gmail.com"
RECIPIENTS_TO   = ["o.salahaddin@aofgroup.com"]
RECIPIENTS_CC   = ["m.hejazi@aofgroup.com","a.alsalem@aofgroup.com"]

# Project Settings
TEMPLATE_ID     = 659312
TZ              = pytz.timezone("Asia/Baghdad")
GOOGLE_SHEET_ID = "1bestuz83Y-6o470OHF-J8dx6CxE4J9goJcj6jnOx5Ds"
MAX_RECORDS     = 5000

# -----------------------------------------------
# FIELD ID MAP
# -----------------------------------------------

# --- Shawarma Bread ---
FID_SHAWARMA_COLOR         = 11183491
FID_SHAWARMA_COLOR_PHOTO   = 11183492
FID_SHAWARMA_QUALITY       = 11183493
FID_SHAWARMA_QUALITY_PHOTO = 11183494
FID_SHAWARMA_SIZE          = 11183495
FID_SHAWARMA_SIZE_PHOTO    = 11183496

# --- Arabi Bread ---
FID_ARABI_COLOR            = 11183499
FID_ARABI_COLOR_PHOTO      = 11183500
FID_ARABI_QUALITY          = 11183501
FID_ARABI_QUALITY_PHOTO    = 11183502
FID_ARABI_SIZE             = 11183503
FID_ARABI_SIZE_PHOTO       = 11183504

# --- Tarabesh Bread  ---
FID_TARABESH_COLOR         = 11707959
FID_TARABESH_COLOR_PHOTO   = 11707960
FID_TARABESH_QUALITY       = 11707961
FID_TARABESH_QUALITY_PHOTO = 11707962
FID_TARABESH_SIZE          = 11707963
FID_TARABESH_SIZE_PHOTO    = 11707964

# ---------------- HELPERS ----------------
def zenput_headers():
    return {
        "X-API-TOKEN": API_KEY,
        "Accept": "application/json"
    }

def safe_api_get(url, params, retries=3):
    """GET with retry logic. Returns response or None on total failure."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=zenput_headers(), params=params, timeout=15)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                print(f"⏳ Rate limited, waiting 15s... (attempt {attempt + 1}/{retries})")
                time.sleep(15)
            else:
                print(f"❌ API Error {resp.status_code} on attempt {attempt + 1}: {resp.text[:200]}")
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt + 1}/{retries}")
        except Exception as e:
            print(f"⚠️ Request error on attempt {attempt + 1}: {e}")
    return None

def get_zenput_signed_url(s3_path):
    if not s3_path:
        return ""
    storage_api_url = f"https://www.zenput.com/api/v2/users/current/storage/?path={quote(s3_path)}"
    try:
        response = requests.get(storage_api_url, headers=zenput_headers(), timeout=10)
        if response.status_code == 200:
            return response.json().get('data', {}).get('location', "")
        else:
            return f"https://www.zenput.com/api/v3/files/download?s3_key={s3_path}"
    except Exception as e:
        print(f"⚠️ Failed to sign URL for {s3_path}: {e}")
        return ""

def format_milliseconds_to_hms(ms_value):
    try:
        if ms_value is None or ms_value == "" or float(ms_value) < 0:
            return "00:00:00"
        total_seconds = int(float(ms_value) / 1000)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception:
        return "00:00:00"

def parse_zenput_value(val):
    if isinstance(val, list) and len(val) > 0:
        if isinstance(val[0], dict) and "s3_key" in val[0]:
            signed_links = []
            for item in val:
                s3_key = item.get('s3_key')
                if s3_key:
                    real_link = get_zenput_signed_url(s3_key)
                    if real_link:
                        signed_links.append(real_link)
            if not signed_links: return ""
            return "\n".join(signed_links)
        return ", ".join(str(v) for v in val)
    val_str = str(val).strip().lower()
    if val_str == "true": return "Yes"
    if val_str == "false": return "No"
    return str(val).strip() if val is not None else ""

def get_photo_link(ans_by_id, field_id):
    """Get a raw signed URL from an image field_id."""
    ans = ans_by_id.get(field_id)
    if not ans:
        return ""
    photo_val = ans.get("value")
    if not isinstance(photo_val, list) or len(photo_val) == 0:
        return ""
    s3_key = photo_val[0].get("s3_key")
    if not s3_key:
        return ""
    return get_zenput_signed_url(s3_key)

def get_photo_notes(ans_by_id, field_id):
    """Get the notes attached to a photo answer."""
    ans = ans_by_id.get(field_id)
    if not ans:
        return ""
    return ans.get("notes", "") or ""

def get_value(ans_by_id, field_id):
    """Get the raw value of any answer by field_id."""
    ans = ans_by_id.get(field_id)
    if not ans:
        return None
    return ans.get("value")

# ---------------- FETCH DATA ----------------
def fetch_submissions_dynamic(template_id):
    all_submissions = []
    start = 0
    limit = 100
    now = datetime.now(TZ)
    today_str = now.strftime("%Y-%m-%d")
    print(f"🚀 Starting Extraction Task (Today: {today_str})")

    while len(all_submissions) < MAX_RECORDS:
        params = {
            "form_template_id": template_id,
            "limit": limit,
            "offset": start,
            "date_submitted_start": today_str
        }

        resp = safe_api_get("https://www.zenput.com/api/v3/submissions/", params)
        if resp is None:
            print("❌ All retries failed. Stopping pagination.")
            break

        batch = resp.json().get("data", [])
        if not batch:
            break

        stop_early = False
        for s in batch:
            meta = s.get("smetadata") or {}
            date_raw = meta.get("date_submitted_local", "")

            if date_raw and not date_raw.startswith(today_str):
                print(f"⚠️ Hit old record ({date_raw[:10]}), stopping pagination early.")
                stop_early = True
                break

            all_submissions.append(s)

        if stop_early:
            break

        start += limit
        if len(batch) < limit:
            break

    print(f"✅ Total valid submissions retrieved: {len(all_submissions)}")
    return all_submissions

# ---------------- PROCESS DATA ----------------
def process_quality_bread_submissions_to_df(submissions):
    if not submissions:
        return pd.DataFrame()

    rows = []
    print("⚙️ Processing rows and signing photo URLs...")

    for s in submissions:
        try:
            meta = s.get("smetadata") or {}
            if not isinstance(meta, dict):
                meta = {}

            answers = s.get("answers") or []
            if not isinstance(answers, list):
                answers = []

            ans_by_id = {
                a.get("field_id"): a
                for a in answers
                if isinstance(a, dict) and a.get("field_id") is not None
            }

            location_obj  = meta.get("location") or {}
            location_name = location_obj.get("name", "") if isinstance(location_obj, dict) else str(location_obj)
            external_key  = location_obj.get("external_key", "") if isinstance(location_obj, dict) else ""

            sub_id        = s.get("id", "")
            legacy_sub_id = s.get("legacy_submission_id", "")
            lat           = meta.get("lat", "")
            lon           = meta.get("lon", "")

            row_data = {
                "Location":              location_name,
                "Location External Key": external_key,
                "Submitted By": (
                    meta.get("created_by", {}).get("display_name")
                    if isinstance(meta.get("created_by"), dict)
                    else meta.get("created_by", "")
                ),
                "Date Submitted": meta.get("date_submitted_local", ""),

                # --- Shawarma Bread ---
                "Is the bread color good?":           parse_zenput_value(get_value(ans_by_id, FID_SHAWARMA_COLOR)),
                "Photo":                              get_photo_link(ans_by_id, FID_SHAWARMA_COLOR_PHOTO),
                "Photo Notes":                        get_photo_notes(ans_by_id, FID_SHAWARMA_COLOR_PHOTO),
                "Quality of shawarma bread good?":    parse_zenput_value(get_value(ans_by_id, FID_SHAWARMA_QUALITY)),
                "Photo2":                             get_photo_link(ans_by_id, FID_SHAWARMA_QUALITY_PHOTO),
                "Photo Notes3":                       get_photo_notes(ans_by_id, FID_SHAWARMA_QUALITY_PHOTO),
                "Size of bread good? as our stander": parse_zenput_value(get_value(ans_by_id, FID_SHAWARMA_SIZE)),
                "Photo4":                             get_photo_link(ans_by_id, FID_SHAWARMA_SIZE_PHOTO),
                "Photo Notes5":                       get_photo_notes(ans_by_id, FID_SHAWARMA_SIZE_PHOTO),

                # --- Tarabesh Bread ---
                "Is the bread color good?6":             parse_zenput_value(get_value(ans_by_id, FID_TARABESH_COLOR)),
                "Photo7":                                get_photo_link(ans_by_id, FID_TARABESH_COLOR_PHOTO),
                "Photo Notes8":                          get_photo_notes(ans_by_id, FID_TARABESH_COLOR_PHOTO),
                "Quality of tarabesh bread good?":       parse_zenput_value(get_value(ans_by_id, FID_TARABESH_QUALITY)),
                "Photo9":                                get_photo_link(ans_by_id, FID_TARABESH_QUALITY_PHOTO),
                "Photo Notes10":                         get_photo_notes(ans_by_id, FID_TARABESH_QUALITY_PHOTO),
                "Size of bread good? as our stander11":  parse_zenput_value(get_value(ans_by_id, FID_TARABESH_SIZE)),
                "Photo12":                               get_photo_link(ans_by_id, FID_TARABESH_SIZE_PHOTO),
                "Photo Notes13":                         get_photo_notes(ans_by_id, FID_TARABESH_SIZE_PHOTO),

                # --- Arabi Bread ---
                "Is the bread color good?14":            parse_zenput_value(get_value(ans_by_id, FID_ARABI_COLOR)),
                "Photo15":                               get_photo_link(ans_by_id, FID_ARABI_COLOR_PHOTO),
                "Photo Notes16":                         get_photo_notes(ans_by_id, FID_ARABI_COLOR_PHOTO),
                "Quality of Arabi bread good?":          parse_zenput_value(get_value(ans_by_id, FID_ARABI_QUALITY)),
                "Photo17":                               get_photo_link(ans_by_id, FID_ARABI_QUALITY_PHOTO),
                "Photo Notes18":                         get_photo_notes(ans_by_id, FID_ARABI_QUALITY_PHOTO),
                "Size of bread good? as our stander19":  parse_zenput_value(get_value(ans_by_id, FID_ARABI_SIZE)),
                "Photo20":                               get_photo_link(ans_by_id, FID_ARABI_SIZE_PHOTO),
                "Photo Notes21":                         get_photo_notes(ans_by_id, FID_ARABI_SIZE_PHOTO),

                # --- Meta Data ---
                "Project": (
                    meta.get("task", {}).get("title")
                    if isinstance(meta.get("task"), dict)
                    else (
                        meta.get("project", {}).get("title", "Quality Bread")
                        if isinstance(meta.get("project"), dict)
                        else "Quality Bread"
                    )
                ),
                "Distance from Location": str(round(float(meta.get("distance_to_account") or 0), 2)),
                "Time to Complete":        format_milliseconds_to_hms(meta.get("time_to_complete")),
                "Timezone":                meta.get("time_zone", ""),
                "Location Map": (
                    f'=HYPERLINK("http://maps.google.com/?q={lat},{lon}", "View Map")'
                    if lat and lon else ""
                ),
                "Submission Legacy Id": legacy_sub_id,
                "Submission Id":        sub_id,
                "Submission Link": (
                    f'=HYPERLINK("https://www.zenput.com/reports/#form_id/{legacy_sub_id}", "View Form")'
                    if legacy_sub_id else ""
                ),
                "PDF": (
                    f'=HYPERLINK("https://www.zenput.com/submission/{sub_id}/pdf/", "Download PDF")'
                    if sub_id else ""
                ),
                "Task Opened At": meta.get("date_created", ""),
                "Time Opened":    meta.get("date_created", "")[11:19] if meta.get("date_created") else "",
            }

            rows.append(row_data)

        except Exception as e:
            print(f"⚠️ Error processing submission {s.get('id', 'unknown')}: {e}")
            continue

    print(f"✅ Processed {len(rows)} rows successfully.")
    return pd.DataFrame(rows)


# =====================================================================
# --- HTML Template ---
# =====================================================================

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
        body { font-family: 'Tajawal', sans-serif; background-color: var(--bg-color); color: var(--text-primary); margin: 0; padding: 0; }
        
        /* FIX 1: Removed 1100px width. Now it perfectly fits A4 Landscape width */
        #dashboard-to-export { padding: 15px; background-color: var(--bg-color); width: 1100px; margin: auto; box-sizing: border-box; }        
        .dashboard-container { display: grid; grid-template-columns: repeat(12, 1fr); gap: 8px; }
        .header { grid-column: 1 / -1; margin-bottom: 10px; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; }
        .header h1 { margin: 0; font-size: 2rem; font-weight: 700; }
        .header p { margin: 4px 0 0; font-size: 1rem; color: var(--text-secondary); }
        
        .card { background-color: var(--card-color); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05), 0 2px 4px -1px rgba(0,0,0,0.04); grid-column: span 12; box-sizing: border-box; }
        @media (min-width: 640px) { .card.kpi { grid-column: span 6; } }
        @media (min-width: 1024px) { .card.kpi { grid-column: span 3; } .card.chart { grid-column: span 6; } }
        
        .card-title { font-size: 1rem; font-weight: 600; color: var(--text-secondary); margin: 0 0 4px 0; white-space: nowrap; }
        .card.kpi { display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; padding: 10px 15px !important; }
        .kpi .value { font-size: 2rem; font-weight: 700; margin: 0; }
        
        .value.orange { color: var(--color-orange); }
        .value.red { color: var(--color-red); }
        
        .bar-item { display: flex; align-items: center; margin-bottom: 10px; font-size: 0.85rem; }
        .bar-label { width: 35%; white-space: nowrap; color: var(--text-secondary); padding-left: 10px; }
        .bar-wrapper { flex-grow: 1; background-color: #f3f4f6; border-radius: 6px; height: 22px; }
        .bar { height: 100%; background: linear-gradient(90deg, var(--color-orange), #fdba74); border-radius: 6px; display: flex; align-items: center; justify-content: flex-start; color: #fff; font-weight: 700; font-size: 0.8rem; padding-right: 8px; box-sizing: border-box; }
        
        .table-wrapper { width: 100%; overflow-x: auto; border: 1px solid var(--border-color); border-radius: 8px; }
        table { width: 100%; border-collapse: collapse; text-align: right; }
        th, td { padding: 10px 14px; font-size: 0.85rem; border-bottom: 1px solid var(--border-color); vertical-align: middle; }
        thead { background-color: var(--bg-color); }
        th { font-weight: 700; color: var(--text-secondary); }
        tbody tr:last-child td { border-bottom: none; }
        
        td .status-badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 500; font-size: 0.8rem; }
        td .status-badge.quality { background-color: #fee2e2; color: #b91c1c; }
        td .status-badge.size { background-color: #ffedd5; color: #9a3412; }
        td .status-badge.color { background-color: #dbeafe; color: #1e40af; }
        
        .photo-container { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
        .issue-photo { width: 70px; height: 70px; object-fit: cover; border-radius: 8px; border: 2px solid var(--border-color); }

        /* FIX 2: Strict Print Rules to stop ugly page breaks */
        @media print {
            body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
            .card { 
                page-break-inside: avoid !important; 
                break-inside: avoid !important; 
                box-shadow: none !important; 
                border: 1px solid #ddd !important; 
                margin-bottom: 15px !important;
            }
            .card.kpi, .card.chart { page-break-inside: avoid !important; break-inside: avoid !important; }
            tr { page-break-inside: avoid !important; break-inside: avoid !important; }
            thead { display: table-header-group; }
        }
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
                <p class="value">{{quality_rate}}%</p>
            </div>
            <div class="card kpi">
                <h2>إجمالي المشاكل</h2>
                <p class="value red">{{total_issues}}</p>
            </div>
            <div class="card kpi">
                <h2>الفروع التي بها مشاكل</h2>
                <p class="value orange">{{branches_with_issues}}</p>
            </div>
            <div class="card kpi">
                <h2>إجمالي التقارير</h2>
                <p class="value">{{total_reports}}</p>
            </div>
            <div class="card chart">
                <h2 class="card-title">المشاكل حسب الفرع</h2>
                {{branch_issues_bars}}
            </div>
            <div class="card chart">
                <h2 class="card-title">المشاكل حسب نوع المنتج</h2>
                {{product_issues_bars}}
            </div>
            <div class="card">
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

# --- Helper Functions (Dashboard Logic) ---

def is_black_image(image_bytes, threshold=125):
    """Returns True if the image is mostly black (average brightness < threshold)."""
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("L")
        avg = sum(img.getdata()) / (img.width * img.height)
        return avg < threshold
    except:
        return False

def get_image_as_base64(url):
    """Downloads an image and converts it to a Base64 encoded string."""
    if not isinstance(url, str) or not url.startswith('http'):
        return None

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    try:
        response = requests.get(url, timeout=15, headers=headers)
        if response.status_code == 200:
            if is_black_image(response.content):
                print(f"🚫 Skipped black image (pixel-based detection)")
                return None
            encoded_string = base64.b64encode(response.content).decode('utf-8')
            content_type = response.headers.get('Content-Type', 'image/jpeg')
            return f"data:{content_type};base64,{encoded_string}"
        else:
            print(f"Failed to download image. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Could not download image. Error: {e}")
    return None

def get_metric_from_question(question):
    """Extracts just the metric from the question string."""
    q_lower = str(question).lower()
    if 'color' in q_lower: return 'اللون'
    if 'quality' in q_lower: return 'الجودة'
    if 'size' in q_lower: return 'الحجم'
    return 'غير محدد'

def create_bar_chart_html(data_counter, max_items=5):
    """Generates HTML for a bar chart from a Counter object."""
    if not data_counter: return "<p>لا توجد بيانات لعرضها.</p>"
    top_items = data_counter.most_common(max_items)
    max_value = top_items[0][1] if top_items else 1
    html = ""
    for item, count in top_items:
        percentage = (count / max_value) * 100
        html += f'<div class="bar-item"><div class="bar-label">{item}</div><div class="bar-wrapper"><div class="bar" style="width: {percentage}%;">{count}</div></div></div>'
    return html

def process_spreadsheet_v2(df):
    """Correctly processes the DataFrame using column position to map products."""
    product_groups = ['خبز شاورما', 'خبز طرابيش', 'خبز عربي']
    num_products = len(product_groups)
    all_checks, issues = [], []
    col_list = list(df.columns)
    question_cols = [col for col in col_list if '?' in str(col)]

    for row_index, row in df.iterrows():
        branch = row['Submitted By']
        for i, q_col_name in enumerate(question_cols):
            product_index = (i // 3) % num_products
            product = product_groups[product_index]
            metric = get_metric_from_question(q_col_name)
            answer = row.get(q_col_name)
            if pd.isna(answer): continue
            answer_str = str(answer).strip().lower()
            is_issue = (answer_str != 'yes')
            all_checks.append({'product': product, 'metric': metric, 'is_issue': is_issue})

            if is_issue:
                q_col_idx = col_list.index(q_col_name)
                photo_col_idx = q_col_idx + 1
                notes_col_idx = q_col_idx + 2
                photo_urls_raw = row.get(col_list[photo_col_idx], '') if photo_col_idx < len(col_list) else ''
                notes = row.get(col_list[notes_col_idx], '') if notes_col_idx < len(col_list) else ''
                photo_urls = re.findall(r'https?://[^\s"]+', str(photo_urls_raw))
                
                if not photo_urls: continue
                problem_description = str(answer) if answer_str != 'no' else f"{metric} غير جيد"
                issues.append({
                    'branch': branch, 'product': product, 'metric': metric,
                    'problem': problem_description, 'notes': str(notes) if pd.notna(notes) else '',
                    'photo_urls': photo_urls
                })
    return all_checks, issues

# --- Email Function ---
def send_final_email(pdf_path, stats, issues_list):
    msg = EmailMessage()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    
    # 1. Compile a list of issues to determine whether there are actually any problems 
    summary_data = {}
    for i in issues_list:
        p = i['product']
        m = i['metric']
        if p not in summary_data: summary_data[p] = set()
        summary_data[p].add(m)
    
    has_issues = bool(summary_data) # Check if there are any problems or not

    # 2. Formatting the problem text
    issues_text = ""
    if has_issues:
        for product, metrics in summary_data.items():
            metrics_str = " و ".join(list(metrics))
            issues_text += f"- {product}: مشاكل في {metrics_str}<br>"
    else:
        issues_text = "- لا توجد ملاحظات جوهرية لهذا اليوم."

    msg['Subject'] = f'📊 تقرير جودة الخبز بالفروع - {today}'
    msg['From'] = f"Business Intelligence <{SENDER_EMAIL}>"
    msg['To'] = ", ".join(RECIPIENTS_TO)
    msg['Cc'] = ", ".join(RECIPIENTS_CC)

    # 3. Conditional block (appears only if has_issues = True)
    action_paragraph = ""
    if has_issues:
        action_paragraph = "<p>نأمل من سيادتكم التكرم بمراجعة هذه الملاحظات، والتفضل باتخاذ ما ترونه مناسبًا من إجراءات لضمان تحسين الجودة والحد من تكرار هذه المشكلات.</p>"

    # 4. Create the HTML with bold text and a uniform font size (14px) applied to the entire body
    email_body = f"""
    <html>
    <body dir="rtl" style="font-family: Arial, sans-serif; font-size: 14px; font-weight: bold; line-height: 1.8; color: #000;">
        
        <p>السادة/ إدارة المشتريات،</p>
        
        <p>مرفق لسيادتكم تقرير جودة أنواع الخبز بالفروع ليوم {today}.</p>
        
        <p>نود الإشارة إلى ملاحظة تكرار بعض المشكلات المتعلقة بجودة أنواع الخبز خلال الفترة الأخيرة، وهو ما قد يؤثر على مستوى الخدمة المقدمة بالفروع.</p>
        
        <p>وتتمثل ملاحظات اليوم فيما يلي:</p>
        <div style="margin-right: 20px;">{issues_text}</div>
        
        {action_paragraph}
        
        <p>وتفضلوا بقبول فائق الاحترام والتقدير،</p>
        
    </body>
    </html>
    """

    msg.add_alternative(email_body, subtype='html')

    # 5. Attach the PDF file
    with open(pdf_path, 'rb') as f:
        msg.add_attachment(f.read(), maintype='application', subtype='pdf', filename=os.path.basename(pdf_path))

    # 6. Execute the transmission process
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, APP_PASSWORD)
        smtp.send_message(msg)

# ---------------- Main Run ----------------
async def main():
    try:
        # 1. Data Collection and Processing
        data     = fetch_submissions_dynamic(TEMPLATE_ID)
        final_df = process_quality_bread_submissions_to_df(data)
        
        if final_df.empty:
            print("⚠️ No data found for the specified date.")
            return

        # 2. Update Google Sheet using automated authentication (Service Account)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_JSON_STR), scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(GOOGLE_SHEET_ID).get_worksheet(0)
        sheet.clear()
        set_with_dataframe(sheet, final_df, row=1, col=1, include_index=False, include_column_header=True)
        print("🎉 Google Sheet updated successfully!")

        # 3. Problem Analysis and Data Preparation
        print("⏳ The issues are currently being analyzed, and the PDF report is being prepared....")
        all_checks, issues = process_spreadsheet_v2(final_df)
        total_reports = len(final_df)
        total_checks_count = len(all_checks)

        # 4. Upload and convert images
        print("جاري تحميل الصور وتضمينها في التقرير...")
        table_rows_html = ""
        final_issues = []

        for issue in issues:
            valid_images = []
            for url in issue['photo_urls']:
                base64_img = get_image_as_base64(url)
                if base64_img: valid_images.append(base64_img)

            if not valid_images:
                print(f"🚫 Skipped row for {issue['branch']} — all photos were black after download")
                continue

            final_issues.append(issue)
            photo_html = '<div class="photo-container">'
            for img in valid_images:
                photo_html += f'<img src="{img}" class="issue-photo" alt="صورة المشكلة">'
            photo_html += '</div>'

            category_map = {'اللون': 'color', 'الجودة': 'quality', 'الحجم': 'size'}
            badge_class = category_map.get(issue['metric'], 'availability')
            full_description = issue['problem']
            if issue['notes']:
                full_description += f"<br><small><b>ملاحظة:</b> {issue['notes']}</small>"

            table_rows_html += f"<tr><td>{issue['branch']}</td><td>{issue['product']}</td><td><span class='status-badge {badge_class}'>{issue['metric']}</span></td><td>{full_description}</td><td>{photo_html}</td></tr>"

        if not final_issues:
            table_rows_html = '<tr><td colspan="5" style="text-align:center; padding: 20px;">لم يتم العثور على مشاكل! عمل رائع.</td></tr>'

        # 5. Calculating Proportions
        total_issues_count    = len(final_issues)
        branches_with_issues  = len(set(issue['branch'] for issue in final_issues))
        quality_rate          = ((total_checks_count - total_issues_count) / total_checks_count * 100) if total_checks_count > 0 else 100
        branch_issues_counter = Counter(issue['branch']  for issue in final_issues)
        product_issues_counter= Counter(issue['product'] for issue in final_issues)

        print(f"✅ تم العثور على {total_issues_count} مشكلة فعلية في التقرير.")

        # 6. Data injection into the template
        final_html = html_template.replace('{{total_reports}}', str(total_reports))\
                                  .replace('{{quality_rate}}', f"{quality_rate:.1f}")\
                                  .replace('{{total_issues}}', str(total_issues_count))\
                                  .replace('{{branches_with_issues}}', str(branches_with_issues))\
                                  .replace('{{branch_issues_bars}}', create_bar_chart_html(branch_issues_counter))\
                                  .replace('{{product_issues_bars}}', create_bar_chart_html(product_issues_counter))\
                                  .replace('{{issues_table_rows}}', table_rows_html)

        # 7. Save HTML and Convert It to PDF Using Playwright
        with open('report.html', 'w', encoding='utf-8') as f: f.write(final_html)
        print("📡 جاري تحويل HTML إلى PDF...")
        current_date = datetime.now(TZ).strftime("%Y-%m-%d")
        pdf_name = f'Bread_Quality_Report_{current_date}.pdf'
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
            page = await browser.new_page()
            await page.set_viewport_size({"width": 1200, "height": 800})
            abs_path = f"file://{os.path.abspath('report.html')}"
            await page.goto(abs_path, wait_until="networkidle", timeout=90000)
            await page.evaluate("document.fonts.ready")
            # THE MAGIC HEIGHT TRICK
            h = await page.evaluate('document.body.scrollHeight')
            await page.pdf(
                path=pdf_name, 
                width='1100px',          # Matches the CSS width exactly
                height=f'{h + 100}px',   # Adds 100px buffer at the bottom
                print_background=True
            )
            await browser.close()

        # 8. Send Email
        send_final_email(pdf_name, {'rate': round(quality_rate, 1), 'issues': len(issues)}, final_issues)
        print("✅ The report was successfully generated, the spreadsheet was updated, and the email was sent!")

    except Exception as e:
        print(f"❌ An error occurred during operation: {e}")

if __name__ == "__main__":
    asyncio.run(main())
