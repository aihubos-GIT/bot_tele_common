from flask import Flask, request, jsonify
import requests
import datetime
import json
import os
from dotenv import load_dotenv
import pytz
import gspread
from google.oauth2.service_account import Credentials
import tempfile

load_dotenv()

app = Flask(__name__)

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
CLICKUP_TEAM_ID = os.getenv("CLICKUP_TEAM_ID")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")

# RenderAPI Configuration
RENDER_API_URL = os.getenv("RENDER_API_URL", "https://render-api-33ry.onrender.com")
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "")

# Mapping tags to chat IDs
TAG_TO_CHAT_ID = {
    "content": "-1003036322284",
    "dev": "-1002896048137",
    "admin": "-1003086591861",
    "default": os.getenv("CHAT_ID")
}

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS_JSON")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

print("="*50)
print("🔍 KIỂM TRA CONFIG:")
print(f"BOT_TOKEN: {BOT_TOKEN[:20]}..." if BOT_TOKEN else "BOT_TOKEN: ❌ KHÔNG CÓ")
print(f"DEFAULT CHAT_ID: {CHAT_ID}" if CHAT_ID else "CHAT_ID: ❌ KHÔNG CÓ")
print(f"CONTENT TEAM CHAT_ID: {TAG_TO_CHAT_ID['content']}")
print(f"DEV TEAM CHAT_ID: {TAG_TO_CHAT_ID['dev']}")
print(f"ADMIN CHAT_ID: {TAG_TO_CHAT_ID['admin']}")
print(f"CLICKUP_API_TOKEN: {CLICKUP_API_TOKEN[:20]}..." if CLICKUP_API_TOKEN else "CLICKUP_API_TOKEN: ❌ KHÔNG CÓ")
print(f"CLICKUP_TEAM_ID: {CLICKUP_TEAM_ID}")
print(f"CLICKUP_LIST_ID: {CLICKUP_LIST_ID}" if CLICKUP_LIST_ID else "CLICKUP_LIST_ID: ❌ KHÔNG CÓ")
print(f"GOOGLE_SHEET_ID: {SHEET_ID}" if SHEET_ID else "GOOGLE_SHEET_ID: ❌ KHÔNG CÓ")
print(f"GOOGLE_CREDENTIALS: {'✅ CÓ (' + str(len(GOOGLE_CREDENTIALS)) + ' chars)' if GOOGLE_CREDENTIALS else '❌ KHÔNG CÓ'}")
print(f"RENDER_API_URL: {RENDER_API_URL}" if RENDER_API_URL else "RENDER_API_URL: ❌ KHÔNG CÓ")
print(f"RENDER_API_KEY: {'✅ CÓ (' + str(len(RENDER_API_KEY)) + ' chars)' if RENDER_API_KEY else '❌ KHÔNG CÓ'}")
print(f"⏰ Server timezone: {datetime.datetime.now(VN_TZ).strftime('%H:%M:%S %d/%m/%Y')}")
print("="*50)

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
WEBHOOK_URL = f"https://bot-tele-common.onrender.com"


def get_vn_now():
    return datetime.datetime.now(VN_TZ)


def format_timestamp(timestamp):
    if not timestamp:
        return "Không có"
    try:
        dt_utc = datetime.datetime.fromtimestamp(int(timestamp) / 1000, tz=pytz.UTC)
        dt_vn = dt_utc.astimezone(VN_TZ)
        return dt_vn.strftime("%H:%M %d/%m/%Y")
    except:
        return "Không xác định"


def check_overdue(due_date):
    if not due_date:
        return False
    try:
        due_utc = datetime.datetime.fromtimestamp(int(due_date) / 1000, tz=pytz.UTC)
        due_vn = due_utc.astimezone(VN_TZ)
        now_vn = get_vn_now()
        return now_vn > due_vn
    except:
        return False


def calculate_duration(start_timestamp):
    if not start_timestamp:
        return ""
    try:
        start_utc = datetime.datetime.fromtimestamp(int(start_timestamp) / 1000, tz=pytz.UTC)
        start_vn = start_utc.astimezone(VN_TZ)
        now_vn = get_vn_now()
        duration = now_vn - start_vn
        
        if duration.days > 0:
            return f"{duration.days} ngày {duration.seconds // 3600} giờ"
        else:
            hours = duration.seconds // 3600
            minutes = (duration.seconds % 3600) // 60
            return f"{hours} giờ {minutes} phút"
    except:
        return ""


def get_chat_id_from_tags(tags):
    if not tags:
        return TAG_TO_CHAT_ID["default"]
    
    tag_names = [tag.get("name", "").lower() for tag in tags if isinstance(tag, dict)]
    
    for tag_name in tag_names:
        if "content" in tag_name:
            print(f"📌 Detected CONTENT tag: {tag_name}")
            return TAG_TO_CHAT_ID["content"]
        elif "dev" in tag_name or "developer" in tag_name:
            print(f"📌 Detected DEV tag: {tag_name}")
            return TAG_TO_CHAT_ID["dev"]
        elif "admin" in tag_name:
            print(f"📌 Detected ADMIN tag: {tag_name}")
            return TAG_TO_CHAT_ID["admin"]
    
    print(f"📌 No matching tag found, using default chat")
    return TAG_TO_CHAT_ID["default"]


def send_message(text, chat_id=None):
    if chat_id is None:
        chat_id = CHAT_ID
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        res = requests.post(TELEGRAM_API, json=payload, timeout=10)
        print(f"✅ Message sent to {chat_id} (status: {res.status_code})")
        return res.status_code
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        return None


def send_to_multiple_chats(text, chat_ids):
    for chat_id in chat_ids:
        send_message(text, chat_id)


def get_task_info(task_id):
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    headers = {"Authorization": CLICKUP_API_TOKEN}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ ClickUp API error: {response.status_code}")
        return None
    except Exception as e:
        print(f"❌ Error getting task info: {e}")
        return None


def get_all_tasks_in_period(start_date, end_date):
    if not CLICKUP_LIST_ID:
        print("❌ CLICKUP_LIST_ID không được cấu hình!")
        return []
    
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    headers = {"Authorization": CLICKUP_API_TOKEN}
    params = {
        "archived": "false",
        "include_closed": "true"
    }
    
    try:
        print(f"\n🔍 Querying all tasks from List {CLICKUP_LIST_ID}...")
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            all_tasks = data.get("tasks", [])
            
            start_ms = int(start_date.timestamp() * 1000)
            end_ms = int(end_date.timestamp() * 1000)
            
            filtered_tasks = []
            for task in all_tasks:
                date_created = task.get('date_created')
                if date_created:
                    created_ms = int(date_created)
                    if start_ms <= created_ms <= end_ms:
                        filtered_tasks.append(task)
            
            print(f"✅ Found {len(filtered_tasks)}/{len(all_tasks)} tasks in period")
            return filtered_tasks
        else:
            print(f"❌ ClickUp API error: {response.status_code}")
            print(f"Response: {response.text}")
            return []
    except Exception as e:
        print(f"❌ Error getting tasks: {e}")
        return []


def get_today_tasks():
    if not CLICKUP_LIST_ID:
        print("❌ CLICKUP_LIST_ID không được cấu hình!")
        return []
    
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    headers = {"Authorization": CLICKUP_API_TOKEN}
    params = {
        "archived": "false",
        "include_closed": "true"
    }
    
    try:
        print(f"\n🔍 Lấy tất cả tasks trong List {CLICKUP_LIST_ID}...")
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            all_tasks = data.get("tasks", [])
            print(f"✅ Tìm thấy {len(all_tasks)} tasks")
            return all_tasks
        else:
            print(f"❌ ClickUp API error: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error getting tasks: {e}")
        return []


def get_week_tasks():
    now = get_vn_now()
    days_since_monday = now.weekday()
    start_of_week = (now - datetime.timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = (start_of_week + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return get_all_tasks_in_period(start_of_week, end_of_week)


def analyze_tasks(tasks):
    """
    Phân tích tasks theo người được ASSIGN (không phải người tạo)
    """
    stats = {
        'total': len(tasks),
        'completed': 0,
        'pending': 0,
        'overdue': 0,
        'unassigned': 0,
        'in_progress': 0,
        'by_user': {},
        'by_priority': {
            'urgent': 0,
            'high': 0,
            'normal': 0,
            'low': 0
        }
    }
    
    for task in tasks:
        status_info = task.get('status', {})
        status = status_info.get('status', '').lower() if isinstance(status_info, dict) else ''
        
        is_completed = status in ['complete', 'completed', 'closed', 'done', 'achevé']
        is_in_progress = status in ['in progress', 'en cours', 'doing']
        
        if is_completed:
            stats['completed'] += 1
        else:
            stats['pending'] += 1
            
            if is_in_progress:
                stats['in_progress'] += 1
            
            due_date = task.get('due_date')
            if due_date and check_overdue(due_date):
                stats['overdue'] += 1
        
        assignees = task.get('assignees', [])
        
        if not assignees or len(assignees) == 0:
            stats['unassigned'] += 1
        else:
            for assignee in assignees:
                username = assignee.get('username', 'Unknown')
                
                if username not in stats['by_user']:
                    stats['by_user'][username] = {
                        'completed': 0, 
                        'pending': 0, 
                        'overdue': 0,
                        'in_progress': 0,
                        'total': 0
                    }
                
                stats['by_user'][username]['total'] += 1
                
                if is_completed:
                    stats['by_user'][username]['completed'] += 1
                else:
                    stats['by_user'][username]['pending'] += 1
                    
                    if is_in_progress:
                        stats['by_user'][username]['in_progress'] += 1
                    
                    due_date = task.get('due_date')
                    if due_date and check_overdue(due_date):
                        stats['by_user'][username]['overdue'] += 1
        
        priority = task.get('priority')
        if isinstance(priority, dict):
            priority_id = priority.get('priority')
        else:
            priority_id = priority
            
        if priority_id == 1:
            stats['by_priority']['urgent'] += 1
        elif priority_id == 2:
            stats['by_priority']['high'] += 1
        elif priority_id == 3:
            stats['by_priority']['normal'] += 1
        elif priority_id == 4:
            stats['by_priority']['low'] += 1
    
    return stats


def get_priority_text(priority_data):
    if not priority_data:
        return "Không có"
    
    if isinstance(priority_data, dict):
        priority_id = priority_data.get("priority")
    else:
        priority_id = priority_data
    
    priority_map = {
        1: "🔴 Khẩn cấp",
        2: "🟠 Cao", 
        3: "🟡 Bình thường",
        4: "🔵 Thấp"
    }
    
    return priority_map.get(priority_id, "Không xác định")


def get_gsheet_client():
    try:
        if not GOOGLE_CREDENTIALS:
            print("❌ Không có GOOGLE_CREDENTIALS_JSON")
            return None
        
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(credentials)
        
        print("✅ Connected to Google Sheet")
        return client
        
    except Exception as e:
        print(f"❌ Error connecting to Google Sheet: {e}")
        return None


def init_sheet_headers():
    try:
        client = get_gsheet_client()
        if not client:
            return False
        
        sheet = client.open_by_key(SHEET_ID)
        
        try:
            worksheet = sheet.worksheet("Tasks")
        except:
            worksheet = sheet.add_worksheet(title="Tasks", rows=1000, cols=12)
            headers = [
                "Timestamp", "Task Name", "Assignee", "Status",
                "Priority", "Created", "Due Date", "Completed",
                "Duration", "On Time?", "URL", "Creator"
            ]
            worksheet.append_row(headers)
        
        return True
    except Exception as e:
        print(f"❌ Error init headers: {e}")
        return False


def backup_to_sheet(task_info):
    try:
        client = get_gsheet_client()
        if not client:
            return False
        
        sheet = client.open_by_key(SHEET_ID)
        worksheet = sheet.worksheet("Tasks")
        
        row = [
            task_info.get("timestamp", ""),
            task_info.get("name", ""),
            task_info.get("assignee", ""),
            task_info.get("status", ""),
            task_info.get("priority", ""),
            task_info.get("created", ""),
            task_info.get("due_date", ""),
            task_info.get("completed", ""),
            task_info.get("duration", ""),
            task_info.get("on_time", ""),
            task_info.get("url", ""),
            task_info.get("creator", "")
        ]
        
        worksheet.append_row(row)
        print(f"✅ Backed up to Google Sheet: {task_info.get('name')}")
        return True
        
    except Exception as e:
        print(f"❌ Error backup to sheet: {e}")
        return False


def generate_report(report_type="daily"):
    now = get_vn_now()
    today_display = now.strftime("%d/%m/%Y")
    time_display = now.strftime("%H:%M")
    
    tasks = get_today_tasks()
    stats = analyze_tasks(tasks)
    
    kpi = (stats['completed'] / stats['total'] * 100) if stats['total'] > 0 else 0
    
    if report_type == "morning":
        header = f"🌅 <b>BÁO CÁO BUỔI SÁNG - {today_display} {time_display}</b>"
        greeting = "☕ Chào buổi sáng! Tình hình công việc hiện tại:"
    elif report_type == "noon":
        header = f"☀️ <b>BÁO CÁO BUỔI TRƯA - {today_display} {time_display}</b>"
        greeting = "🍜 Giờ nghỉ trưa! Cập nhật tiến độ:"
    elif report_type == "evening":
        header = f"🌙 <b>BÁO CÁO KẾT THÚC NGÀY - {today_display} {time_display}</b>"
        greeting = "📊 Tổng kết ngày làm việc:"
    else:
        header = f"📊 <b>BÁO CÁO - {today_display} {time_display}</b>"
        greeting = "📈 Tình hình công việc:"
    
    msg = f"""
{header}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{greeting}
"""
    
    if stats['total'] == 0:
        msg += f"\n⚠️ Chưa có task nào trong List"
    else:
        msg += f"\n📋 <b>Tổng tasks:</b> {stats['total']}"
        msg += f"\n✅ <b>Đã hoàn thành:</b> {stats['completed']} (<b>{kpi:.1f}%</b>)"
        
        if stats['in_progress'] > 0:
            msg += f"\n🔄 <b>Đang làm:</b> {stats['in_progress']}"
        
        remaining = stats['pending'] - stats['in_progress']
        if remaining > 0:
            msg += f"\n⏳ <b>Chưa làm:</b> {remaining}"
        
        if stats['overdue'] > 0:
            msg += f"\n🔴 <b>Quá hạn:</b> {stats['overdue']}"
        
        if stats['unassigned'] > 0:
            msg += f"\n❓ <b>Chưa phân công:</b> {stats['unassigned']}"
        
        if stats['by_user']:
            msg += f"\n\n👥 <b>KPI theo người được giao việc:</b>"
            
            sorted_users = sorted(
                stats['by_user'].items(), 
                key=lambda x: (x[1]['completed'] / x[1]['total'] if x[1]['total'] > 0 else 0), 
                reverse=True
            )
            
            for username, user_stats in sorted_users:
                user_kpi = (user_stats['completed'] / user_stats['total'] * 100) if user_stats['total'] > 0 else 0
                
                if user_kpi >= 90:
                    icon = "🌟"
                elif user_kpi >= 70:
                    icon = "✅"
                elif user_kpi >= 50:
                    icon = "⚠️"
                else:
                    icon = "🔴"
                
                msg += f"\n   {icon} <b>{username}</b>: {user_stats['completed']}/{user_stats['total']} (<b>{user_kpi:.0f}%</b>)"
                
                if user_stats.get('in_progress', 0) > 0:
                    msg += f" - 🔄 {user_stats['in_progress']} đang làm"
                
                if user_stats.get('overdue', 0) > 0:
                    msg += f" - 🔴 {user_stats['overdue']} quá hạn"
        
        total_priority = sum(stats['by_priority'].values())
        if total_priority > 0:
            msg += f"\n\n⚡ <b>Độ ưu tiên:</b>"
            if stats['by_priority']['urgent'] > 0:
                msg += f"\n   🔴 Khẩn cấp: {stats['by_priority']['urgent']}"
            if stats['by_priority']['high'] > 0:
                msg += f"\n   🟠 Cao: {stats['by_priority']['high']}"
            if stats['by_priority']['normal'] > 0:
                msg += f"\n   🟡 Bình thường: {stats['by_priority']['normal']}"
            if stats['by_priority']['low'] > 0:
                msg += f"\n   🔵 Thấp: {stats['by_priority']['low']}"
    
    msg += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if report_type == "morning":
        msg += f"\n💪 Chúc mọi người làm việc hiệu quả!"
    elif report_type == "noon":
        msg += f"\n🔋 Nghỉ ngơi đầy năng lượng, chiều cố gắng nào!"
    elif report_type == "evening":
        if kpi >= 80:
            msg += f"\n🎉 Xuất sắc! KPI rất cao!"
        elif kpi >= 60:
            msg += f"\n👏 Tốt lắm! Tiếp tục phát huy!"
        else:
            msg += f"\n💪 Ngày mai cố gắng hơn nữa nhé!"
        msg += f"\n😴 Chúc ngủ ngon!"
        
        week_tasks = get_week_tasks()
        if week_tasks:
            week_stats = analyze_tasks(week_tasks)
            kpi_week = (week_stats['completed'] / week_stats['total'] * 100) if week_stats['total'] > 0 else 0
            
            msg += f"\n\n📅 <b>KPI TUẦN NÀY (Tasks mới tạo):</b>"
            msg += f"\n   • Tổng: {week_stats['total']}"
            msg += f"\n   • Hoàn thành: {week_stats['completed']} (<b>{kpi_week:.1f}%</b>)"
            msg += f"\n   • Còn lại: {week_stats['pending']}"
            
            if week_stats['overdue'] > 0:
                msg += f"\n   • Quá hạn: {week_stats['overdue']}"
    
    return msg


def generate_weekly_report_html(week_stats, start_date, end_date):
    now = get_vn_now()
    
    kpi = (week_stats['completed'] / week_stats['total'] * 100) if week_stats['total'] > 0 else 0
    
    user_rows = ""
    if week_stats['by_user']:
        sorted_users = sorted(
            week_stats['by_user'].items(),
            key=lambda x: (x[1]['completed'] / x[1]['total'] if x[1]['total'] > 0 else 0),
            reverse=True
        )
        
        for username, user_stats in sorted_users:
            user_kpi = (user_stats['completed'] / user_stats['total'] * 100) if user_stats['total'] > 0 else 0
            
            if user_kpi >= 90:
                kpi_class = "excellent"
                icon = "🌟"
            elif user_kpi >= 70:
                kpi_class = "good"
                icon = "✅"
            elif user_kpi >= 50:
                kpi_class = "average"
                icon = "⚠️"
            else:
                kpi_class = "poor"
                icon = "🔴"
            
            user_rows += f"""
            <tr class="{kpi_class}">
                <td>{icon} <strong>{username}</strong></td>
                <td>{user_stats['total']}</td>
                <td>{user_stats['completed']}</td>
                <td>{user_stats['pending']}</td>
                <td>{user_stats.get('in_progress', 0)}</td>
                <td>{user_stats.get('overdue', 0)}</td>
                <td class="kpi-cell"><strong>{user_kpi:.1f}%</strong></td>
            </tr>
            """
    
    html = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Báo Cáo Tuần - AIHubOS</title>
        <style>
            @page {{
                size: A4;
                margin: 1.5cm;
            }}
            body {{
                font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: #7A3F30;
                background: #E9D9C5;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 900px;
                margin: 0 auto;
                background: #E9D9C5;
            }}
            .header {{
                background: linear-gradient(135deg, #0F1330 0%, #1a1f4d 100%);
                color: #D4A459;
                padding: 40px;
                border-radius: 15px;
                text-align: center;
                margin-bottom: 30px;
                box-shadow: 0 8px 20px rgba(15, 19, 48, 0.3);
            }}
            .header h1 {{
                margin: 0;
                font-size: 36px;
                font-weight: 700;
                letter-spacing: 1px;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }}
            .header .date {{
                margin-top: 15px;
                font-size: 16px;
                color: #D4A459;
                opacity: 0.95;
                font-weight: 500;
            }}
            .summary {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 20px;
                margin-bottom: 30px;
            }}
            .summary-card {{
                background: white;
                padding: 25px;
                border-radius: 12px;
                text-align: center;
                box-shadow: 0 4px 15px rgba(122, 63, 48, 0.1);
                border: 2px solid #D4A459;
                transition: transform 0.3s ease;
            }}
            .summary-card:hover {{
                transform: translateY(-5px);
            }}
            .summary-card h3 {{
                margin: 0 0 15px 0;
                font-size: 14px;
                color: #7A3F30;
                text-transform: uppercase;
                letter-spacing: 1px;
                font-weight: 600;
            }}
            .summary-card .value {{
                font-size: 42px;
                font-weight: 700;
                color: #D4A459;
                margin-bottom: 8px;
            }}
            .summary-card .label {{
                font-size: 13px;
                color: #7A3F30;
                opacity: 0.8;
            }}
            .kpi-chart {{
                background: white;
                padding: 30px;
                border-radius: 12px;
                margin-bottom: 30px;
                box-shadow: 0 4px 15px rgba(122, 63, 48, 0.1);
                border: 2px solid #D4A459;
            }}
            .kpi-chart h3 {{
                margin-top: 0;
                color: #7A3F30;
                font-size: 20px;
                margin-bottom: 20px;
            }}
            .kpi-bar {{
                background: #E9D9C5;
                height: 50px;
                border-radius: 25px;
                overflow: hidden;
                position: relative;
                border: 2px solid #D4A459;
            }}
            .kpi-fill {{
                height: 100%;
                background: linear-gradient(90deg, #D4A459 0%, #C3A387 100%);
                display: flex;
                align-items: center;
                justify-content: center;
                color: #0F1330;
                font-weight: 700;
                font-size: 20px;
                transition: width 0.3s ease;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 30px;
                background: white;
                box-shadow: 0 4px 15px rgba(122, 63, 48, 0.1);
                border-radius: 12px;
                overflow: hidden;
            }}
            th {{
                background: #0F1330;
                color: #D4A459;
                padding: 16px;
                text-align: left;
                font-weight: 600;
                font-size: 14px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            td {{
                padding: 14px 16px;
                border-bottom: 1px solid #E9D9C5;
                color: #7A3F30;
            }}
            tr:hover {{
                background: #f9f6f2;
            }}
            tr.excellent {{
                background: #d4e8d4;
            }}
            tr.good {{
                background: #dae8f5;
            }}
            tr.average {{
                background: #fff4d6;
            }}
            tr.poor {{
                background: #fce4e4;
            }}
            .kpi-cell {{
                font-size: 17px;
                font-weight: 700;
                color: #D4A459;
            }}
            .footer {{
                text-align: center;
                padding: 25px;
                color: #7A3F30;
                border-top: 3px solid #D4A459;
                margin-top: 30px;
                background: white;
                border-radius: 12px;
            }}
            .footer strong {{
                color: #D4A459;
                font-size: 18px;
            }}
            .priority-section {{
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 20px;
                margin-bottom: 30px;
            }}
            .priority-card {{
                background: white;
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                border: 2px solid #D4A459;
                box-shadow: 0 4px 15px rgba(122, 63, 48, 0.1);
            }}
            .priority-card .icon {{
                font-size: 28px;
                margin-bottom: 10px;
            }}
            .priority-card .count {{
                font-size: 32px;
                font-weight: 700;
                margin: 10px 0;
                color: #D4A459;
            }}
            .priority-card .label {{
                font-size: 13px;
                color: #7A3F30;
                font-weight: 600;
            }}
            h3 {{
                color: #7A3F30;
                font-size: 22px;
                margin-bottom: 20px;
                padding-left: 10px;
                border-left: 4px solid #D4A459;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 BÁO CÁO TUẦN - AIHUBOS</h1>
                <div class="date">
                    Tuần từ {start_date.strftime('%d/%m/%Y')} đến {end_date.strftime('%d/%m/%Y')}<br>
                    Tạo lúc: {now.strftime('%H:%M:%S %d/%m/%Y')}
                </div>
            </div>

            <div class="summary">
                <div class="summary-card">
                    <h3>Tổng Tasks</h3>
                    <div class="value">{week_stats['total']}</div>
                    <div class="label">Tasks trong tuần</div>
                </div>
                <div class="summary-card">
                    <h3>Hoàn Thành</h3>
                    <div class="value">{week_stats['completed']}</div>
                    <div class="label">{kpi:.1f}% KPI</div>
                </div>
                <div class="summary-card">
                    <h3>Chưa Xong</h3>
                    <div class="value">{week_stats['pending']}</div>
                    <div class="label">{week_stats.get('in_progress', 0)} đang làm</div>
                </div>
            </div>

            <div class="kpi-chart">
                <h3>📈 KPI Tuần</h3>
                <div class="kpi-bar">
                    <div class="kpi-fill" style="width: {kpi}%">{kpi:.1f}%</div>
                </div>
            </div>

            <h3>🎯 Độ Ưu Tiên Tasks</h3>
            <div class="priority-section">
                <div class="priority-card">
                    <div class="icon">🔴</div>
                    <div class="count">{week_stats['by_priority'].get('urgent', 0)}</div>
                    <div class="label">Khẩn cấp</div>
                </div>
                <div class="priority-card">
                    <div class="icon">🟠</div>
                    <div class="count">{week_stats['by_priority'].get('high', 0)}</div>
                    <div class="label">Cao</div>
                </div>
                <div class="priority-card">
                    <div class="icon">🟡</div>
                    <div class="count">{week_stats['by_priority'].get('normal', 0)}</div>
                    <div class="label">Bình thường</div>
                </div>
                <div class="priority-card">
                    <div class="icon">🔵</div>
                    <div class="count">{week_stats['by_priority'].get('low', 0)}</div>
                    <div class="label">Thấp</div>
                </div>
            </div>

            <h3>👥 KPI Theo Người Được Giao Việc</h3>
            <table>
                <thead>
                    <tr>
                        <th>Người thực hiện</th>
                        <th>Tổng</th>
                        <th>Hoàn thành</th>
                        <th>Còn lại</th>
                        <th>Đang làm</th>
                        <th>Quá hạn</th>
                        <th>KPI</th>
                    </tr>
                </thead>
                <tbody>
                    {user_rows}
                </tbody>
            </table>

            <div class="footer">
                <p><strong>AIHubOS Automation System v1.3</strong></p>
                <p>🤖 Báo cáo tự động - Không cần thao tác thủ công</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

def generate_and_send_weekly_pdf():
    print(f"\n📊 Generating weekly report PDF...")
    
    now = get_vn_now()
    days_since_monday = now.weekday()
    start_of_week = (now - datetime.timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = (start_of_week + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999)
    
    week_tasks = get_all_tasks_in_period(start_of_week, end_of_week)
    
    if not week_tasks:
        print("   ⚠️  Không có tasks trong tuần này")
        return False
    
    week_stats = analyze_tasks(week_tasks)
    print(f"   ✅ Analyzed {len(week_tasks)} tasks")
    
    html_content = generate_weekly_report_html(week_stats, start_of_week, end_of_week)
    print(f"   ✅ Generated HTML report")
    
    if not RENDER_API_KEY or not RENDER_API_URL:
        print("   ❌ RENDER_API_KEY hoặc RENDER_API_URL chưa được config!")
        return False
    
    render_url = f"{RENDER_API_URL}/render"
    headers = {
        "Authorization": f"Bearer {RENDER_API_KEY}",
        "Content-Type": "application/json"
    }
    
    filename = f"weekly_report_{start_of_week.strftime('%Y%m%d')}_{end_of_week.strftime('%Y%m%d')}.pdf"
    
    payload = {
        "html": html_content,
        "filename": filename
    }
    
    try:
        print(f"   🔄 Calling RenderAPI...")
        response = requests.post(render_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            print(f"   ✅ PDF generated successfully")
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(response.content)
                pdf_path = tmp_file.name
            
            all_chat_ids = list(set(TAG_TO_CHAT_ID.values()))
            
            for chat_id in all_chat_ids:
                try:
                    telegram_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
                    
                    with open(pdf_path, 'rb') as pdf_file:
                        files = {'document': (filename, pdf_file, 'application/pdf')}
                        data = {
                            'chat_id': chat_id,
                            'caption': f'📊 <b>BÁO CÁO TUẦN</b>\n\nTuần: {start_of_week.strftime("%d/%m")} - {end_of_week.strftime("%d/%m/%Y")}\n✅ Hoàn thành: {week_stats["completed"]}/{week_stats["total"]} tasks',
                            'parse_mode': 'HTML'
                        }
                        
                        tg_response = requests.post(telegram_url, files=files, data=data, timeout=30)
                        
                        if tg_response.status_code == 200:
                            print(f"   ✅ Sent PDF to {chat_id}")
                        else:
                            print(f"   ❌ Failed to send to {chat_id}: {tg_response.text}")
                
                except Exception as e:
                    print(f"   ❌ Error sending to {chat_id}: {e}")
            
            try:
                os.unlink(pdf_path)
            except:
                pass
            
            return True
            
        else:
            print(f"   ❌ RenderAPI error: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    
    except Exception as e:
        print(f"   ❌ Error calling RenderAPI: {e}")
        return False


@app.route('/telegram', methods=['POST'])
def telegram_handler():
    data = request.get_json()
    
    if "message" in data:
        message = data["message"]
        text = message.get("text", "")
        
        if text == "/report_eod":
            msg = generate_report("evening")
            all_chat_ids = list(TAG_TO_CHAT_ID.values())
            send_to_multiple_chats(msg, all_chat_ids)
        
        elif text == "/report_now":
            msg = generate_report("daily")
            all_chat_ids = list(TAG_TO_CHAT_ID.values())
            send_to_multiple_chats(msg, all_chat_ids)
    
    return {"ok": True}, 200


@app.route('/clickup', methods=['POST', 'GET'])
def clickup_webhook():
    print("\n========== CLICKUP WEBHOOK RECEIVED ==========")
    print(f"⏰ Time (VN): {get_vn_now().strftime('%H:%M:%S %d/%m/%Y')}")
    
    data = request.get_json()
    
    try:
        with open('clickup_data.json', 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, indent=2, ensure_ascii=False))
            f.write("\n\n" + "="*50 + "\n\n")
    except Exception as e:
        print(f"❌ Error logging: {e}")
    
    event = data.get("event", "")
    history_items = data.get("history_items", [])
    task_id = data.get("task_id", "")
    
    task_data = get_task_info(task_id)
    
    if not task_data:
        return {"ok": True}, 200
    
    tags = task_data.get("tags", [])
    target_chat_id = get_chat_id_from_tags(tags)
    
    print(f"📍 Target chat ID: {target_chat_id}")
    print(f"🏷️ Tags: {[tag.get('name') for tag in tags if isinstance(tag, dict)]}")
    
    task_name = task_data.get("name", "Không rõ")
    task_url = task_data.get("url", "")
    
    status_info = task_data.get("status", {})
    status = status_info.get("status", "Không rõ") if isinstance(status_info, dict) else "Không rõ"
    
    creator = task_data.get("creator", {})
    creator_name = creator.get("username", "Không rõ") if isinstance(creator, dict) else "Không rõ"
    
    assignees = task_data.get("assignees", [])
    if assignees:
        assignees_list = [a.get("username", "N/A") for a in assignees]
        assignees_text = ", ".join(assignees_list)
    else:
        assignees_text = "Chưa phân công"
    
    priority_data = task_data.get("priority")
    priority_text = get_priority_text(priority_data)
    
    due_date = task_data.get("due_date")
    due_date_text = "Không có"
    is_overdue = False
    if due_date:
        due_date_text = format_timestamp(due_date)
        is_overdue = check_overdue(due_date)
    
    date_created = task_data.get("date_created")
    created_time = format_timestamp(date_created)
    
    now = get_vn_now().strftime("%H:%M:%S %d/%m/%Y")
    
    action_user = "Không rõ"
    if history_items:
        first_item = history_items[0]
        user_info = first_item.get("user", {})
        if isinstance(user_info, dict):
            action_user = user_info.get("username", "Không rõ")
    
    if event == "taskCreated":
        overdue_warning = ""
        if is_overdue:
            overdue_warning = "\n⚠️ <b>CẢNH BÁO: ĐÃ QUÁ HẠN!</b>"
        
        msg = f"""
🆕 <b>TASK MỚI ĐƯỢC TẠO</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👤 Người tạo: <b>{creator_name}</b>
👥 Phân công: <b>{assignees_text}</b>
⚡ Mức độ: {priority_text}
📅 Deadline: {due_date_text}{overdue_warning}
🕒 Tạo lúc: {created_time}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
        send_message(msg.strip(), target_chat_id)
    
    elif event == "taskUpdated":
        for item in history_items:
            field = item.get("field", "")
            
            if field == "status":
                before = item.get("before", {})
                after = item.get("after", {})
                
                old_status = before.get("status", "Không rõ") if isinstance(before, dict) else "Không rõ"
                new_status = after.get("status", "Không rõ") if isinstance(after, dict) else "Không rõ"
                
                if new_status.lower() in ["complete", "completed", "closed", "done", "achevé"]:
                    completion_status = ""
                    time_diff_msg = ""
                    
                    if due_date:
                        try:
                            due_datetime = datetime.datetime.fromtimestamp(int(due_date) / 1000, tz=pytz.UTC).astimezone(VN_TZ)
                            now_datetime = get_vn_now()
                            time_diff = due_datetime - now_datetime
                            
                            hours_diff = time_diff.total_seconds() / 3600
                            days_diff = time_diff.days
                            
                            if hours_diff < 0:
                                abs_hours = abs(hours_diff)
                                if abs_hours < 24:
                                    time_diff_msg = f"\n⏰ Trễ deadline: <b>{int(abs_hours)} giờ {int((abs_hours % 1) * 60)} phút</b>"
                                else:
                                    time_diff_msg = f"\n⏰ Trễ deadline: <b>{abs(days_diff)} ngày</b>"
                                completion_status = "\n🔴 <b>TRẠNG THÁI: TRỄ DEADLINE</b>"
                            elif hours_diff >= 24:
                                if days_diff >= 1:
                                    time_diff_msg = f"\n⚡ Hoàn thành sớm: <b>{days_diff} ngày</b>"
                                else:
                                    time_diff_msg = f"\n⚡ Hoàn thành sớm: <b>{int(hours_diff)} giờ</b>"
                                completion_status = "\n🌟 <b>VƯỢT TIẾN ĐỘ! XUẤT SẮC!</b> 🎉"
                            else:
                                time_diff_msg = f"\n⏰ Còn {int(hours_diff)} giờ {int((hours_diff % 1) * 60)} phút đến deadline"
                                completion_status = "\n✅ <b>HOÀN THÀNH ĐÚNG TIẾN ĐỘ!</b> 👏"
                        except Exception as e:
                            print(f"❌ Error calculating time diff: {e}")
                    else:
                        completion_status = "\n✅ <b>HOÀN THÀNH!</b>"
                    
                    time_to_complete = ""
                    if date_created:
                        duration_str = calculate_duration(date_created)
                        if duration_str:
                            time_to_complete = f"\n⏱️ Thời gian làm: <b>{duration_str}</b>"
                    
                    msg = f"""
✅ <b>TASK HOÀN THÀNH</b>{completion_status}
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👤 Người hoàn thành: <b>{action_user}</b>
👥 Đã phân công cho: <b>{assignees_text}</b>
⚡ Mức độ: {priority_text}
📅 Deadline: {due_date_text}{time_diff_msg}{time_to_complete}
🕒 Hoàn thành lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
                    send_message(msg.strip(), target_chat_id)
                    
                    duration_str = calculate_duration(date_created) if date_created else ""
                    on_time_status = "Không xác định"
                    
                    if due_date:
                        on_time_status = "Trễ" if is_overdue else "Đúng hạn"
                    
                    backup_info = {
                        "timestamp": now,
                        "name": task_name,
                        "assignee": action_user,
                        "status": new_status,
                        "priority": priority_text,
                        "created": created_time,
                        "due_date": due_date_text,
                        "completed": now,
                        "duration": duration_str,
                        "on_time": on_time_status,
                        "url": task_url,
                        "creator": creator_name
                    }
                    
                    backup_to_sheet(backup_info)
                
                else:
                    msg = f"""
🔄 <b>THAY ĐỔI TRẠNG THÁI</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👤 Người thay đổi: <b>{action_user}</b>
📌 Từ: {old_status} → <b>{new_status}</b>
⚡ Mức độ: {priority_text}
🕒 Lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
                    send_message(msg.strip(), target_chat_id)
            
            elif field == "assignee_add":
                after = item.get("after", {})
                new_assignee = after.get("username", "Không rõ") if isinstance(after, dict) else "Không rõ"
                
                overdue_warning = ""
                if is_overdue:
                    overdue_warning = "\n⚠️ <b>Task đã quá hạn!</b>"
                
                msg = f"""
👤 <b>PHÂN CÔNG TASK</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
➕ Được giao cho: <b>{new_assignee}</b>
⚡ Mức độ: {priority_text}
📅 Deadline: {due_date_text}{overdue_warning}
🕒 Lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
                send_message(msg.strip(), target_chat_id)
            
            elif field == "assignee_rem":
                before = item.get("before", {})
                removed_assignee = before.get("username", "Không rõ") if isinstance(before, dict) else "Không rõ"
                
                msg = f"""
👤 <b>XÓA PHÂN CÔNG</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
➖ Đã xóa: <b>{removed_assignee}</b>
⚡ Mức độ: {priority_text}
🕒 Lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
                send_message(msg.strip(), target_chat_id)
            
            elif field == "due_date":
                after = item.get("after", {})
                new_due = format_timestamp(after) if after else "Không có"
                
                msg = f"""
📅 <b>THAY ĐỔI DEADLINE</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👤 Người thay đổi: <b>{action_user}</b>
📅 Deadline mới: <b>{new_due}</b>
⚡ Mức độ: {priority_text}
👥 Phụ trách: {assignees_text}
🕒 Lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
                send_message(msg.strip(), target_chat_id)
        
        if is_overdue and status.lower() not in ["complete", "completed", "closed", "done", "achevé"]:
            msg = f"""
⚠️ <b>CẢNH BÁO: TASK QUÁ HẠN!</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👥 Người phụ trách: <b>{assignees_text}</b>
📅 Deadline: {due_date_text}
⚡ Mức độ: {priority_text}
⏰ <b>ĐÃ QUÁ HẠN!</b>
📌 Trạng thái: {status}
🕒 Kiểm tra lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem ngay</a>
"""
            send_message(msg.strip(), target_chat_id)
    
    elif event == "taskDeleted":
        msg = f"""
🗑️ <b>TASK ĐÃ BỊ XÓA</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👤 Người xóa: <b>{action_user}</b>
⚡ Mức độ: {priority_text}
👥 Đã phân công cho: {assignees_text}
🕒 Xóa lúc: {now}
━━━━━━━━━━━━━━━━━━━━
"""
        send_message(msg.strip(), target_chat_id)
    
    elif event == "taskCommentPosted":
        comment_text = "Không có nội dung"
        for item in history_items:
            if item.get("field") == "comment":
                comment_data = item.get("comment", {})
                if isinstance(comment_data, dict):
                    comment_text = comment_data.get("text_content", "Không có nội dung")
                break
        
        if len(comment_text) > 200:
            comment_text = comment_text[:200] + "..."
        
        msg = f"""
💬 <b>COMMENT MỚI</b>
━━━━━━━━━━━━━━━━━━━━
📋 Task: <b>{task_name}</b>
👤 Người comment: <b>{action_user}</b>
⚡ Mức độ: {priority_text}
💭 Nội dung: {comment_text}
🕒 Lúc: {now}
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem chi tiết</a>
"""
        send_message(msg.strip(), target_chat_id)
    
    return {"ok": True}, 200


@app.route('/', methods=['GET'])
def home():
    return "✅ ClickUp ↔ Telegram bot đang hoạt động!", 200


@app.route('/trigger_morning_report', methods=['GET', 'HEAD'])
def trigger_morning_report():
    if request.method == 'HEAD':
        return '', 200
    
    print(f"\n🌅 Morning report triggered at {get_vn_now().strftime('%H:%M:%S')}")
    try:
        msg = generate_report("morning")
        all_chat_ids = list(set(TAG_TO_CHAT_ID.values()))
        send_to_multiple_chats(msg, all_chat_ids)
        return 'OK', 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return 'ER', 500


@app.route('/trigger_noon_report', methods=['GET', 'HEAD'])
def trigger_noon_report():
    if request.method == 'HEAD':
        return '', 200
    
    print(f"\n☀️ Noon report triggered at {get_vn_now().strftime('%H:%M:%S')}")
    try:
        msg = generate_report("noon")
        all_chat_ids = list(set(TAG_TO_CHAT_ID.values()))
        send_to_multiple_chats(msg, all_chat_ids)
        return 'OK', 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return 'ER', 500


@app.route('/trigger_evening_report', methods=['GET', 'HEAD'])
def trigger_evening_report():
    if request.method == 'HEAD':
        return '', 200
    
    print(f"\n🌙 Evening report triggered at {get_vn_now().strftime('%H:%M:%S')}")
    try:
        msg = generate_report("evening")
        all_chat_ids = list(set(TAG_TO_CHAT_ID.values()))
        send_to_multiple_chats(msg, all_chat_ids)
        return 'OK', 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return 'ER', 500


@app.route('/trigger_weekly_report', methods=['GET', 'HEAD'])
def trigger_weekly_report():
    if request.method == 'HEAD':
        return '', 200
    
    print(f"\n📊 Weekly report triggered at {get_vn_now().strftime('%H:%M:%S')}")
    
    try:
        success = generate_and_send_weekly_pdf()
        
        if success:
            return jsonify({
                "status": "success",
                "message": "Weekly report generated and sent successfully"
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to generate or send weekly report"
            }), 500
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/setup_webhook', methods=['GET'])
def setup_webhook():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    telegram_webhook = f"{WEBHOOK_URL}/telegram"
    
    response = requests.post(url, data={"url": telegram_webhook})
    result = response.json()
    
    if result.get("ok"):
        return f"✅ Webhook đã được set thành công!<br>URL: {telegram_webhook}<br>Response: {result}", 200
    else:
        return f"❌ Lỗi set webhook!<br>Response: {result}", 500

@app.route('/trigger_deadline_warning', methods=['GET', 'HEAD'])
def trigger_deadline_warning():
    if request.method == 'HEAD':
        return '', 200
    
    print(f"\n⚠️ Deadline warning check triggered at {get_vn_now().strftime('%H:%M:%S')}")
    
    try:
        tasks = get_today_tasks()
        
        if not tasks:
            print("   ℹ️  No tasks found")
            return 'OK', 200
        
        now_vn = get_vn_now()
        tomorrow = now_vn + datetime.timedelta(days=1)
        tomorrow_start = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_end = tomorrow.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        warnings_sent = 0
        
        for task in tasks:
            status_info = task.get('status', {})
            status = status_info.get('status', '').lower() if isinstance(status_info, dict) else ''
            
            if status in ['complete', 'completed', 'closed', 'done', 'achevé']:
                continue
            
            due_date = task.get('due_date')
            if not due_date:
                continue
            
            try:
                due_utc = datetime.datetime.fromtimestamp(int(due_date) / 1000, tz=pytz.UTC)
                due_vn = due_utc.astimezone(VN_TZ)
                
                if tomorrow_start <= due_vn <= tomorrow_end:
                    task_name = task.get('name', 'Không rõ')
                    task_url = task.get('url', '')
                    
                    assignees = task.get('assignees', [])
                    if assignees:
                        assignees_list = [a.get("username", "N/A") for a in assignees]
                        assignees_text = ", ".join(assignees_list)
                    else:
                        assignees_text = "Chưa phân công"
                    
                    priority_data = task.get('priority')
                    priority_text = get_priority_text(priority_data)
                    
                    due_date_text = format_timestamp(due_date)
                    
                    tags = task.get("tags", [])
                    target_chat_id = get_chat_id_from_tags(tags)
                    
                    hours_left = (due_vn - now_vn).total_seconds() / 3600
                    
                    msg = f"""
⏰ <b>CẢNH BÁO: TASK SẮP HẾT HẠN!</b>
━━━━━━━━━━━━━━━━━━━━
📋 <b>{task_name}</b>
👥 Người phụ trách: <b>{assignees_text}</b>
⚡ Mức độ: {priority_text}
📅 Deadline: <b>{due_date_text}</b>
⏳ Còn lại: <b>{int(hours_left)} giờ {int((hours_left % 1) * 60)} phút</b>
📌 Trạng thái: {status}
━━━━━━━━━━━━━━━━━━━━
⚠️ <b>Task sẽ hết hạn trong vòng 24 giờ!</b>
━━━━━━━━━━━━━━━━━━━━
🔗 <a href="{task_url}">Xem ngay</a>
"""
                    send_message(msg.strip(), target_chat_id)
                    warnings_sent += 1
                    print(f"   ✅ Warning sent for task: {task_name}")
            
            except Exception as e:
                print(f"   ❌ Error processing task: {e}")
                continue
        
        print(f"   📊 Total warnings sent: {warnings_sent}")
        return 'OK', 200
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return 'ER', 500

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)