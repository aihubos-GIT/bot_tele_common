import requests
import json

API_TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"

# Dùng task ID từ một trong những tasks của bạn
task_id = "input_task_id_here"  # Thay thành task ID thực tế

url = f"https://api.clickup.com/api/v2/task/{task_id}"
headers = {"Authorization": API_TOKEN}

try:
    response = requests.get(url, headers=headers)
    data = response.json()
    
    print("\n========== FULL TASK DATA ==========")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    print("\n========== PRIORITY FIELD ==========")
    priority = data.get("priority")
    print(f"Priority value: {priority}")
    print(f"Priority type: {type(priority)}")
    print(f"Priority raw: {repr(priority)}")
    
except Exception as e:
    print(f"Error: {e}")