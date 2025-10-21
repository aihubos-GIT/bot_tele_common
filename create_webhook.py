import requests
import json

API_TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"
TEAM_ID = "90181891084"
NGROK_URL = "https://suellen-overhomely-destinee.ngrok-free.dev"

# URL để tạo webhook
url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook"

headers = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json"
}

# Payload - các event cần monitoring
payload = {
    "endpoint": f"{NGROK_URL}/clickup",
    "events": [
        "taskCreated",
        "taskUpdated", 
        "taskDeleted",
        "taskCommentPosted"
    ]
}

print("🚀 Đang tạo webhook...")
print(f"📡 Webhook URL: {payload['endpoint']}")
print(f"📋 Events: {', '.join(payload['events'])}")
print("=" * 50)

try:
    response = requests.post(url, json=payload, headers=headers)
    
    print(f"\n✅ Status Code: {response.status_code}")
    print(f"📝 Response:")
    print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    
    if response.status_code in [200, 201]:
        webhook_id = response.json().get("id")
        print(f"\n✅ Webhook tạo thành công!")
        print(f"🆔 Webhook ID: {webhook_id}")
        print(f"💾 Lưu lại ID này nếu muốn xóa webhook sau!")
    else:
        print(f"\n❌ Lỗi: {response.text}")
        
except Exception as e:
    print(f"❌ Lỗi kết nối: {e}")