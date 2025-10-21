import requests
import json

API_TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"
TEAM_ID = "90181891084"

url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook"

headers = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json"
}

print("📋 Đang lấy danh sách webhook...")
print("=" * 60)

try:
    response = requests.get(url, headers=headers)
    
    print(f"\n✅ Status Code: {response.status_code}")
    
    data = response.json()
    print(f"\n📝 Response:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    if "webhooks" in data:
        webhooks = data.get("webhooks", [])
        print(f"\n\n📊 Tổng số webhook: {len(webhooks)}")
        
        for i, webhook in enumerate(webhooks, 1):
            print(f"\n--- Webhook {i} ---")
            print(f"ID: {webhook.get('id')}")
            print(f"Endpoint: {webhook.get('endpoint')}")
            print(f"Events: {webhook.get('events')}")
            print(f"Active: {webhook.get('active', 'N/A')}")
    
except Exception as e:
    print(f"❌ Lỗi: {e}")