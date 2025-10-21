import requests
import json

API_TOKEN = "pk_294795597_OQHB3WHNCL5EN6MQQKTX1IUTHRHDY0EO"
TEAM_ID = "90181891084"

url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook"
headers = {"Authorization": API_TOKEN}

print("=" * 70)
print("🗑️ XÓA TẤT CẢ WEBHOOK CLICKUP")
print("=" * 70)

# Lấy danh sách webhook
response = requests.get(url, headers=headers)
webhooks = response.json().get("webhooks", [])

print(f"\n📋 Tìm thấy {len(webhooks)} webhook")

if not webhooks:
    print("✅ Không có webhook nào để xóa")
else:
    for i, webhook in enumerate(webhooks, 1):
        webhook_id = webhook.get("id")
        endpoint = webhook.get("endpoint")
        
        print(f"\n{i}. ID: {webhook_id}")
        print(f"   Endpoint: {endpoint}")
        
        # Xóa webhook
        delete_url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook/{webhook_id}"
        delete_response = requests.delete(delete_url, headers=headers)
        
        if delete_response.status_code == 200:
            print(f"   ✅ Đã xóa")
        else:
            print(f"   ❌ Lỗi: {delete_response.text}")

print("\n" + "=" * 70)
print("✅ HOÀN THÀNH! Bây giờ chạy reset_webhook.py để tạo webhook mới")
print("=" * 70)