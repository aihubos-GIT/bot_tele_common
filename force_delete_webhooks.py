import requests
import json

API_TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"
TEAM_ID = "90181891084"

# Webhook IDs cần xóa (từ output bạn vừa gửi)
webhook_ids = [
    "219e702d-ac47-47fb-997d-014ae9047317",
    "0ca4e420-cf04-410e-a22c-e4dff1284cb8"
]

print("=" * 70)
print("🗑️ XÓA WEBHOOK TRỰC TIẾP")
print("=" * 70)

headers = {"Authorization": API_TOKEN}

for webhook_id in webhook_ids:
    url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook/{webhook_id}"
    
    print(f"\n🔗 URL: {url}")
    print(f"🔑 Token: {API_TOKEN[:20]}...")
    
    try:
        response = requests.delete(url, headers=headers)
        
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            print(f"✅ Xóa webhook {webhook_id} thành công")
        else:
            print(f"❌ Lỗi: {response.text}")
    
    except Exception as e:
        print(f"❌ Exception: {e}")

print("\n" + "=" * 70)
print("✅ XONG! Kiểm tra lại webhook bằng lệnh:")
print("   python list_webhooks.py")
print("=" * 70)