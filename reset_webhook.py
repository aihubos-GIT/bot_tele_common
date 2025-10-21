import requests
import json

API_TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"
TEAM_ID = "90181891084"

# LẤY NGROK URL TỪ INPUT
print("=" * 70)
print("⚠️  HƯU Ý: NGROK URL THAY ĐỔI MỖI LẦN KHỞI ĐỘNG!")
print("=" * 70)

ngrok_url = input("\n📡 Nhập ngrok URL mới (vd: https://xxxx-xxxx-xxxx.ngrok-free.dev): ").strip()

if not ngrok_url.startswith("https://"):
    print("❌ URL phải bắt đầu bằng https://")
    exit()

ENDPOINT = f"{ngrok_url}/clickup"

print(f"\n✅ Endpoint sẽ được set: {ENDPOINT}")
print("=" * 70)

# BƯỚC 1: LẤY DANH SÁCH WEBHOOK CÓ
print("\n1️⃣ Lấy danh sách webhook hiện tại...")

list_url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook"
headers = {"Authorization": API_TOKEN}

try:
    response = requests.get(list_url, headers=headers)
    webhooks = response.json().get("webhooks", [])
    print(f"✅ Tìm thấy {len(webhooks)} webhook")
    
    # BƯỚC 2: XÓA TẤT CẢ WEBHOOK CÓ
    if webhooks:
        print("\n2️⃣ Xóa webhook cũ...")
        for webhook in webhooks:
            webhook_id = webhook.get("id")
            delete_url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook/{webhook_id}"
            delete_response = requests.delete(delete_url, headers=headers)
            if delete_response.status_code == 200:
                print(f"   ✅ Xóa webhook: {webhook_id}")
            else:
                print(f"   ❌ Lỗi xóa webhook: {webhook_id}")
    
    # BƯỚC 3: TẠO WEBHOOK MỚI
    print("\n3️⃣ Tạo webhook mới...")
    
    create_url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook"
    payload = {
        "endpoint": ENDPOINT,
        "events": [
            "taskCreated",
            "taskUpdated",
            "taskDeleted",
            "taskCommentPosted"
        ]
    }
    
    create_response = requests.post(create_url, json=payload, headers=headers)
    
    if create_response.status_code in [200, 201]:
        webhook_data = create_response.json()
        print(f"✅ Webhook mới tạo thành công!")
        print(f"   ID: {webhook_data.get('id')}")
        print(f"   Endpoint: {webhook_data.get('endpoint')}")
        print(f"   Events: {webhook_data.get('events')}")
    else:
        print(f"❌ Lỗi: {create_response.text}")
    
    print("\n" + "=" * 70)
    print("✅ HOÀN THÀNH! Giờ tạo task trong ClickUp để test")
    print("=" * 70)
    
except Exception as e:
    print(f"❌ Lỗi: {e}")