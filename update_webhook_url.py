import requests
import json

API_TOKEN = "pk_294795597_OQHB3WHNCL5EN6MQQKTX1IUTHRHDY0EO"
TEAM_ID = "90181891084"

webhook_ids = [
    "219e702d-ac47-47fb-997d-014ae9047317",
    "0ca4e420-cf04-410e-a22c-e4dff1284cb8"
]

# Ngrok URL mới
ngrok_url = input("📡 Nhập ngrok URL mới (vd: https://xxxx-xxxx.ngrok-free.dev): ").strip()
new_endpoint = f"{ngrok_url}/clickup"

print("=" * 70)
print("🔄 CẬP NHẬT WEBHOOK URL")
print("=" * 70)

headers = {"Authorization": API_TOKEN}

for webhook_id in webhook_ids:
    url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook/{webhook_id}"
    
    payload = {
        "endpoint": new_endpoint
    }
    
    print(f"\n🔗 Updating webhook: {webhook_id}")
    print(f"   New endpoint: {new_endpoint}")
    
    try:
        response = requests.put(url, json=payload, headers=headers)
        
        print(f"   Status: {response.status_code}")
        
        if response.status_code in [200, 201]:
            print(f"   ✅ Update thành công!")
            print(f"   Response: {response.json()}")
        else:
            print(f"   Response: {response.text[:300]}")
    
    except Exception as e:
        print(f"   Error: {e}")

print("\n" + "=" * 70)
print("✅ XONG!")
print("=" * 70)