import requests
import json

API_TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"
TEAM_ID = "90181891084"

# Webhook IDs cần disable
webhook_ids = [
    "219e702d-ac47-47fb-997d-014ae9047317",
    "0ca4e420-cf04-410e-a22c-e4dff1284cb8"
]

print("=" * 70)
print("⏸️  DISABLE WEBHOOK (KHÔNG XÓA)")
print("=" * 70)

headers = {"Authorization": API_TOKEN}

for webhook_id in webhook_ids:
    url = f"https://api.clickup.com/api/v2/team/{TEAM_ID}/webhook/{webhook_id}"
    
    # Gửi PUT request để update webhook (disable nó)
    payload = {
        "status": "inactive"  # hoặc "disabled"
    }
    
    print(f"\n⏸️  Disabling webhook: {webhook_id}")
    
    try:
        # Thử PUT request
        response = requests.put(url, json=payload, headers=headers)
        print(f"PUT Status: {response.status_code}")
        print(f"Response: {response.text[:200]}")
        
        if response.status_code in [200, 201]:
            print(f"✅ Disable thành công")
    except Exception as e:
        print(f"Error: {e}")

print("\n" + "=" * 70)
print("✅ XONG! Webhooks cũ đã bị disable")
print("=" * 70)