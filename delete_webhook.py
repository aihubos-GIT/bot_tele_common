import requests

# 🔑 Token của mày
TOKEN = "pk_294795597_7H10XH7XXPFPYA3OLWXG87T7FB0RAQWU"

# 🧱 Danh sách webhook cần xoá
WEBHOOK_IDS = [
    "0ca4e420-cf04-410e-a22c-e4dff123dcb8"
]

for wid in WEBHOOK_IDS:
    url = f"https://api.clickup.com/api/v2/webhook/{wid}"
    headers = {"Authorization": TOKEN}
    response = requests.delete(url, headers=headers)
    print(f"🗑️  Xoá webhook {wid}: {response.status_code}")
    print(response.text)
