import requests

# Thay token của bạn vào đây
TOKEN = "pk_294795597_OJ9QUW50XMB73LF6UO0EEZFO4EP7JDZR"

headers = {
    "Authorization": TOKEN,
    "Content-Type": "application/json"
}

print("=" * 50)
print("🧪 TEST CLICKUP TOKEN")
print("=" * 50)

# Test 1: Get Team
print("\n1️⃣ Testing: GET /api/v2/team")
try:
    response = requests.get("https://api.clickup.com/api/v2/team", headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(f"   ✅ Token valid!")
        data = response.json()
        print(f"   Teams: {data}")
    else:
        print(f"   ❌ Error: {response.text}")
except Exception as e:
    print(f"   ❌ Exception: {e}")

# Test 2: Get specific task
print("\n2️⃣ Testing: GET /api/v2/task/{TASK_ID}")
TASK_ID = "86ev6qvee"  # Thay bằng task ID thực của bạn
try:
    response = requests.get(f"https://api.clickup.com/api/v2/task/{TASK_ID}", headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(f"   ✅ Can fetch tasks!")
        data = response.json()
        print(f"   Task name: {data.get('name')}")
    else:
        print(f"   ❌ Error: {response.text}")
except Exception as e:
    print(f"   ❌ Exception: {e}")

print("\n" + "=" * 50)