# 🤖 ClickUp ↔ Telegram Bot

Tự động hóa thông báo từ ClickUp sang Telegram. Bot sẽ gửi thông báo tức thì khi có bất kỳ thay đổi nào trong tasks và tổng hợp báo cáo hàng ngày lúc 22h.

---

## ✨ Chức năng

### 1. **Thông báo tức thì** 🔔
Bot sẽ gửi thông báo đến Telegram khi:
- ✅ **Task mới được tạo** - Hiển thị: tên task, người tạo, người được phân công, mức độ ưu tiên, deadline
- ✅ **Task hoàn thành** - Hiển thị trạng thái (trễ/đúng tiến độ/sớm), thời gian hoàn thành
- ✅ **Thay đổi trạng thái** - Khi status thay đổi
- ✅ **Phân công task** - Khi có người được giao task mới
- ✅ **Xóa phân công** - Khi xóa người phụ trách
- ✅ **Thay đổi deadline** - Khi deadline được cập nhật
- ✅ **Cảnh báo quá hạn** - Khi task chưa hoàn thành nhưng đã quá deadline
- ✅ **Comment mới** - Khi có người bình luận trên task

### 2. **Tổng hợp hàng ngày** 📊
Lúc 22h hàng ngày, bot sẽ gửi báo cáo:
- Danh sách task **đã hoàn thành** trong ngày
- Danh sách task **chưa hoàn thành** với tiến độ hiện tại
- Thống kê tổng hợp theo từng người

---

## 🚀 Cài đặt

### Yêu cầu
- Python 3.8+
- pip (package manager)

### Bước 1: Clone hoặc tải code

```bash
git clone <repo-url>
cd clickup-telegram-bot
```

### Bước 2: Cài đặt dependencies

```bash
pip install flask requests python-dotenv
```

### Bước 3: Tạo file `.env`

Tạo file `.env` trong thư mục root với nội dung:

```env
BOT_TOKEN=7743481184:AAG7mt4MYz4XBGb1-SeHd0nLMy2TM6OVxys
CHAT_ID=-1003065878488
CLICKUP_API_TOKEN=pk_294795597_J765YB5QS2IERBZ50NK3OI5GK37B0MNZ
```

**Hướng dẫn lấy token:**

- **BOT_TOKEN**: Từ BotFather trên Telegram (`/start` → `/newbot`)
- **CHAT_ID**: ID của group Telegram (có thể là số âm)
- **CLICKUP_API_TOKEN**: Từ ClickUp Settings → Integrations → API → Create Token

### Bước 4: Khởi động bot

```bash
python app.py
```

Bạn sẽ thấy:
```
🔍 KIỂM TRA CONFIG:
BOT_TOKEN: 7743481184:AAG7mt4MYz4...
CHAT_ID: -1003065878488
CLICKUP_API_TOKEN: pk_294795597_J765YB5QS2...
==================================================
```

---

## 🌐 Thiết lập Webhook

### Dùng ngrok (để test trên localhost)

**Bước 1: Cài đặt ngrok**
```bash
# macOS
brew install ngrok

# Windows - download từ https://ngrok.com/download
```

**Bước 2: Chạy ngrok**
```bash
ngrok http 5000
```

Bạn sẽ thấy:
```
Forwarding    https://abc1234-ef56.ngrok-free.dev -> http://localhost:5000
```

**Bước 3: Tạo webhook**

Chạy script `reset_webhook.py` và nhập ngrok URL:
```bash
python reset_webhook.py
# Nhập: https://abc1234-ef56.ngrok-free.dev
```

### Deploy lên server (Production)

Nếu deploy lên VPS/server thực:
1. Thay ngrok URL bằng domain của server
2. Cập nhật webhook bằng script

---

## 📁 Cấu trúc file

```
.
├── app.py                    # Bot chính
├── create_webhook.py         # Script tạo webhook
├── reset_webhook.py          # Script xóa + tạo webhook lại
├── list_webhooks.py          # Script xem danh sách webhook
├── delete_webhook.py         # Script xóa webhook
├── .env                      # Config (không commit)
├── .gitignore               # Git ignore
├── clickup_data.json        # Log webhook (debug)
└── README.md                # File này
```

---

## 🔧 Cách sử dụng

### Test bot
```bash
# Gửi test message
curl http://localhost:5000/test

# Hoặc vào browser
http://localhost:5000/test
```

### Kiểm tra bot hoạt động
1. Tạo task mới trong ClickUp
2. Kiểm tra Telegram có nhận thông báo
3. Xem terminal của bot có log webhook

### Debug
- File `clickup_data.json` chứa tất cả webhook nhận được
- Terminal in ra detail của mỗi webhook

---

## 🎯 Các sự kiện được hỗ trợ

| Event | Mô tả |
|-------|-------|
| `taskCreated` | Task mới được tạo |
| `taskUpdated` | Task được cập nhật (status, assignee, deadline, comment...) |
| `taskDeleted` | Task bị xóa |
| `taskCommentPosted` | Có comment mới trên task |

---

## 📊 Báo cáo hàng ngày (22h)

Bot sẽ tự động gửi báo cáo:

```
📊 TỔNG HỢP TASK - 17/10/2025
━━━━━━━━━━━━━━━━━━━━

✅ ĐÃ HOÀN THÀNH (3):
• Task 1 - hoàn thành sớm 2 ngày
• Task 2 - hoàn thành đúng tiến độ
• Task 3 - hoàn thành trễ 5 giờ

⏳ CHƯA HOÀN THÀNH (2):
• Task 4 - 50% (Người phụ trách: admin)
• Task 5 - Chưa bắt đầu (quá hạn!)

📈 THỐNG KÊ:
• Tổng task ngày hôm nay: 5
• Hoàn thành: 3 (60%)
• Chưa hoàn thành: 2 (40%)
```

---

## ⚙️ Cấu hình Priority

| ID | Ký hiệu | Mô tả |
|-----|---------|-------|
| 1 | 🔴 | Khẩn cấp |
| 2 | 🟠 | Cao |
| 3 | 🟡 | Bình thường |
| 4 | 🔵 | Thấp |

---

## 🆘 Troubleshooting

### Bot không nhận webhook

**Vấn đề:** ClickUp không gửi webhook đến bot

**Giải pháp:**
1. Kiểm tra ngrok URL còn active: `ngrok http 5000`
2. Xóa webhook cũ và tạo lại: `python reset_webhook.py`
3. Kiểm tra Bot Token và Chat ID đúng không
4. Xem log file `clickup_data.json`

### Telegram không nhận thông báo

**Vấn đề:** Bot gửi thông báo nhưng Telegram không hiển thị

**Giải pháp:**
1. Kiểm tra BOT_TOKEN đúng: `python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('BOT_TOKEN'))"`
2. Kiểm tra CHAT_ID đúng (phải có dấu trừ `-` nếu là group)
3. Bot phải là member của group
4. Test bằng curl:
   ```bash
   curl -X POST "https://api.telegram.org/bot<BOT_TOKEN>/sendMessage" \
     -H "Content-Type: application/json" \
     -d '{"chat_id": "<CHAT_ID>", "text": "Test"}'
   ```

### Priority không hiển thị

**Giải pháp:** Priority không được set trong ClickUp sẽ hiển thị "Không có"

---

## 📝 Logs

Bot tự động log tất cả webhook vào file `clickup_data.json`. Để xóa log:

```bash
rm clickup_data.json
```

---

## 🔐 Bảo mật

⚠️ **KHÔNG commit file `.env` lên Git!**

Thêm vào `.gitignore`:
```
.env
clickup_data.json
```

---

## 📞 Hỗ trợ

Nếu gặp vấn đề:
1. Kiểm tra `clickup_data.json` xem webhook nhận được gì
2. Xem terminal bot in ra gì
3. Kiểm tra logs của ClickUp API

---

## 📄 License

MIT License

---

**Tạo bởi:** Your Name  
**Cập nhật lần cuối:** 17/10/2025