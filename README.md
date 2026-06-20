# Hệ thống Chat Realtime Phân Tán

## Kiến trúc

```
Clients (WebSocket / Socket.IO)
     ↓
Nginx (Load Balancer — ip_hash sticky session + auto failover)
     ↓          ↓          ↓
Server-1    Server-2    Server-3   (Node.js + Socket.IO)
     ↓          ↓          ↓
         MongoDB Change Streams      ← đồng bộ tin nhắn giữa các server
              ↓
         MongoDB (Atlas)             ← lưu trữ tin nhắn + resume token
```

## Tính năng

### Tính năng cốt lõi

- **Chat realtime qua WebSocket** — gửi/nhận tin nhắn ngay lập tức trong
  cùng 1 room, không cần reload trang.
- **Đăng ký username đơn giản** — không cần tài khoản/mật khẩu.
- **Join/leave room theo tên** — kèm thông báo hệ thống khi có người vào/ra.
- **Lưu lịch sử tin nhắn** — mọi tin nhắn được ghi vào MongoDB; khi vào
  room, client tự động tải 50 tin nhắn gần nhất.
- **REST API lấy lịch sử** — `GET /api/messages/:room`, dùng được độc lập
  với WebSocket.
- **Health check endpoint** — `GET /health`, trả về trạng thái + server ID
  + uptime, dùng để Nginx/giám sát kiểm tra server còn sống.

### Tính năng phân tán (Distributed System)

- **Nhiều server song song** — 3 instance Node.js độc lập, chia tải qua
  Nginx, mở rộng được theo chiều ngang (horizontal scaling).
- **Đồng bộ tin nhắn cross-server bằng MongoDB Change Streams** — mỗi
  server "watch" collection `messages`; khi bất kỳ server nào insert tin
  nhắn mới, tất cả server khác đều phát hiện và broadcast cho client của
  mình. Người dùng kết nối vào server khác nhau vẫn thấy đúng và đủ tin
  nhắn của nhau theo thời gian thực.
- **Nginx load balancer với `ip_hash`** — đảm bảo sticky session, bắt buộc
  với WebSocket vì đây là kết nối lâu dài (persistent connection), không
  thể đổi server giữa phiên như HTTP request thường.

### Tính năng chịu lỗi (Fault Tolerance) — mới bổ sung

Hệ thống được thiết kế để khi 1 server gặp sự cố, dịch vụ vẫn tiếp tục
hoạt động và tự phục hồi mà không cần can thiệp thủ công, thông qua 3 lớp
phối hợp:

1. **Lớp dữ liệu — Resume Token cho Change Stream**
   Mỗi server lưu lại vị trí đã xử lý (resume token) vào collection
   `ChangeStreamCheckpoint` sau mỗi tin nhắn. Khi server tắt rồi khởi động
   lại, nó đọc token này để tiếp tục đúng từ điểm dừng — không bỏ lỡ tin
   nhắn xảy ra trong lúc offline. Nếu kết nối Change Stream bị lỗi/đứt bất
   ngờ, server tự động thử kết nối lại sau vài giây.

2. **Lớp hạ tầng — Nginx tự phát hiện và tránh server chết**
   Cấu hình `max_fails` + `fail_timeout` giúp Nginx tự đánh dấu 1 server là
   "down" sau một số lần request thất bại liên tiếp, tạm ngừng gửi traffic
   mới tới nó. Kết hợp `proxy_next_upstream`, Nginx tự động chuyển các HTTP
   request (API, health check) sang server còn sống ngay trong cùng 1
   request, client không hề biết có sự cố xảy ra.

3. **Lớp client — Socket.IO tự reconnect và tự vào lại room**
   WebSocket là kết nối liên tục, nên khi server xử lý nó sập, kết nối đó
   đứt vĩnh viễn (không "retry" được như HTTP). Client phát hiện mất kết
   nối, tự động thử kết nối lại (`reconnectionAttempts: Infinity`), khi
   thành công sẽ tự gọi lại `register` + `join_room` — vì kết nối mới có
   `socket.id` khác, server không còn nhớ phiên cũ. UI hiển thị rõ trạng
   thái "Đang kết nối lại..." để người dùng biết hệ thống đang tự khôi
   phục.

Cả 3 lớp trên đã được kiểm thử thủ công: tắt 1 server đang xử lý người
dùng, xác nhận (a) MongoDB không mất tin nhắn gửi trong lúc server offline,
(b) `curl /health` qua Nginx vẫn trả 200 từ server khác, (c) client tự
chuyển trạng thái "Mất kết nối → Đang kết nối lại → Đã kết nối lại" và tiếp
tục chat được mà không cần thao tác thủ công.

---

## Giai đoạn 1 — Chạy 1 server đơn (không cần Docker)

### Yêu cầu
- Node.js 18+
- MongoDB (local hoặc MongoDB Atlas)

### Bước 1: Cài dependencies
```bash
cd server
npm install
```

### Bước 2: Cấu hình `.env`
```bash
cp .env.example .env
```
Sửa `MONGO_URI` theo đúng kết nối MongoDB của bạn (local hoặc Atlas).

### Bước 3: Chạy server
```bash
node index.js
```
Log mong đợi:
```
[server-1] MongoDB connected
[server-1] No checkpoint found, starting fresh
[server-1] Watching MongoDB change stream for messages
[server-1] Server running on http://localhost:3001
```

### Bước 4: Mở client
Mở `client/index.html` trong trình duyệt, nhập URL `http://localhost:3001`,
username và room bất kỳ → Kết nối.

---

## Giai đoạn 2 — Chạy nhiều server (test đồng bộ phân tán)

Không cần cài thêm gì — MongoDB Atlas mặc định đã hỗ trợ Change Streams
(chạy ở dạng Replica Set).

### Chạy 3 server song song

**Terminal 1:**
```bash
cd server
node index.js
```

**Terminal 2 (Windows PowerShell):**
```powershell
cd server
$env:PORT="3002"; $env:SERVER_ID="server-2"; $env:MONGO_URI="<your-mongo-uri>"
node index.js
```

**Terminal 3 (Windows PowerShell):**
```powershell
cd server
$env:PORT="3003"; $env:SERVER_ID="server-3"; $env:MONGO_URI="<your-mongo-uri>"
node index.js
```

(Trên macOS/Linux dùng `PORT=3002 SERVER_ID=server-2 MONGO_URI=... node index.js`)

### Test phân tán
1. Mở `client/index.html` lần 1 → kết nối `http://localhost:3001`, username `Alice`
2. Mở `client/index.html` lần 2 → kết nối `http://localhost:3002`, username `Bob`
3. Alice gửi tin nhắn → **Bob nhận được dù ở server khác** ✓
4. Mỗi tin nhắn hiển thị `serverHandled` — bằng chứng 2 client ở 2 server
   khác nhau vẫn đồng bộ được nhờ MongoDB Change Streams.

### Test khả năng chịu lỗi (failover + recovery)
1. Gửi 1 tin nhắn từ server-2 (để checkpoint được lưu lần đầu)
2. Tắt server-2 (Ctrl+C)
3. Gửi vài tin nhắn từ server-1/server-3 trong lúc server-2 đang tắt
4. Khởi động lại server-2 (`node index.js`)
5. Log phải hiện `Resuming change stream from saved checkpoint` (thay vì
   `No checkpoint found`)
6. Client kết nối lại server-2 phải thấy đầy đủ các tin nhắn đã gửi trong
   lúc nó offline.

---

## Giai đoạn 3 — Load Balancer với Nginx

### Windows (không cần Docker)
1. Tải Nginx for Windows bản binary tại https://nginx.org/en/download.html
   (cột **Stable** → link `nginx/Windows-x.xx.x`)
2. Giải nén, sửa `conf/nginx.conf` theo file `nginx-windows.conf` trong
   repo (đổi `root` đúng đường dẫn thư mục `client` trên máy bạn)
3. Đảm bảo cả 3 server (3001–3003) đang chạy
4. Chạy Nginx:
   ```powershell
   cd C:\nginx-x.xx.x
   .\nginx.exe
   ```
5. Mở `http://localhost:8080` (qua Nginx, **không** mở trực tiếp file HTML)

### Docker Compose (khi có Docker/WSL2)
```bash
docker compose up --build -d
curl http://localhost/health
```

### Truy cập
| URL | Mô tả |
|-----|-------|
| `http://localhost:8080` (Windows) hoặc `http://localhost` (Docker) | Chat client qua Nginx |
| `.../health` | Health check (gọi nhiều lần → cùng 1 IP luôn ra cùng 1 server nhờ `ip_hash`) |
| `.../api/messages/general` | Lịch sử tin nhắn room `general` |

### Test failover qua Nginx
1. Mở `http://localhost:8080`, kết nối, gửi 1 tin nhắn, xem "Xử lý bởi:
   server-X" để biết bạn đang bị `ip_hash` khoá vào server nào
2. Tắt đúng server đó (Ctrl+C)
3. Quan sát: trạng thái chuyển "Mất kết nối → Đang kết nối lại... → Đã kết
   nối lại ✓" tự động, không cần bấm gì
4. Gửi tin nhắn mới — vẫn gửi được, "Xử lý bởi" đổi sang server còn sống

---

## Cách MongoDB Change Streams hoạt động (thay thế Redis Pub/Sub)

```
Client A (Server-1)  →  gửi "Hello"
                          ↓
Server-1 chỉ INSERT tin nhắn vào MongoDB (KHÔNG emit trực tiếp)
                          ↓
MongoDB ghi vào oplog (Replica Set)
                          ↓
Change Stream trên CẢ 3 server đều phát hiện document mới
                          ↓
Mỗi server tự emit cho client đang kết nối với mình
                          ↓
Client B (Server-2) nhận được "Hello"  ✓
Client C (Server-3) nhận được "Hello"  ✓

Đồng thời: mỗi server lưu resume token sau khi xử lý xong,
để không mất sự kiện nếu phải tắt/mở lại.
```

---

## Cấu trúc thư mục

```
.
├── server/
│   ├── index.js        # Entry point: Express + Socket.IO + Change Stream watcher
│   ├── models.js        # Schema: User, Room, Message, ChangeStreamCheckpoint
│   ├── package.json
│   └── .env.example
├── client/
│   └── index.html        # Client thuần HTML/JS, dùng Socket.IO client + auto-reconnect
├── nginx.conf            # Cấu hình Nginx cho Docker (server1/server2/server3)
├── nginx-windows.conf    # Cấu hình Nginx chạy trực tiếp trên Windows (127.0.0.1:PORT)
├── docker-compose.yml
└── README.md
```

## Video demo
Link video: https://drive.google.com/file/d/1dSSS_JEUbmk1O9jPesKePtjlnrF4Hr_o/view?usp=sharing