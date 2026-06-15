# Hệ thống Chat Realtime Phân Tán

## Kiến trúc
```
Clients (WebSocket)
     ↓
Nginx (Load Balancer - ip_hash sticky session)
     ↓          ↓          ↓
Server-1    Server-2    Server-3   (Node.js + Socket.IO)
     ↓          ↓          ↓
         Redis Pub/Sub              ← đồng bộ tin nhắn giữa các server
              ↓
         MongoDB                    ← lưu trữ tin nhắn
```

---

## Giai đoạn 1 — Chạy 1 server đơn (không cần Docker)

### Yêu cầu
- Node.js 18+
- MongoDB (chạy local hoặc dùng MongoDB Atlas)
- Redis (có thể bỏ qua ở giai đoạn này)

### Bước 1: Cài dependencies
```bash
cd server
npm install
```

### Bước 2: Tắt Redis tạm thời (nếu chưa có)
Trong `index.js`, comment dòng:
```js
// await connectRedis()
```

### Bước 3: Chạy server
```bash
cp .env.example .env
# Sửa MONGO_URI nếu cần
node index.js
```

### Bước 4: Mở client
Mở file `client/index.html` trong trình duyệt, nhập URL `http://localhost:3001`.

---

## Giai đoạn 2 — Chạy nhiều server + Redis (test phân tán)

### Yêu cầu thêm: Redis

**Cài Redis trên Mac:**
```bash
brew install redis && brew services start redis
```

**Cài Redis trên Ubuntu:**
```bash
sudo apt install redis-server && sudo systemctl start redis
```

### Chạy 3 server song song
```bash
# Terminal 1
PORT=3001 SERVER_ID=server-1 node server/index.js

# Terminal 2
PORT=3002 SERVER_ID=server-2 node server/index.js

# Terminal 3
PORT=3003 SERVER_ID=server-3 node server/index.js
```

### Test phân tán
1. Mở `client/index.html` lần 1 → kết nối vào `http://localhost:3001`, username=`Alice`
2. Mở `client/index.html` lần 2 → kết nối vào `http://localhost:3002`, username=`Bob`
3. Alice gửi tin nhắn → **Bob nhận được dù ở server khác** ✓
4. Mỗi tin nhắn hiển thị `serverHandled` — bằng chứng hai client ở 2 server khác nhau vẫn chat được

---

## Giai đoạn 3 — Load Balancer với Docker Compose

### Yêu cầu: Docker Desktop

```bash
# Khởi động toàn bộ hệ thống
docker compose up --build -d

# Kiểm tra
curl http://localhost/health
```

### Truy cập
| URL | Mô tả |
|-----|-------|
| `http://localhost` | Chat client |
| `http://localhost/health` | Health check (refresh nhiều lần → thấy server khác nhau) |
| `http://localhost/api/messages/general` | Lịch sử tin nhắn room general |

### Xem log thời gian thực
```bash
docker compose logs -f server1 server2 server3
```

### Monitor Redis Pub/Sub
```bash
docker compose exec redis redis-cli MONITOR
# Gửi 1 tin nhắn → thấy Redis broadcast đến các server
```

---

## Cách Redis Pub/Sub hoạt động

```
Client A (Server-1)  →  gửi "Hello"
                          ↓
Server-1 PUBLISH vào Redis channel: socket.io#/##room:general
                          ↓
Redis broadcast đến TẤT CẢ subscribers
                          ↓
Server-2, Server-3 SUBSCRIBE → nhận event → forward đến clients trong room
                          ↓
Client B (Server-2) nhận được "Hello"  ✓
Client C (Server-3) nhận được "Hello"  ✓
```

---

## Checklist báo cáo

- [x] Nhiều server chạy song song (horizontal scaling)
- [x] Redis Pub/Sub đồng bộ tin nhắn cross-server
- [x] Nginx Load Balancer với sticky session (ip_hash)
- [x] WebSocket proxy đúng chuẩn (Upgrade header)
- [x] Lưu trữ tin nhắn persistent (MongoDB)
- [x] Health check endpoint
- [x] Containerized với Docker Compose
