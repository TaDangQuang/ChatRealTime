#!/bin/bash
# ============================================================
#  Các lệnh hay dùng khi làm project chat phân tán
#  Chạy từ thư mục gốc chat-app/
# ============================================================

# ── Giai đoạn 1: Chạy thủ công (không Docker) ──────────────
# Cần cài MongoDB và Redis trên máy trước

run_local() {
  cd server && npm install
  # Chạy 3 server trên 3 cổng khác nhau
  PORT=3001 SERVER_ID=server-1 node index.js &
  PORT=3002 SERVER_ID=server-2 node index.js &
  PORT=3003 SERVER_ID=server-3 node index.js &
  echo "3 servers đang chạy: 3001, 3002, 3003"
  echo "Mở client/index.html rồi đổi Server URL để test"
}

# ── Giai đoạn 2&3: Chạy toàn bộ với Docker ─────────────────

# Build và khởi động tất cả
start_all() {
  docker compose up --build -d
  echo ""
  echo "Hệ thống đang chạy!"
  echo "  Chat UI:     http://localhost"
  echo "  Health:      http://localhost/health"
  echo "  Redis CLI:   docker compose exec redis redis-cli"
  echo "  Mongo shell: docker compose exec mongo mongosh chatapp"
}

# Xem log real-time của tất cả server
watch_logs() {
  docker compose logs -f server1 server2 server3
}

# ── Debug Redis Pub/Sub ─────────────────────────────────────
# Chạy lệnh này để thấy Redis nhận/gửi message gì
debug_redis() {
  echo "Theo dõi tất cả Redis commands (Ctrl+C để dừng):"
  docker compose exec redis redis-cli MONITOR
}

# Xem có bao nhiêu socket.io channel đang active
redis_channels() {
  docker compose exec redis redis-cli PUBSUB CHANNELS "*"
}

# ── Load test ───────────────────────────────────────────────
# Gửi 100 request đến health endpoint, xem phân phối về các server
load_test() {
  echo "Gửi 100 request, đếm phân phối theo server..."
  for i in $(seq 1 100); do
    curl -s http://localhost/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['server'])"
  done | sort | uniq -c | sort -rn
}

# ── Dừng hệ thống ───────────────────────────────────────────
stop_all() {
  docker compose down
}

# ── In hướng dẫn ────────────────────────────────────────────
echo "=== Chat App Helper ==="
echo "Các hàm có sẵn:"
echo "  run_local      - Chạy 3 server thủ công (không Docker)"
echo "  start_all      - Khởi động toàn bộ với Docker Compose"
echo "  watch_logs     - Xem log real-time"
echo "  debug_redis    - Monitor Redis pub/sub"
echo "  redis_channels - Xem các channel đang active"
echo "  load_test      - Test phân phối load balancer"
echo "  stop_all       - Dừng toàn bộ"
echo ""
echo "Ví dụ: source scripts.sh && start_all"
