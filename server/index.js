require('dotenv').config()
const express    = require('express')
const http       = require('http')
const { Server } = require('socket.io')
const mongoose   = require('mongoose')
const cors       = require('cors')
const { Message, ChangeStreamCheckpoint } = require('./models')

const PORT      = process.env.PORT      || 3001
const SERVER_ID = process.env.SERVER_ID || 'server-1'

// ─── Express app ─────────────────────────────────────────────────────────────
const app    = express()
const server = http.createServer(app)

app.use(cors())
app.use(express.json())

app.get('/health', (req, res) => {
  res.json({ status: 'ok', server: SERVER_ID, port: PORT, uptime: process.uptime() })
})

app.get('/api/messages/:room', async (req, res) => {
  try {
    const messages = await Message
      .find({ room: req.params.room })
      .sort({ createdAt: 1 })
      .limit(50)
    res.json(messages)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─── Socket.IO ───────────────────────────────────────────────────────────────
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
})

// ─── Đồng bộ cross-server bằng MongoDB Change Streams ────────────────────────
// Thay thế Redis Pub/Sub: mỗi server "watch" collection messages.
// Khi BẤT KỲ server nào insert tin nhắn mới, TẤT CẢ server (kể cả chính nó)
// sẽ nhận được thông báo qua change stream và broadcast cho client local.
//
// FAULT TOLERANCE: dùng resume token để khi server này tắt rồi mở lại
// (failover/recovery), nó tiếp tục đọc đúng từ điểm đã dừng, không bỏ lỡ
// tin nhắn xảy ra trong lúc offline. Nếu mất kết nối, tự thử lại sau vài giây.
//
// CHỐNG VÒNG LẶP LỖI VÔ HẠN (bài học từ lần crash trước):
// - isWatcherStarting: chặn việc gọi watchMessageChanges() chồng lên nhau
//   khi cả 'error' và 'close' cùng bắn ra cho 1 lần lỗi.
// - reconnectAttempts: backoff tăng dần thay vì spam thử lại mỗi 3s, và có
//   giới hạn để không bao giờ gửi hàng trăm request lỗi/giây vào MongoDB.
// - Nếu lỗi liên quan tới session/connection pool đã chết (không chỉ riêng
//   change stream), phải mongoose.connect() lại từ đầu trước khi watch tiếp.
const BASE_RECONNECT_DELAY_MS = 3000
const MAX_RECONNECT_DELAY_MS  = 30000
const MAX_RECONNECT_ATTEMPTS  = 10

let isWatcherStarting   = false
let reconnectAttempts   = 0
let activeChangeStream  = null

function scheduleWatcherRestart(reason) {
  if (isWatcherStarting) return   // đã có 1 lần restart đang chờ, không xếp chồng thêm

  reconnectAttempts++
  if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
    console.error(
      `[${SERVER_ID}] Đã thử kết nối lại ${MAX_RECONNECT_ATTEMPTS} lần vẫn lỗi. ` +
      `Dừng tự động retry để tránh crash. Cần kiểm tra thủ công (mạng / MongoDB Atlas).`
    )
    return
  }

  // Backoff tăng dần: 3s, 6s, 12s, 24s... tối đa 30s — tránh dội lỗi liên tục
  const delay = Math.min(BASE_RECONNECT_DELAY_MS * 2 ** (reconnectAttempts - 1), MAX_RECONNECT_DELAY_MS)
  console.log(`[${SERVER_ID}] ${reason}. Thử kết nối lại sau ${delay / 1000}s (lần ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`)

  isWatcherStarting = true
  setTimeout(async () => {
    try {
      // Nếu kết nối Mongoose gốc đã chết (readyState !== 1 = connected),
      // phải reconnect lại TOÀN BỘ trước khi mở change stream mới —
      // đây là phần còn thiếu ở bản trước, gây lỗi MongoExpiredSessionError lặp lại.
      if (mongoose.connection.readyState !== 1) {
        console.log(`[${SERVER_ID}] Kết nối MongoDB đã đóng, đang kết nối lại...`)
        await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/chatapp')
        console.log(`[${SERVER_ID}] MongoDB reconnected`)
      }
      await watchMessageChanges()
    } catch (err) {
      console.error(`[${SERVER_ID}] Reconnect thất bại:`, err.message)
      isWatcherStarting = false
      scheduleWatcherRestart('Reconnect thất bại')
    }
  }, delay)
}

async function watchMessageChanges() {
  isWatcherStarting = false

  // Đóng watcher cũ nếu còn sót (phòng trường hợp gọi hàm này 2 lần)
  if (activeChangeStream) {
    try { await activeChangeStream.close() } catch (_) { /* đã chết thì thôi */ }
    activeChangeStream = null
  }

  // Đọc resume token đã lưu lần trước (nếu có) — đây là "bookmark"
  const checkpoint = await ChangeStreamCheckpoint.findOne({ serverId: SERVER_ID })
  const options = checkpoint
    ? { resumeAfter: checkpoint.resumeToken }
    : {}

  if (checkpoint) {
    console.log(`[${SERVER_ID}] Resuming change stream from saved checkpoint`)
  } else {
    console.log(`[${SERVER_ID}] No checkpoint found, starting fresh`)
  }

  let changeStream
  try {
    changeStream = Message.watch([{ $match: { operationType: 'insert' } }], options)
  } catch (err) {
    // Resume token quá cũ (MongoDB Atlas đã xoá phần oplog tương ứng, thường
    // do oplog chỉ giữ trong ~24h) -> không thể resume.
    // QUAN TRỌNG: phải XOÁ checkpoint hỏng khỏi DB, nếu không thì lần
    // scheduleWatcherRestart() tiếp theo sẽ đọc lại đúng token chết này và
    // lặp lại lỗi y hệt mãi mãi (đây là bug đã gây crash ở lần trước).
    console.error(`[${SERVER_ID}] Resume token invalid, xoá checkpoint cũ và bắt đầu lại từ đầu:`, err.message)
    try {
      await ChangeStreamCheckpoint.deleteOne({ serverId: SERVER_ID })
    } catch (delErr) {
      console.error(`[${SERVER_ID}] Không xoá được checkpoint hỏng:`, delErr.message)
    }
    changeStream = Message.watch([{ $match: { operationType: 'insert' } }])
  }

  activeChangeStream = changeStream

  changeStream.on('change', async (change) => {
    // Stream đang nhận event bình thường -> reset bộ đếm lỗi
    reconnectAttempts = 0

    const msg = change.fullDocument

    io.to(msg.room).emit('new_message', {
      _id:           msg._id,
      room:          msg.room,
      sender:        msg.sender,
      content:       msg.content,
      serverHandled: msg.originServer,
      createdAt:     msg.createdAt
    })

    // Lưu lại resume token sau mỗi sự kiện đã xử lý thành công.
    try {
      await ChangeStreamCheckpoint.updateOne(
        { serverId: SERVER_ID },
        { resumeToken: change._id, updatedAt: new Date() },
        { upsert: true }
      )
    } catch (err) {
      console.error(`[${SERVER_ID}] Failed to save checkpoint:`, err.message)
    }
  })

  changeStream.on('error', async (err) => {
    console.error(`[${SERVER_ID}] Change stream error:`, err.message)
    activeChangeStream = null

    // Lỗi này có thể bắn ra SAU KHI stream đã chạy (không chỉ lúc gọi .watch()),
    // vì driver xác nhận resume token hỏng bất đồng bộ với MongoDB Atlas.
    // Phải xoá checkpoint hỏng ở đây nữa, không chỉ ở nhánh try/catch phía trên,
    // nếu không scheduleWatcherRestart() sẽ đọc lại đúng token chết này mãi.
    if (err.message && err.message.includes('no longer be in the oplog')) {
      console.error(`[${SERVER_ID}] Resume token đã hết hạn (oplog không còn giữ) — xoá checkpoint cũ`)
      try {
        await ChangeStreamCheckpoint.deleteOne({ serverId: SERVER_ID })
      } catch (delErr) {
        console.error(`[${SERVER_ID}] Không xoá được checkpoint hỏng:`, delErr.message)
      }
    }

    scheduleWatcherRestart('Change stream gặp lỗi')
  })

  changeStream.on('close', () => {
    console.warn(`[${SERVER_ID}] Change stream closed unexpectedly`)
    activeChangeStream = null
    scheduleWatcherRestart('Change stream bị đóng')
  })

  console.log(`[${SERVER_ID}] Watching MongoDB change stream for messages`)
  return changeStream
}

// ─── Socket.IO events ────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[${SERVER_ID}] Client connected: ${socket.id}`)

  socket.on('register', ({ username }) => {
    socket.data.username = username
    console.log(`[${SERVER_ID}] User registered: ${username} (${socket.id})`)
  })

  socket.on('join_room', async ({ room }) => {
    socket.join(room)
    socket.data.room = room

    console.log(`[${SERVER_ID}] ${socket.data.username} joined room: ${room}`)

    // Lưu ý: 'user_joined' chỉ broadcast trong server hiện tại (không qua Change Stream).
    // Nếu cần đồng bộ presence cross-server, phải ghi event này vào một collection riêng
    // và watch nó tương tự như messages.
    io.to(room).emit('user_joined', {
      username: socket.data.username,
      serverHandled: SERVER_ID,
      timestamp: new Date()
    })

    const history = await Message
      .find({ room })
      .sort({ createdAt: 1 })
      .limit(50)
    socket.emit('message_history', history)
  })

  // Client gửi tin nhắn — CHỈ insert vào MongoDB.
  // Việc emit 'new_message' cho client được xử lý bởi watchMessageChanges()
  // ở TRÊN, để đảm bảo mọi server (gồm cả server gốc) đều emit qua đúng 1 con đường.
  socket.on('send_message', async ({ room, content }) => {
    const username = socket.data.username || 'Anonymous'

    try {
      await Message.create({
        room,
        sender: username,
        content,
        originServer: SERVER_ID   // ghi server đã nhận tin nhắn gốc, dùng để demo
      })
      console.log(`[${SERVER_ID}] Message in ${room} from ${username}: ${content}`)
    } catch (err) {
      // Trước đây lỗi ở đây (vd. MongoExpiredSessionError lúc connection pool
      // vừa chết) sẽ lọt thẳng ra ngoài và crash hẳn Node.js process.
      // Giờ chỉ báo lỗi cho đúng client đó, server vẫn tiếp tục chạy.
      console.error(`[${SERVER_ID}] Gửi tin nhắn thất bại:`, err.message)
      socket.emit('send_message_error', { error: 'Không thể gửi tin nhắn, vui lòng thử lại.' })
    }
  })

  socket.on('leave_room', ({ room }) => {
    socket.leave(room)
    io.to(room).emit('user_left', {
      username: socket.data.username,
      timestamp: new Date()
    })
  })

  socket.on('disconnect', () => {
    console.log(`[${SERVER_ID}] Client disconnected: ${socket.id}`)
    if (socket.data.room) {
      io.to(socket.data.room).emit('user_left', {
        username: socket.data.username,
        timestamp: new Date()
      })
    }
  })
})

// ─── Bootstrap ───────────────────────────────────────────────────────────────
async function start() {
  await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/chatapp')
  console.log(`[${SERVER_ID}] MongoDB connected`)

  // Thay cho connectRedis(): mỗi server tự watch change stream của riêng nó,
  // có resume token để không mất tin nhắn khi tắt/mở lại
  await watchMessageChanges()

  server.listen(PORT, () => {
    console.log(`[${SERVER_ID}] Server running on http://localhost:${PORT}`)
    console.log(`[${SERVER_ID}] Health check: http://localhost:${PORT}/health`)
  })
}

start().catch(console.error)

// ─── Chống crash toàn bộ process do lỗi MongoDB lọt ra ngoài ─────────────────
// Lần trước, lỗi MongoExpiredSessionError lọt ra khỏi mọi try/catch và làm
// crash hẳn Node.js process (server tắt hoàn toàn, không còn cách phục hồi
// tự động). Đây là lớp bảo vệ cuối cùng: log lỗi lại, không để Node.js thoát
// đột ngột — quan trọng đặc biệt với socket.emit('send_message') vốn không
// có try/catch khi insert vào MongoDB.
process.on('uncaughtException', (err) => {
  console.error(`[${SERVER_ID}] Uncaught exception (server vẫn tiếp tục chạy):`, err.message)
})

process.on('unhandledRejection', (err) => {
  console.error(`[${SERVER_ID}] Unhandled rejection (server vẫn tiếp tục chạy):`, err.message || err)
})

// ─── Graceful shutdown ────────────────────────────────────────────────────────
// Khi server bị dừng (Ctrl+C, hoặc orchestrator gửi SIGTERM khi restart),
// đóng kết nối sạch sẽ thay vì để treo. Resume token đã lưu trong DB nên
// lần khởi động lại sẽ tự đọc tiếp từ đúng vị trí.
process.on('SIGINT', async () => {
  console.log(`[${SERVER_ID}] Shutting down gracefully...`)
  await mongoose.disconnect()
  process.exit(0)
})

process.on('SIGTERM', async () => {
  console.log(`[${SERVER_ID}] Received SIGTERM, shutting down...`)
  await mongoose.disconnect()
  process.exit(0)
})