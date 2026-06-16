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
const RECONNECT_DELAY_MS = 3000

async function watchMessageChanges() {
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
    // Resume token quá cũ (MongoDB đã xoá oplog tương ứng) -> không thể resume.
    // Phải bắt đầu lại từ đầu (mất khả năng catch-up nhưng không crash server).
    console.error(`[${SERVER_ID}] Resume token invalid, starting fresh:`, err.message)
    changeStream = Message.watch([{ $match: { operationType: 'insert' } }])
  }

  changeStream.on('change', async (change) => {
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
    // upsert: tạo mới nếu chưa có, ghi đè nếu đã có.
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

  changeStream.on('error', (err) => {
    console.error(`[${SERVER_ID}] Change stream error:`, err.message)
    // Tự động thử kết nối lại sau RECONNECT_DELAY_MS giây — đây là phần
    // "tự phục hồi" khi mất kết nối tạm thời tới MongoDB.
    setTimeout(() => {
      console.log(`[${SERVER_ID}] Attempting to restart change stream...`)
      watchMessageChanges().catch(console.error)
    }, RECONNECT_DELAY_MS)
  })

  changeStream.on('close', () => {
    console.warn(`[${SERVER_ID}] Change stream closed unexpectedly`)
    setTimeout(() => {
      console.log(`[${SERVER_ID}] Attempting to restart change stream...`)
      watchMessageChanges().catch(console.error)
    }, RECONNECT_DELAY_MS)
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

    await Message.create({
      room,
      sender: username,
      content,
      originServer: SERVER_ID   // ghi server đã nhận tin nhắn gốc, dùng để demo
    })

    console.log(`[${SERVER_ID}] Message in ${room} from ${username}: ${content}`)
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