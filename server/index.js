require('dotenv').config()
const express    = require('express')
const http       = require('http')
const { Server } = require('socket.io')
const mongoose   = require('mongoose')
const { createClient } = require('redis')
const { createAdapter }= require('@socket.io/redis-adapter')
const cors       = require('cors')
const { Message } = require('./models')

const PORT      = process.env.PORT      || 3001
const SERVER_ID = process.env.SERVER_ID || 'server-1'

// ─── Express app ─────────────────────────────────────────────────────────────
const app    = express()
const server = http.createServer(app)

app.use(cors())
app.use(express.json())

// Health check — Load Balancer dùng endpoint này để kiểm tra server còn sống
app.get('/health', (req, res) => {
  res.json({ status: 'ok', server: SERVER_ID, port: PORT, uptime: process.uptime() })
})

// REST API: lấy lịch sử tin nhắn của một room
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

// ─── Kết nối Redis (chạy 2 client: pub + sub) ────────────────────────────────
async function connectRedis() {
  const pubClient = createClient({
    socket: { host: process.env.REDIS_HOST || 'localhost',
              port: process.env.REDIS_PORT || 6379 }
  })
  const subClient = pubClient.duplicate()

  await Promise.all([pubClient.connect(), subClient.connect()])

  // Gắn Redis adapter vào Socket.IO
  // Từ đây: io.to(room).emit() sẽ tự broadcast qua TẤT CẢ server instances
  io.adapter(createAdapter(pubClient, subClient))

  console.log(`[${SERVER_ID}] Redis adapter connected`)
  return { pubClient, subClient }
}

// ─── Socket.IO events ────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[${SERVER_ID}] Client connected: ${socket.id}`)

  // Client gửi username khi đăng nhập
  socket.on('register', ({ username }) => {
    socket.data.username = username
    console.log(`[${SERVER_ID}] User registered: ${username} (${socket.id})`)
  })

  // Client join vào một room
  socket.on('join_room', async ({ room }) => {
    socket.join(room)
    socket.data.room = room

    console.log(`[${SERVER_ID}] ${socket.data.username} joined room: ${room}`)

    // Thông báo cho mọi người trong room
    io.to(room).emit('user_joined', {
      username: socket.data.username,
      serverHandled: SERVER_ID,   // để demo biết server nào xử lý
      timestamp: new Date()
    })

    // Trả về lịch sử 50 tin nhắn gần nhất
    const history = await Message
      .find({ room })
      .sort({ createdAt: 1 })
      .limit(50)
    socket.emit('message_history', history)
  })

  // Client gửi tin nhắn
  socket.on('send_message', async ({ room, content }) => {
    const username = socket.data.username || 'Anonymous'

    // Lưu vào MongoDB
    const msg = await Message.create({ room, sender: username, content })

    // Broadcast đến tất cả client trong room (kể cả trên server khác nhờ Redis)
    io.to(room).emit('new_message', {
      _id:            msg._id,
      room,
      sender:         username,
      content,
      serverHandled:  SERVER_ID,   // demo: thấy tin nhắn đi qua server nào
      createdAt:      msg.createdAt
    })

    console.log(`[${SERVER_ID}] Message in ${room} from ${username}: ${content}`)
  })

  // Client rời room
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
  // Kết nối MongoDB
  await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/chatapp')
  console.log(`[${SERVER_ID}] MongoDB connected`)

  // Kết nối Redis (Giai đoạn 1: comment dòng này nếu chưa có Redis)
  //await connectRedis()

  server.listen(PORT, () => {
    console.log(`[${SERVER_ID}] Server running on http://localhost:${PORT}`)
    console.log(`[${SERVER_ID}] Health check: http://localhost:${PORT}/health`)
  })
}

start().catch(console.error)
