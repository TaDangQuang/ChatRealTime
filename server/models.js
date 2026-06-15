const mongoose = require('mongoose')

// ─── User ────────────────────────────────────────────────────────────────────
const userSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true, trim: true },
  createdAt: { type: Date, default: Date.now }
})

// ─── Room ────────────────────────────────────────────────────────────────────
const roomSchema = new mongoose.Schema({
  name:      { type: String, required: true, unique: true, trim: true },
  members:   [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  createdAt: { type: Date, default: Date.now }
})

// ─── Message ─────────────────────────────────────────────────────────────────
const messageSchema = new mongoose.Schema({
  room:      { type: String, required: true },          // room name
  sender:    { type: String, required: true },          // username
  content:   { type: String, required: true },
  type:      { type: String, enum: ['text', 'system'], default: 'text' },
  createdAt: { type: Date, default: Date.now }
})

// Index để query tin nhắn theo room nhanh hơn
messageSchema.index({ room: 1, createdAt: -1 })

module.exports = {
  User:    mongoose.model('User', userSchema),
  Room:    mongoose.model('Room', roomSchema),
  Message: mongoose.model('Message', messageSchema)
}
