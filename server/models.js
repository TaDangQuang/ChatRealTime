const mongoose = require('mongoose')

const userSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true, trim: true },
  createdAt: { type: Date, default: Date.now }
})

const roomSchema = new mongoose.Schema({
  name:      { type: String, required: true, unique: true, trim: true },
  members:   [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  createdAt: { type: Date, default: Date.now }
})

const messageSchema = new mongoose.Schema({
  room:         { type: String, required: true },         
  sender:       { type: String, required: true },          
  content:      { type: String, required: true },
  type:         { type: String, enum: ['text', 'system'], default: 'text' },
  originServer: { type: String },                        
  createdAt:    { type: Date, default: Date.now }
})

messageSchema.index({ room: 1, createdAt: -1 })

const checkpointSchema = new mongoose.Schema({
  serverId:    { type: String, required: true, unique: true },
  resumeToken: { type: mongoose.Schema.Types.Mixed, required: true },
  updatedAt:   { type: Date, default: Date.now }
})

module.exports = {
  User:               mongoose.model('User', userSchema),
  Room:                mongoose.model('Room', roomSchema),
  Message:             mongoose.model('Message', messageSchema),
  ChangeStreamCheckpoint: mongoose.model('ChangeStreamCheckpoint', checkpointSchema)
}