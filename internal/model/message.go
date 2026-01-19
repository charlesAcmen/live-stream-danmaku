package model

import (
	"encoding/json" //Delayed Parsing
	"time"
)

// ==========================================
// Part 1: Protocol Constants
// ==========================================

// Define message types
const (
	// TypeDanmu: Standard chat message
	TypeDanmaku = 101
	// TypeStats: Room statistics (Online count, Likes)
	TypeStats = 102

	// ActionLike: User sends a "Like" signal (Client -> Server)
	ActionLike = 103
)

// ==========================================
// Part 2: The Envelope
// ==========================================

// WsPacket is the standard wrapper for ALL websocket communications.
// Every message sent or received must follow this structure.
type WsPacket struct {
	// Type tells the receiver what 'Data' contains.
	Type int `json:"type"`

	// type RawMessage []byte,rather than map[int]interface{} using Assert
	Data json.RawMessage `json:"data"`
}

// ==========================================
// Part 3: The Payloads
// ==========================================

// table danmaku_messages in danmaku_db database
// DanmakuMessage represents the standard data format for communication.
// It is used for:
// 1. Storage in MySQL (GORM tags)
// 2. Transmission via WebSocket/Redis/Kafka (JSON tags)
type DanmakuMessage struct {
	//struct tag,结构体标签,reflection for GORM(Go Object-Relational Mapping)
	ID uint64 `gorm:"primaryKey;autoIncrement" json:"id"`

	// Room info
	//idx_room_time:composite index/joint index
	RoomID string `gorm:"type:varchar(50);not null;index:idx_room_time" json:"room_id"`

	// RoomName string `gorm:"type:varchar(100);not null"`

	// User info
	UserID   string `gorm:"type:varchar(50);not null;index" json:"user_id"`
	Username string `gorm:"type:varchar(50);not null" json:"username"`
	//redundancy to avoid join tables
	UserAvatar string `gorm:"type:varchar(255)" json:"user_avatar"`

	// Content
	// utf8mb4 coding
	Content string `gorm:"type:varchar(500);not null" json:"content"`

	// Time
	SendTime time.Time `gorm:"type:datetime(3);not null;index:idx_room_time" json:"send_time"`
}

func (DanmakuMessage) TableName() string {
	return "danmaku_messages"
}

// StatsData: The content for TypeStats
type StatsData struct {
	Online int64 `json:"online"`
	Likes  int64 `json:"likes"`
}

// CmdLike: The content for ActionLike (Client -> Server)
// Currently empty, but extensible (e.g., send 10 likes at once).
type CmdLike struct {
	Count int `json:"count"`
}
