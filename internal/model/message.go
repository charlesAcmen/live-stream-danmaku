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

// DanmakuMessage: The content for TypeDanmaku
// table danmaku_messages in danmaku_db database
// DanmakuMessage represents the standard data format for communication.
// It is used for:
// 1. Storage in MySQL (GORM tags)
// 2. Transmission via WebSocket/Redis/Kafka (JSON tags)
type DanmakuMessage struct {
	//struct tag,结构体标签,reflection for GORM(Go Object-Relational Mapping)
	ID uint64 `gorm:"primaryKey;autoIncrement" json:"id"`

	// Room info
	//idx_room_time:composite index/joint index with SendTime
	RoomID string `gorm:"type:varchar(50);not null;index:idx_room_time" json:"room_id"`

	// RoomName string `gorm:"type:varchar(100);not null"`

	// User info
	// foreign key for user table
	UserID uint64 `gorm:"not null;index" json:"user_id"`

	// Content
	// utf8mb4 coding
	Content string `gorm:"type:varchar(500);not null" json:"content"`

	// Time
	//conposite index with RoomID
	//precision till thousand level:12:00:01.456
	SendTime time.Time `gorm:"type:datetime(3);not null;index:idx_room_time" json:"send_time"`

	//index:idx_room_time
	//danmaku in storage is ordered first by roomid,then time
	//efficient for WHERE room_id = '1001' ORDER BY send_time
	//which requires ordering in memory
}

// used by GORM to migrate table name
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
	Count uint32 `json:"count"`
}
