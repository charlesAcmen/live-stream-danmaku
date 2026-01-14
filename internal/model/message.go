package model

import (
	"time"
)

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
	RoomID string `gorm:"type:varchar(50);not null;index:idx_room_time"`

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
