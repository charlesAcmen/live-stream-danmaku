package model

import (
	"time"
)

// table danmaku_messages in danmaku_db database
type DanmakuMessage struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	// Room info
	RoomID   string `gorm:"type:varchar(50);not null;index:idx_room_time"`
	RoomName string `gorm:"type:varchar(100);not null"` // 冗余存储，避免连表

	// User info
	UserID   string `gorm:"type:varchar(50);not null;index"`
	Username string `gorm:"type:varchar(50);not null"`
	Avatar   string `gorm:"type:varchar(255)"`

	// Content
	// utf8mb4 coding
	Content string `gorm:"type:varchar(500);not null"`

	// Time
	SendTime time.Time `gorm:"type:datetime(3);not null;index:idx_room_time"`
}
