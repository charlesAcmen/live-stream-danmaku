package model

import (
	"time"
)

// table danmaku_messages in danmaku_db database
type DanmakuMessage struct {
	//struct tag,结构体标签,reflection for GORM(Go Object-Relational Mapping)
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	// Room info
	//idx_room_time:composite index/joint index
	RoomID string `gorm:"type:varchar(50);not null;index:idx_room_time"`

	RoomName string `gorm:"type:varchar(100);not null"`

	// User info
	UserID   string `gorm:"type:varchar(50);not null;index"`
	Username string `gorm:"type:varchar(50);not null"`
	//redundancy to avoid join tables
	Avatar string `gorm:"type:varchar(255)"`

	// Content
	// utf8mb4 coding
	Content string `gorm:"type:varchar(500);not null"`

	// Time
	SendTime time.Time `gorm:"type:datetime(3);not null;index:idx_room_time"`
}

func (DanmakuMessage) TableName() string {
	return "danmaku_messages"
}
