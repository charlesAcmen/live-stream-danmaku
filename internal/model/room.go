package model

import "time"

type Room struct {
	ID          uint   `gorm:"primaryKey;autoIncrement"`
	RoomNumber  string `gorm:"type:varchar(20);uniqueIndex;not null"`
	StreamerID  uint   `gorm:"index;not null"`             // user id of streamer
	Title       string `gorm:"type:varchar(100);not null"` // title of stream
	CoverImg    string `gorm:"type:varchar(255)"`          // cover image url
	OnlineCount int    `gorm:"default:0"`                  // snapshot value of online audients,real value is stored in redis
	IsLive      bool   `gorm:"default:false;index"`
	CreatedAt   time.Time
}
