package model

import "time"

type User struct {
	ID        uint   `gorm:"primaryKey;autoIncrement"`
	Username  string `gorm:"type:varchar(50);uniqueIndex;not null"`
	Password  string `gorm:"type:varchar(100);not null"`
	Avatar    string `gorm:"type:varchar(255)"` // profile picture URL
	CreatedAt time.Time
	UpdatedAt time.Time
}
