package model

type Room struct {
	ID         uint64 `gorm:"primaryKey;autoIncrement"`
	RoomNumber string `gorm:"type:varchar(20);uniqueIndex;not null"`
}
