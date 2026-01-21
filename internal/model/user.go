package model

type User struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	Username string `gorm:"type:varchar(50);uniqueIndex;not null"`
}
