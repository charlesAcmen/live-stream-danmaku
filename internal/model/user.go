package model

type User struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Username string `gorm:"type:varchar(50);uniqueIndex;not null"`
}
