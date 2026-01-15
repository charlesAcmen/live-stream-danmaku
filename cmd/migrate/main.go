package main

import (
	"log"

	"github.com/charlesAcmen/livestream-danmaku/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	// 1. Connect to MySQL
	dsn := "root:root@tcp(localhost:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("[MIGRATE]Failed to connect to database:", err)
	}

	// 2. Migrate automatically
	// GORM will create tables in the database automatically based on the structure of model.DanmuMessage
	err = db.AutoMigrate(&model.DanmakuMessage{})
	if err != nil {
		log.Fatal("[MIGRATE]Migration failed:", err)
	}

	log.Println("[MIGRATE]🎉 Database migration successful! Table 'danmu_messages' created.")
}
