package main

import (
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"

	"gorm.io/driver/mysql" //tells GORM to use MySQL driver
	"gorm.io/gorm"
)

func main() {
	// 1. Connect to MySQL
	//DSN:data source name
	//username:password@protocol(address:port)/dbname?param=value&...
	//matches with docker-compose.yaml
	//127.0.0.1 to avoid IPv6 issues in wsl

	dsn := "root:root@tcp(127.0.0.1:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	// utf8 cannot store emojis
	// mb4;most bytes 4
	// parseTime:time.Time <-->MySQL timestamp string
	// loc:system's local timezone for time conversion
	// lazy open:doesnot necessarily establish tcp connection immediately
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Log.Fatal("[MIGRATE]Failed to connect to database:", zap.Error(err))
	}

	// 2. Migrate automatically
	// GORM will create tables in the database automatically based on the structure of model.DanmuMessage
	err = db.AutoMigrate(&model.DanmakuMessage{})
	if err != nil {
		logger.Log.Fatal("[MIGRATE]Migration failed:", zap.Error(err))
	}

	logger.Log.Info("[MIGRATE]🎉 Database migration successful! Table 'danmu_messages' created.")
}
