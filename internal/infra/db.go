package infra

import (
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// InitDB connect to db and return db instance
func InitDB() *gorm.DB {
	dsn := "root:root@tcp(127.0.0.1:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Log.Fatal("[DB INFRA]Failed to connect to database", zap.Error(err))
	}

	// Auto-Migrate (Create tables if not exist)
	if err := db.AutoMigrate(&model.DanmakuMessage{}); err != nil {
		logger.Log.Fatal("[DB INFRA]Database table DanmakuMessage migration failed", zap.Error(err))
	}
	if err := db.AutoMigrate(&model.Room{}); err != nil {
		logger.Log.Fatal("[DB INFRA]Database table Room migration failed", zap.Error(err))
	}
	if err := db.AutoMigrate(&model.User{}); err != nil {
		logger.Log.Fatal("[DB INFRA]Database table User migration failed", zap.Error(err))
	}
	return db
}
