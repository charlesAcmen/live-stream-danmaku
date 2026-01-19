package main

import (
	"flag" // command line arguments

	"github.com/charlesAcmen/livestream-danmaku/internal/api"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
	"github.com/charlesAcmen/livestream-danmaku/internal/service"
	"github.com/charlesAcmen/livestream-danmaku/internal/ws"
	"github.com/gin-gonic/gin" // Gin is a web framework for Go.
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	// 1. Initialize Logger
	// Use "dev" for colored output, "prod" for JSON.
	logger.InitLogger("dev")
	defer logger.Sync()

	// Define command-line flag for port. Default is 8080.
	// port is argument name,8080 is default value, "server port" is help message
	// port is in *string type
	port := flag.String("port", "8080", "server port")
	//resolve arguments,port is given value
	flag.Parse()
	logger.Log.Info("[SERVER]Starting server...", zap.String("port", *port))

	//IMPORTANT:
	//initialize Global WebSocket Handlers
	//logic handler functions[Danmaku,Like,Broadcast stats]
	ws.InitHandlers()

	// 1. Init DB (Shared by all layers)
	dsn := "root:root@tcp(127.0.0.1:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Log.Fatal("[SERVER]Failed to connect to database", zap.Error(err))
	}

	// Auto-Migrate (Create tables if not exist)
	if err := db.AutoMigrate(&model.DanmakuMessage{}); err != nil {
		logger.Log.Fatal("[SERVER]Database migration failed", zap.Error(err))
	}

	// 2. Init Layers (Dependency Injection)
	// Repo -> Service -> Handler
	messageRepo := repo.NewMessageRepo(db)
	chatService := service.NewChatService(messageRepo)
	chatHandler := api.NewChatHandler(chatService)

	// 3. Init WebSocket Manager
	manager := ws.NewManager()
	go manager.Start()

	// 4. Init Router
	r := gin.Default()

	// HTTP API Group
	// Group: RESTful API v1
	v1 := r.Group("/api/v1")
	{
		// Playback (VOD) API: Fetch historical messages from MySQL
		// live stream playback url
		// URL: http://localhost:8080/api/v1/playback?room=1001&time=...
		v1.GET("/playback", chatHandler.HandlePlaybackRequest)
	}

	// WebSocket Route
	r.GET("/ws", func(c *gin.Context) {
		ws.WsHandler(manager, c.Writer, c.Request)
	})

	addr := ":" + *port
	if err := r.Run(addr); err != nil {
		logger.Log.Fatal("[SERVER]Server startup failed", zap.Error(err))
	}
	logger.Log.Info("[SERVER]Server listening", zap.String("address", addr))
}
