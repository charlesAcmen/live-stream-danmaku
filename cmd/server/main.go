package main

import (
	"flag" // command line arguments
	"log"

	"github.com/charlesAcmen/livestream-danmaku/internal/api"
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
	"github.com/charlesAcmen/livestream-danmaku/internal/service"
	"github.com/charlesAcmen/livestream-danmaku/internal/ws"
	"github.com/gin-gonic/gin" // Gin is a web framework for Go.
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	// Define command-line flag for port. Default is 8080.
	// port is argument name,8080 is default value, "server port" is help message
	// port is in *string type
	port := flag.String("port", "8080", "server port")
	//resolve arguments,port is given value
	flag.Parse()

	//IMPORTANT:initialize logic handler functions[Danmaku,Like,Broadcast stats]
	ws.InitHandlers()

	// 1. Init DB (Shared by all layers)
	dsn := "root:root@tcp(127.0.0.1:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("[MIGRATE]Failed to connect to database:", err)
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
	v1 := r.Group("/api/v1")
	{
		//live stream playback url
		// URL: http://localhost:8080/api/v1/playback?room=1001&time=...
		v1.GET("/playback", chatHandler.HandlePlaybackRequest)
	}

	// WebSocket Route
	r.GET("/ws", func(c *gin.Context) {
		// 这里还是直接调 wsHandler (稍微有点混搭，暂时没关系)
		// 理想情况下 wsHandler 也应该封装进 api 包
		ws.WsHandler(manager, c.Writer, c.Request)
	})

	// ... Run
	r.Run(":" + *port)

	// // 1. Initialize the Manager
	// manager := ws.NewManager()
	// // 2. Start the Manager (start a goroutine to run it in the background)
	// go manager.Start()

	// // 3. Initialize Gin
	// r := gin.Default()

	// // 4. define WebSocket route
	// // execute lambda when receives GET method under /ws route
	// r.GET("/ws", func(c *gin.Context) {
	// 	wsHandler(manager, c.Writer, c.Request)
	// })

	// addr := ":" + *port
	// log.Printf("[SERVER]Starting server on port %s...", addr)
	// // Run server on the specified port
	// if err := r.Run(addr); err != nil {
	// 	log.Fatal("[SERVER]Server run failed:", err)
	// }
}
