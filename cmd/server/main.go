package main

import (
	"fmt"
	"net/http"

	"github.com/charlesAcmen/livestream-danmaku/internal/ws"
	"github.com/gin-gonic/gin"     // Gin is a web framework for Go.
	"github.com/gorilla/websocket" // Gorilla WebSocket is a WebSocket implementation for Go.
)

// Configure the Upgrader
// An Upgrader converts an HTTP connection to a WebSocket connection.
var upgrader = websocket.Upgrader{
	// CheckOrigin allows us to customize which requests are allowed to connect.
	// By default, the Upgrader checks if the request origin matches the host.
	CheckOrigin: func(r *http.Request) bool {
		// returning true means: "Allow ALL connections from ANY website or domain."
		// WARNING: This is great for development (e.g., localhost),
		// but can be insecure in production (CSRF: Cross-Site Request Forgery risks).
		return true
	},
}

func main() {
	// 1. Initialize the Manager
	manager := ws.NewManager()
	// 2. Start the Manager (start a goroutine to run it in the background)
	go manager.Start()

	// 3. 初始化 Gin
	r := gin.Default()

	// 4. 定义 WebSocket 路由
	r.GET("/ws", func(c *gin.Context) {
		wsHandler(manager, c.Writer, c.Request)
	})

	fmt.Println("直播弹幕服务器启动 :8080...")
	r.Run(":8080")
}

// 处理具体的连接请求
func wsHandler(manager *ws.Manager, w http.ResponseWriter, r *http.Request) {
	// HTTP 升级为 WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	// 创建一个新用户
	client := &ws.Client{
		Socket: conn,
		Send:   make(chan []byte, 1024), // 缓冲 1024 条消息
	}

	// 注册给管家
	manager.Register <- client

	// 开启读写协程
	// 注意：WritePump 必须跑在协程里，ReadPump 因为有死循环，可以在当前协程跑
	go client.WritePump()
	client.ReadPump(manager)
}
