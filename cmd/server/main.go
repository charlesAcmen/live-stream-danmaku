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

	// 3. Initialize Gin
	r := gin.Default()

	// 4. define WebSocket route
	// execute lambda when receives GET method under /ws route
	r.GET("/ws", func(c *gin.Context) {
		wsHandler(manager, c.Writer, c.Request)
	})

	fmt.Println("Live chat server is running on :8080...")
	r.Run(":8080")
}

// handle specific connection requests
func wsHandler(manager *ws.Manager, w http.ResponseWriter, r *http.Request) {
	// upgrade HTTP connection to WebSocket connection
	// HTTP:short connection per request&response
	// WebSocket:long connection, keep-alive
	conn, err := upgrader.Upgrade(w, r, nil)
	//Upgrade:Hijack tcp socket connection to upgrade to WebSocket connection
	if err != nil {
		return
	}

	// create a new client
	client := &ws.Client{
		Socket: conn,
		Send:   make(chan []byte, 1024), // buffer 1024 bytes
	}

	// register the client to the manager
	manager.Register <- client

	// start read and write goroutines
	// note: WritePump must run in a goroutine, because ReadPump has a infinite loop, it can run in the current goroutine
	go client.WritePump()
	//dependency injection:
	//inject manager to client rather than storing manager pointer in client struct
	client.ReadPump(manager)
}
