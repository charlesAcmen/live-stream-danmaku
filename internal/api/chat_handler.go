// DDD,domain drived design,领域驱动设计
// Controller/Handler
package api

import (
	"net/http"
	"strconv"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/service"
	"github.com/charlesAcmen/livestream-danmaku/internal/ws"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
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

type ChatHandler struct {
	service *service.ChatService
	manager *ws.Manager
}

func NewChatHandler(s *service.ChatService, m *ws.Manager) *ChatHandler {
	return &ChatHandler{
		service: s,
		manager: m,
	}
}

func (h *ChatHandler) ConnectWebSocket(c *gin.Context) {
	// 1. Parse user info from URL c parameters
	// Example: ws://localhost:8081/ws?uid=1001&name=Alice&room=Live001
	uidStr := c.Query("uid")
	name := c.Query("name")
	room := c.Query("room")

	// Simple validation (Production needs JWT)
	if uidStr == "" || room == "" {
		logger.Log.Warn("[API] Rejecting connection: missing uid or room",
			zap.String("uid", uidStr),
			zap.String("room", room),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing uid or room"})
		return
	}

	// 3. FIX: Convert String to Uint64
	// ParseUint(string, base, bitSize)
	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil {
		logger.Log.Warn("[API] Rejecting connection: invalid uid format",
			zap.String("uid", uidStr),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid uid format, must be numeric"})
		return
	}

	// 4. Upgrade Protocol
	// upgrade HTTP connection to WebSocket connection
	// HTTP:short connection per request&response
	// WebSocket:long connection, keep-alive
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Log.Error("[API] Upgrade failed", zap.Error(err))
		return
	}

	// 5. Create Client
	client := ws.NewClient(uid, name, room, conn)

	// 6. Register to Manager
	h.manager.Register <- client

	// 7. Start Pumps
	go client.WritePump()
	//dependency injection:
	//inject manager to client rather than storing manager pointer in client struct
	client.ReadPump(h.manager)
}

// HandlePlaybackRequest handles GET /api/v1/playback
// Example: GET /api/v1/playback?room=1001&time=2026-01-14T17:40:31Z
// func (h *ChatHandler) HandlePlaybackRequest(c *gin.Context) {
// 	roomID := c.Query("room")
// 	if roomID == "" {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "room_id is required"})
// 		return
// 	}

// 	// 2. Parse Time
// 	// Front-end should send time in standard format (e.g., RFC3339)
// 	timeStr := c.Query("time")
// 	var cTime time.Time
// 	var err error

// 	if timeStr != "" {
// 		// RFC3339 is like "2006-01-02T15:04:05Z07:00"
// 		// This is the standard format for JSON APIs.
// 		cTime, err = time.Parse(time.RFC3339, timeStr)
// 		if err != nil {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid time format, use RFC3339"})
// 			return
// 		}
// 	} else {
// 		// If no time provided, maybe start from the beginning?
// 		// Or handle as error? Let's assume start from zero (beginning of recording).
// 		cTime = time.Time{}
// 	}

// 	// 3. Call Service
// 	msgs, err := h.service.GetPlaybackDanmaku(roomID, cTime)
// 	if err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch messages"})
// 		return
// 	}

// 	// 4. Return JSON
// 	c.JSON(http.StatusOK, gin.H{
// 		"data":  msgs,
// 		"count": len(msgs),
// 	})
// }
