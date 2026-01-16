// DDD,domain drived design,领域驱动设计
// Controller/Handler
package api

import (
	"net/http"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/service"

	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	service *service.ChatService
}

func NewChatHandler(s *service.ChatService) *ChatHandler {
	return &ChatHandler{service: s}
}

// HandlePlaybackRequest handles GET /api/v1/playback
// Example: GET /api/v1/playback?room=1001&time=2026-01-14T17:40:31Z
func (h *ChatHandler) HandlePlaybackRequest(c *gin.Context) {
	roomID := c.Query("room")
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room_id is required"})
		return
	}

	// 2. Parse Time
	// Front-end should send time in standard format (e.g., RFC3339)
	timeStr := c.Query("time")
	var queryTime time.Time
	var err error

	if timeStr != "" {
		// RFC3339 is like "2006-01-02T15:04:05Z07:00"
		// This is the standard format for JSON APIs.
		queryTime, err = time.Parse(time.RFC3339, timeStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid time format, use RFC3339"})
			return
		}
	} else {
		// If no time provided, maybe start from the beginning?
		// Or handle as error? Let's assume start from zero (beginning of recording).
		queryTime = time.Time{}
	}

	// 3. Call Service
	msgs, err := h.service.GetPlaybackDanmaku(roomID, queryTime)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch messages"})
		return
	}

	// 4. Return JSON
	c.JSON(http.StatusOK, gin.H{
		"data":  msgs,
		"count": len(msgs),
	})
}
