// DDD,domain drived design,领域驱动设计
// Controller/Handler
package api

import (
	"net/http"
	"strconv"

	"github.com/charlesAcmen/livestream-danmaku/internal/service"

	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	service *service.ChatService
}

func NewChatHandler(s *service.ChatService) *ChatHandler {
	return &ChatHandler{service: s}
}

// GetHistory handles GET /api/v1/history?room=1001
func (h *ChatHandler) GetHistory(c *gin.Context) {
	roomID := c.Query("room")
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room_id is required"})
		return
	}

	limitStr := c.Query("limit")
	limit, _ := strconv.Atoi(limitStr)

	// Call Service
	msgs, err := h.service.GetRoomHistory(roomID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  msgs,
		"count": len(msgs),
	})
}
