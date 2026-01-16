package service

import (
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
)

// ChatService handles business logic for chat.
type ChatService struct {
	repo *repo.MessageRepo
}

func NewChatService(repo *repo.MessageRepo) *ChatService {
	return &ChatService{repo: repo}
}

// GetRoomHistory returns the message list for frontend.
func (s *ChatService) GetRoomHistory(roomID string, limit int) ([]*model.DanmakuMessage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50 // Default limit
	}

	// Business Logic: Maybe checking if the room exists? (Skipped for now)

	// Fetch latest history (Time.Now)
	return s.repo.GetHistoryByRoomID(roomID, time.Now(), limit)
}
