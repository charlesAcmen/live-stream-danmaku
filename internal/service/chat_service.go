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

// GetPlaybackDanmaku handles the logic for video replay.
// startTime: the specific time corresponding to the video progress bar
func (s *ChatService) GetPlaybackDanmaku(roomID string, startTime time.Time) ([]*model.DanmakuMessage, error) {
	// 1. Hard limit protection
	// We don't want the frontend to request 10,000 items at once.
	const defaultLimit = 100

	// 2. Call Repo
	return s.repo.GetPlayBackDanmaku(roomID, startTime, defaultLimit)
}
