package service

import (
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
)

// ChatService handles business logic for chat.
type ChatService struct {
	repo *repo.MessageRepo
}

func NewChatService(repo *repo.MessageRepo) *ChatService {
	return &ChatService{repo: repo}
}
