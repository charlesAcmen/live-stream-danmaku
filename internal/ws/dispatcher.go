package ws

import (
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"go.uber.org/zap"
)

type MessageHandler func(client *Client, manager *Manager, data []byte)

// actionRegistry stores the mapping between action types and their handlers
var actionRegistry = make(map[int]MessageHandler)

// RegisterAction adds a new handler to the registry
func RegisterAction(msgType int, handler MessageHandler) {
	actionRegistry[msgType] = handler
}

// Dispatch: dispatch message based on type
func Dispatch(client *Client, manager *Manager, msgType int, data []byte) {
	if handler, ok := actionRegistry[msgType]; ok {
		handler(client, manager, data)
	} else {
		logger.Log.Warn("[DISPATCHER]Unknown message type", zap.Int("msgType", msgType))
	}
}
