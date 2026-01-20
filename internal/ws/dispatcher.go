package ws

import (
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"go.uber.org/zap"
)

type MessageHandler func(client *Client, manager *Manager, data []byte)

// handler map
var handlerMap = make(map[int]MessageHandler)

func Register(msgType int, handler MessageHandler) {
	handlerMap[msgType] = handler
}

// Dispatch: dispatch message based on type
func Dispatch(client *Client, manager *Manager, msgType int, data []byte) {
	if handler, ok := handlerMap[msgType]; ok {
		handler(client, manager, data)
	} else {
		logger.Log.Warn("[DISPATCHER]Unknown message type", zap.Int("msgType", msgType))
	}
}
