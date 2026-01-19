package ws

import (
	"encoding/json"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"

	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"
)

// InitHandlers registers all business logic callbacks.
// This implements the "Strategy Pattern" to handle different WebSocket message types.
// MUST be called in main.go before server starts.
func InitHandlers() {

	// 1. Handle Danmaku Logic
	Register(model.TypeDanmaku, func(c *Client, m *Manager, data []byte) {
		// A. Parse the inner content
		// The client only sends {"content": "hello"}
		var inputMsg model.DanmakuMessage
		if err := json.Unmarshal(data, &inputMsg); err != nil {
			logger.Log.Error("[SERVER HANDLER]Invalid Danmaku Data format",
				zap.String("uid", c.UserID),
				zap.Error(err),
			)
			return
		}

		// B. Enrich the message (Add Server-Side Metadata)
		// Client doesn't know its own ID/Avatar trustfully, Server adds it.
		// This ensures all downstream systems (Redis, Kafka) know WHO sent it.
		fullMsg := model.DanmakuMessage{
			RoomID:     c.RoomID,
			UserID:     c.UserID,
			Username:   c.Username,
			UserAvatar: c.UserAvatar,
			Content:    inputMsg.Content,
			//if sent successfully,set send time as accepting time
			SendTime: time.Now(),
		}

		// C. Wrap in outgoing Envelope
		dataBytes, _ := json.Marshal(fullMsg)
		outgoingPacket := model.WsPacket{
			Type: model.TypeDanmaku, // 101
			Data: dataBytes,
		}

		// D. send the JSON bytes to Manager's broadcast channel
		finalBytes, _ := json.Marshal(outgoingPacket)
		m.Broadcast <- finalBytes
		logger.Log.Debug("[SERVER HANDLER]Danmaku processed",
			zap.String("uid", c.UserID),
			zap.String("room", c.RoomID),
			zap.String("content", inputMsg.Content),
		)
	})

	logger.Log.Info("[SERVER HANDLER]Registered Handler: Danmaku")

	// 2. Handle Like Logic
	Register(model.ActionLike, func(c *Client, m *Manager, data []byte) {
		// No data parsing needed for simple like
		// Update the like counter in Redis (via Manager)
		m.AddLike()
		logger.Log.Debug("[SERVER HANDLER]User liked the stream",
			zap.String("uid", c.UserID),
			zap.String("room", c.RoomID),
		)

	})
	logger.Log.Info("Registered Handler: Like/Action")
}
