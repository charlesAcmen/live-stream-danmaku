package ws

import (
	"encoding/json"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"

	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"
)

// RegisterActionHandlers registers all business logic callbacks.
// This implements the "Strategy Pattern" to handle different WebSocket message types.
// MUST be called in main.go of server before server starts.
func RegisterActionHandlers() {

	// 1. Handle Danmaku Logic
	RegisterAction(model.TypeDanmaku, func(c *Client, m *Manager, data []byte) {
		// A. Parse the inner content
		// The client only sends {"content": "hello"}
		var inputMsg model.DanmakuMessage
		if err := json.Unmarshal(data, &inputMsg); err != nil {
			logger.Log.Error("[SERVER HANDLER]Invalid Danmaku Data format",
				zap.Uint64("uid", c.UserID),
				zap.Error(err),
			)
			return
		}

		// B. Enrich the message (Add Server-Side Metadata)
		// Client doesn't know its own ID/Avatar trustfully, Server adds it.
		// This ensures all downstream systems (Redis, Kafka) know WHO sent it.
		fullMsg := model.DanmakuMessage{
			RoomID:  c.RoomID,
			UserID:  c.UserID,
			Content: inputMsg.Content,
			//if sent successfully,set send time as accepting time
			SendTime: time.Now(),
		}

		// C. Wrap in outgoing Envelope
		dataBytes, _ := json.Marshal(fullMsg)
		outgoingPacket := model.WsPacket{
			Type:   model.TypeDanmaku,
			RoomID: c.RoomID,
			Data:   dataBytes,
		}
		m.Broadcast <- &outgoingPacket
		// logger.Log.Debug("[SERVER HANDLER]Danmaku processed",
		// 	zap.Uint64("uid", c.UserID),
		// 	zap.String("room", c.RoomID),
		// 	zap.String("content", inputMsg.Content),
		// )
	})

	logger.Log.Info("[SERVER HANDLER]Registered Handler: Danmaku")

	// 2. Handle Like Logic
	RegisterAction(model.ActionLike, func(c *Client, m *Manager, data []byte) {
		// A. Parse Input
		// Client sends: {"count": 10}
		var likes model.Like
		if err := json.Unmarshal(data, &likes); err != nil {
			// Default to 1 if payload is empty or invalid
			likes.Count = 1
		}
		// defend from frontend
		if likes.Count <= 0 {
			likes.Count = 1
		}

		m.likesMu.Lock()
        m.localLikes[c.RoomID] += likes.Count
        m.likesMu.Unlock()

		// B. Wrap in WsPacket (Envelope)
		// We re-marshal the likes to ensure format consistency
		dataBytes, _ := json.Marshal(likes)

		outgoingPacket := model.WsPacket{
			Type:   model.ActionLike,
			RoomID: c.RoomID,
			Data:   dataBytes,
		}

		// C. Send to Manager (Handover responsibility)
		m.Broadcast <- &outgoingPacket

		logger.Log.Debug("[SERVER HANDLER] Like packet forwarded to manager",
			zap.Uint64("uid", c.UserID),
			zap.String("room", c.RoomID),
		)
	})
	logger.Log.Info("[SERVER HANDLER]Registered Handler: Like/Action")
}
