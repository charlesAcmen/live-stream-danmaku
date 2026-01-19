package ws

import (
	"encoding/json"
	"log"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/model"
)

// InitHandlers registers all business logic.
// MUST be called in main.go before server starts.
func InitHandlers() {

	// 1. Handle Danmaku Logic
	Register(model.TypeDanmaku, func(c *Client, m *Manager, data []byte) {
		// A. Parse the inner content
		// The client only sends {"content": "hello"}
		var inputMsg model.DanmakuMessage
		if err := json.Unmarshal(data, &inputMsg); err != nil {
			log.Println("[HANDLER]Invalid Danmaku Data")
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
	})

	log.Print("[HANDLER]Registered Danmaku handler")

	// 2. Handle Like Logic
	Register(model.ActionLike, func(c *Client, m *Manager, data []byte) {
		// No data parsing needed for simple like
		m.AddLike()
		log.Printf("[HANDLER] User %s liked the stream", c.UserID)

	})
	log.Print("[HANDLER]Registered like & action handler")
}
