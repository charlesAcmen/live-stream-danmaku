package ws

import (
	"encoding/json"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model" //database structures
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type Client struct {
	UserID     string // client id
	Username   string
	UserAvatar string
	RoomID     string

	Socket *websocket.Conn // the websocket connection that the server holds for each client
	Send   chan []byte     // a channel to send messages to the client
}

// ReadPump: clients read from websocket to receive message, then send to manager to broadcast
func (c *Client) ReadPump(manager *Manager) {
	defer func() {
		// if read failed (client disconnected), unregister the client
		manager.Unregister <- c
		c.Socket.Close()
	}()

	for {
		// 1. read message from WebSocket
		_, messageBytes, err := c.Socket.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,         // 1001: browser navigation leave/refreshing
				websocket.CloseAbnormalClosure) { //1006:connection closed abnormally
				//log when error does not belong to reasons mentioned above
				logger.Log.Error("[CLIENT]error: %v", zap.Error(err))
			}
			break // if read error, break the loop
		}

		// 2. Parse the Envelope (WsPacket)
		var packet model.WsPacket
		if err := json.Unmarshal(messageBytes, &packet); err != nil {
			logger.Log.Error("[CLIENT]Invalid JSON format")
			continue
		}

		// 3. Dispatch to Handler
		// We pass 'manager' because handlers need to Broadcast or AddLike.
		Dispatch(c, manager, packet.Type, packet.Data)
	}
}

// WritePump: listen from Send channel, once there is a message, write to WebSocket
// i.e pumps messages from the hub to the websocket connection
func (c *Client) WritePump() {
	defer func() {
		c.Socket.Close()
	}()

	for {
		// get message from Send channel
		// Note: 'message' here is already a JSON string coming from Redis/Manager.
		message, ok := <-c.Send
		if !ok {
			// channel is closed
			c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		//Note: could use NextWriter here to enable write in stream manner
		//avoiding for too many times when huge wave of danmaku coming

		// write to WebSocket
		c.Socket.WriteMessage(websocket.TextMessage, message)

	}
}
