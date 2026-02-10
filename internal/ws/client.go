package ws

import (
	"encoding/json"
	"sync"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model" //database structures
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	SendChanSize = 1024
)

type Client struct {
	UserID   uint64 // client id
	Username string
	RoomID   string

	Socket *websocket.Conn // the websocket connection that the server holds for each client
	Send   chan []byte     // a channel to send messages to the client
	// done is used to signal the goroutines to stop
	done chan struct{}
	// once ensures that the Close operations are only performed once
	once sync.Once
}

func NewClient(uid uint64, name, room string, conn *websocket.Conn) *Client {
	return &Client{
		UserID:   uid,
		Username: name,
		RoomID:   room,
		Socket:   conn,
		Send:     make(chan []byte, SendChanSize),
		done:     make(chan struct{}),
	}
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
		select {
		// get message from Send channel
		// Note: 'message' here is already a JSON string coming from Redis/Manager.
		case message, ok := <-c.Send:
			if !ok {
				// channel is closed
				// Manager closed the channel (though in our new logic, manager shouldn't)
				c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			//Note: could use NextWriter here to enable write in stream manner
			//avoiding for too many times when huge wave of danmaku coming

			// write to WebSocket
			if err := c.Socket.WriteMessage(websocket.TextMessage, message); err != nil {
				logger.Log.Warn("[CLIENT] Write error",
					zap.Uint64("uid", c.UserID), zap.Error(err))
				return
			}
		case <-c.done:
			// Signal received to stop this goroutine
			logger.Log.Debug("[CLIENT] WritePump exiting via done channel", zap.Uint64("uid", c.UserID))
			return
		}
	}
}

// Close gracefully closes the client connection and signals goroutines to exit.
// It is idempotent thanks to sync.Once.
func (c *Client) Close() {
	c.once.Do(func() {
		// 1. Close the done channel to signal WritePump and other observers
		close(c.done)
		// 2. Close the physical connection
		c.Socket.Close()
		// Note: We DO NOT close c.Send here to prevent panic in workers
		logger.Log.Info("[CLIENT] Connection closed safely", zap.Uint64("uid", c.UserID))
	})
}
