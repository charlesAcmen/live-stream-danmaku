package ws

import (
	"encoding/json"
	"log"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/model" //database structures
	"github.com/gorilla/websocket"
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
				log.Printf("[CLIENT]error: %v", err)
			}
			break // if read error, break the loop
		}

		// This ensures all downstream systems (Redis, Kafka) know WHO sent it.
		msgObj := model.DanmakuMessage{
			RoomID:     c.RoomID,
			UserID:     c.UserID,
			Username:   c.Username,
			UserAvatar: c.UserAvatar,
			Content:    string(messageBytes), // The actual text
			//if sent successfully,set send time as accepting time
			SendTime: time.Now(),
		}

		// 3. Serialize to JSON bytes
		jsonBytes, err := json.Marshal(msgObj)
		if err != nil {
			log.Println("[CLIENT]JSON Marshal Error:", err)
			continue
		}

		// 2. send the JSON bytes to Manager's broadcast channel
		manager.Broadcast <- jsonBytes
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
