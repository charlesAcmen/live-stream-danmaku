package ws

import (
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
		_, message, err := c.Socket.ReadMessage()
		if err != nil {
			break // if read error, break the loop
		}
		// 2. send message to Manager's broadcast channel
		manager.Broadcast <- message
	}
}

// WritePump: listen from Send channel, once there is a message, write to WebSocket
func (c *Client) WritePump() {
	defer func() {
		c.Socket.Close()
	}()

	for {
		// get message from Send channel
		message, ok := <-c.Send
		if !ok {
			// channel is closed
			c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		// write to WebSocket
		c.Socket.WriteMessage(websocket.TextMessage, message)
	}
}
