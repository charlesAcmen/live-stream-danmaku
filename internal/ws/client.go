package ws

import (
	"github.com/gorilla/websocket"
)

type Client struct {
	ID     string          // user id
	Socket *websocket.Conn // the websocket connection that the user holds
	Send   chan []byte     // a channel to send messages to the user
}

// ReadPump (读泵): 专门负责从 WebSocket 读消息，然后丢给 Manager 进行广播
func (c *Client) ReadPump(manager *Manager) {
	defer func() {
		// 如果读不出来了(断连)，就注销
		manager.Unregister <- c
		c.Socket.Close()
	}()

	for {
		// 1. 读取消息
		_, message, err := c.Socket.ReadMessage()
		if err != nil {
			break // 读错了(比如用户拔网线了)，跳出循环
		}
		// 2. 把读到的消息，塞到 Manager 的广播通道里
		manager.Broadcast <- message
	}
}

// WritePump (写泵): 专门负责监听 Send 管道，一旦有消息，就写给 WebSocket
// 类似于 C++ 里开一个线程专门 send()
func (c *Client) WritePump() {
	defer func() {
		c.Socket.Close()
	}()

	for {
		// 从 Send 管道里拿消息
		message, ok := <-c.Send
		if !ok {
			// 管道被关闭了
			c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		// 写给用户
		c.Socket.WriteMessage(websocket.TextMessage, message)
	}
}
