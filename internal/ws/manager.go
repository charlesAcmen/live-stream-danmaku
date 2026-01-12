package ws

import "log"

// Manager is responsible for managing all clients
type Manager struct {
	// Register channel: when a new client joins, send the client pointer to the channel
	Register chan *Client

	// Unregister channel: when a client disconnects, send the client pointer to the channel
	Unregister chan *Client

	// Broadcast channel: when a new message is to be broadcast, send the []byte to the channel
	Broadcast chan []byte

	// Core state: save all online clients
	// key is client pointer, value is bool (true means online)
	Clients map[*Client]bool
}

func NewManager() *Manager {
	return &Manager{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan []byte),
		Clients:    make(map[*Client]bool),
	}
}
func (m *Manager) Start() {
	for {

		select {
		case conn := <-m.Register:
			m.Clients[conn] = true

		case conn := <-m.Unregister:
			if _, ok := m.Clients[conn]; ok {
				delete(m.Clients, conn)
				// close send channel to prevent GoRoutine leak
				close(conn.Send)
				log.Println("[MANAGER]Client disconnected", conn.ID)
			}

		case message := <-m.Broadcast:
			for conn := range m.Clients {
				select {
				case conn.Send <- message:
					// successfully sent
				default:
					// if failed to send (receiver buffer is full), kick the client to prevent blocking the server
					// this is a self-protection mechanism for high-concurrency systems
					close(conn.Send)
					delete(m.Clients, conn)
				}
			}
		}
	}
}
