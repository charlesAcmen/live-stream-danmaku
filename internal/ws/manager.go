package ws

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

// Manager is responsible for managing all websocket clients and the Redis connection
type Manager struct {
	// Register channel: when a new client joins, send the client pointer to the channel
	Register chan *Client

	// Unregister channel: when a client disconnects, send the client pointer to the channel
	Unregister chan *Client

	// Broadcast channel: when a new message is to be broadcast, send the []byte to the channel
	Broadcast chan []byte

	// Keep track of connected clients
	// key is client pointer, value is bool (true means online)
	Clients map[*Client]bool

	// RedisClient: the connection to the Redis server.
	RedisClient *redis.Client
}

// Global constant for the Redis channel name.
// In a real app, this might be dynamic (e.g., "room:1001").
const RedisChannel = "chat_room"

func NewManager() *Manager {
	// Initialize Redis client.
	// Ensure your Redis is running on localhost:6379 via Docker.
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return &Manager{
		Register:    make(chan *Client),
		Unregister:  make(chan *Client),
		Broadcast:   make(chan []byte),
		Clients:     make(map[*Client]bool),
		RedisClient: rdb,
	}
}

// Creates the main loop for the manager.
func (m *Manager) Start() {
	// Start a separate goroutine to subscribe to Redis messages.
	go m.subscribeToRedis()
	for {
		select {
		// 1. New client connecting
		case client := <-m.Register:
			m.Clients[client] = true
			log.Println(
				"[MANAGER]New client ", client.ID, " connected. Total:", len(m.Clients))
		// 2. Client disconnecting
		case client := <-m.Unregister:
			if _, ok := m.Clients[client]; ok {
				delete(m.Clients, client)
				// close send channel to prevent GoRoutine leak
				close(client.Send)
				log.Println("[MANAGER]Client disconnected", client.ID, ". Total:", len(m.Clients))
			}

		case message := <-m.Broadcast:
			// Publish the message to the "chat_room" channel in Redis.
			err := m.RedisClient.Publish(context.Background(), RedisChannel, message).Err()
			if err != nil {
				log.Printf("[MANAGER]Error publishing to Redis: %v", err)
			}
			log.Printf("[MANAGER]Message published to Redis: %s", message)
			// for conn := range m.Clients {
			// 	select {
			// 	case conn.Send <- message:
			// 		// successfully sent
			// 	default:
			// 		// if failed to send (receiver buffer is full), kick the client to prevent blocking the server
			// 		// this is a self-protection mechanism for high-concurrency systems
			// 		close(conn.Send)
			// 		delete(m.Clients, conn)
			// 	}
			// }
		}
	}
}

// subscribeToRedis listens for messages from Redis and broadcasts them locally.
func (m *Manager) subscribeToRedis() {
	// Subscribe to the channel.
	pubsub := m.RedisClient.Subscribe(context.Background(), RedisChannel)
	defer pubsub.Close()

	// Go channel to receive Redis messages.
	ch := pubsub.Channel()

	// Loop over messages received from Redis.
	for msg := range ch {
		// msg.Payload contains the actual message string.
		// Now we broadcast this message to all LOCAL clients.
		for client := range m.Clients {
			select {
			case client.Send <- []byte(msg.Payload):
			default:
				// If client's send buffer is full, close and remove to prevent blocking.
				close(client.Send)
				delete(m.Clients, client)
			}
		}
	}
}
