package ws

import (
	"context" // context is used to manage the lifecycle of the Redis client
	"encoding/json"
	"log"
	"net/http"
	"time" //broadcast stats data timer

	"github.com/IBM/sarama" //driver for apache Kafka clients lib
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"github.com/gorilla/websocket" // Gorilla WebSocket is a WebSocket implementation for Go.
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

	KafkaProducer sarama.SyncProducer
}

const (
	RedisChannel   = "chat_room"
	KafkaTopic     = "danmaku_save_topic" // Topic name for Kafka
	KeyOnlineCount = "room:1001:online"   // Key for online user count
	KeyTotalLikes  = "room:1001:likes"    // Key for total likes
)

func NewManager() *Manager {
	// Initialize Redis client.
	// Ensure Redis is running on localhost:6379 via Docker.
	// Lazy loading: only create Redis client when first time trying to contact
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// 2. Init Kafka Producer
	// Configure Sarama settings
	config := sarama.NewConfig()
	// We must wait for the acknowledgment from Kafka to ensure data is safe.
	//   - NoResponse
	//   - WaitForLocal: Leader returns OK after receiving
	//   - WaitForAll: all follower synced
	config.Producer.RequiredAcks = sarama.WaitForAll
	// We need to return success info to avoid errors in SyncProducer.
	config.Producer.Return.Successes = true
	// multiple partitions in one topic of Kafka
	// Use Random partitioner to distribute messages evenly in all partitions
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// Connect to Kafka (running on localhost:9092 (in .yaml) via Docker)
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		// In production, we might want to retry or fail gracefully.
		// For now, we panic because without Kafka, persistence fails.
		log.Panicf("[MANAGER]Failed to start Kafka producer: %v", err)
	}

	return &Manager{
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Broadcast:     make(chan []byte),
		Clients:       make(map[*Client]bool),
		RedisClient:   rdb,
		KafkaProducer: producer,
	}
}

// Creates the main loop for the manager.
func (m *Manager) Start() {
	// Start a separate goroutine to subscribe to Redis messages.
	go m.subscribeToRedis()
	// 2. [NEW] Setup Ticker: Broadcast stats every 3 seconds
	// This prevents broadcasting storm when likes increase rapidly.
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		// 1. New client connecting
		case client := <-m.Register:
			m.Clients[client] = true
			log.Println(
				"[MANAGER]New client ", client.UserID, " connected. Total:", len(m.Clients))
			// Increment online count in Redis
			m.RedisClient.Incr(context.Background(), KeyOnlineCount)
			// Send recent history (cached in Redis) to the new user
			// go m.sendRecentHistory(client)
		// 2. Client disconnecting
		case client := <-m.Unregister:
			if _, ok := m.Clients[client]; ok {
				delete(m.Clients, client)
				// close send channel to prevent GoRoutine leak
				close(client.Send)
				log.Println("[MANAGER]Client disconnected", client.UserID, ". Total:", len(m.Clients))
				// Decrement online count in Redis
				m.RedisClient.Decr(context.Background(), KeyOnlineCount)
			}

		case message := <-m.Broadcast:
			// Publish the message to the "RedisChannel" in Redis.
			err := m.RedisClient.Publish(context.Background(), RedisChannel, message).Err()
			if err != nil {
				log.Printf("[MANAGER]Error publishing to Redis: %v", err)
			} else {
				log.Printf("[MANAGER]Message published to Redis: %s", message)
			}
			// Produce to Kafka (For Storage/Persistence)
			// 'message' is already a JSON bytes containing user info & content.
			kafkaMsg := &sarama.ProducerMessage{
				Topic: KafkaTopic,
				//Kafka is a byte logging system that deal with binary bytes stream
				Value: sarama.ByteEncoder(message),
			}

			// Send to Kafka (Sync)
			// partition,offset,err
			_, _, err = m.KafkaProducer.SendMessage(kafkaMsg)
			if err != nil {
				log.Printf("[MANAGER]Kafka Produce Error: %v", err)
				// Note: In real world, we might want to save to a local file if Kafka fails (Fallback).
			}
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
		// --- Time to Broadcast Stats ---
		case <-ticker.C:
			m.broadcastStats()
		}
	}
}

// subscribeToRedis listens for messages from Redis and broadcasts them locally.
func (m *Manager) subscribeToRedis() {
	// Subscribe to the channel.
	//context.Background(): a default context that never cancels, never expires, and has no values.
	//or context.WithTimeOut(...,3*time.Second)
	pubsub := m.RedisClient.Subscribe(context.Background(), RedisChannel)
	defer pubsub.Close()

	// Go channel to receive Redis messages.
	// read only
	ch := pubsub.Channel()

	// Loop over messages received from Redis.
	for msg := range ch {
		// msg.Payload is the JSON string
		// Now we broadcast this message to all LOCAL clients.
		for client := range m.Clients {
			select {
			//msg.Payload:string
			case client.Send <- []byte(msg.Payload):
			default:
				// If client's send buffer is full, close and remove to prevent blocking.
				close(client.Send)
				delete(m.Clients, client)
			}
		}
	}
}

// broadcastStats fetches stats from Redis and broadcasts to LOCAL clients.
func (m *Manager) broadcastStats() {
	log.Print("[MANAGER]BroadcastStats called")
	ctx := context.Background()

	// 1. Fetch data from Redis (Single Source of Truth)
	online, _ := m.RedisClient.Get(ctx, KeyOnlineCount).Int64()
	likes, _ := m.RedisClient.Get(ctx, KeyTotalLikes).Int64()

	// 2. Wrap data into StatsData struct
	stats := model.StatsData{
		Online: online,
		Likes:  likes,
	}

	// 3. Serialize payload
	dataBytes, _ := json.Marshal(stats)

	// 4. Wrap in WsPacket (Envelope)
	packet := model.WsPacket{
		Type: model.TypeStats, // 102
		Data: dataBytes,
	}
	finalBytes, _ := json.Marshal(packet)

	// 5. Send to all local clients
	// We do NOT publish to Redis here, because every server instance has its own ticker
	// and will fetch the same data from Redis.
	for client := range m.Clients {
		select {
		case client.Send <- finalBytes:
		default:
			close(client.Send)
			delete(m.Clients, client)
		}
	}
}

func (m *Manager) AddLike() {

}

// Configure the Upgrader
// An Upgrader converts an HTTP connection to a WebSocket connection.
var upgrader = websocket.Upgrader{
	// CheckOrigin allows us to customize which requests are allowed to connect.
	// By default, the Upgrader checks if the request origin matches the host.
	CheckOrigin: func(r *http.Request) bool {
		// returning true means: "Allow ALL connections from ANY website or domain."
		// WARNING: This is great for development (e.g., localhost),
		// but can be insecure in production (CSRF: Cross-Site Request Forgery risks).
		return true
	},
}

// handle specific connection requests
func WsHandler(manager *Manager, w http.ResponseWriter, r *http.Request) {
	// 1. Parse user info from URL query parameters
	// Example: ws://localhost:8081/ws?uid=1001&name=Alice&room=Live001
	query := r.URL.Query()
	uid := query.Get("uid")
	name := query.Get("name")
	room := query.Get("room")

	// Simple validation (Production needs JWT)
	if uid == "" || room == "" {
		http.Error(w, "Missing uid or room", http.StatusBadRequest)
		return
	}

	// upgrade HTTP connection to WebSocket connection
	// HTTP:short connection per request&response
	// WebSocket:long connection, keep-alive
	conn, err := upgrader.Upgrade(w, r, nil)
	//Upgrade:Hijack tcp socket connection to upgrade to WebSocket connection
	if err != nil {
		log.Println("[SERVER]Upgrade error:", err)
		return
	}

	// create a new client
	client := &Client{
		UserID:     uid,
		Username:   name,
		RoomID:     room,
		UserAvatar: "default_avatar.png", // Mock avatar
		Socket:     conn,
		Send:       make(chan []byte, 1024), // buffer 1024 bytes
	}

	// register the client to the manager
	manager.Register <- client

	// start read and write goroutines
	// note: WritePump must run in a goroutine, because ReadPump has a infinite loop, it can run in the current goroutine
	go client.WritePump()
	//dependency injection:
	//inject manager to client rather than storing manager pointer in client struct
	client.ReadPump(manager)
}
