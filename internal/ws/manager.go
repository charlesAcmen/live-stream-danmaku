package ws

import (
	"context" // context is used to manage the lifecycle of the Redis client
	"encoding/json"
	"fmt"
	"sync"
	"time" //broadcast stats data timer

	"github.com/IBM/sarama" //driver for apache Kafka clients lib
	"github.com/charlesAcmen/livestream-danmaku/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model" // Gorilla WebSocket is a WebSocket implementation for Go.
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Manager is responsible for managing all websocket clients and the Redis connection
type Manager struct {
	// Register channel: when a new client joins, send the client pointer to the channel
	Register chan *Client

	// Unregister channel: when a client disconnects, send the client pointer to the channel
	Unregister chan *Client

	// Broadcast channel: when a new message is to be broadcast, send the []byte to the channel
	Broadcast chan []byte

	// Rooms maps RoomID to a set of Clients in that room
	// map[RoomID]map[ClientPointer]exists
	Rooms map[string]map[*Client]bool

	// Keep track of connected clients
	// key is client pointer, value is bool (true means online)
	// Clients map[*Client]bool

	mu sync.RWMutex // Protects the Rooms map
	//Read and write mutex lock
	//RLock() when multiple routines read room map to broadcast simultaneously,
	//non blocking

	// RedisClient: the connection to the Redis server.
	RedisClient *redis.Client

	// KafkaProducer sarama.SyncProducer
	KafkaProducer sarama.AsyncProducer
}

const (
	KafkaTopic = "danmaku_save_topic" // Topic name for Kafka
)

func NewManager() *Manager {
	rdb := infra.InitRedis()
	producer := infra.InitKafkaProducer()
	return &Manager{
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Broadcast:     make(chan []byte),
		Rooms:         make(map[string]map[*Client]bool),
		RedisClient:   rdb,
		KafkaProducer: producer,
	}
}

// Creates the main loop for the manager.
func (m *Manager) Start() {
	logger.Log.Info("[MANAGER] Multi-room Manager started")

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case client := <-m.Register:
			m.handleRegister(client)
		case client := <-m.Unregister:
			m.handleUnregister(client)
		case message := <-m.Broadcast:
			m.publishToRedis(message)
			// Produce to Kafka (For Storage/Persistence)
			// 'message' is already a JSON bytes containing user info & content.
			kafkaMsg := &sarama.ProducerMessage{
				Topic: KafkaTopic,
				//Kafka is a byte logging system that deal with binary bytes stream
				Value: sarama.ByteEncoder(message),
			}

			select {
			case m.KafkaProducer.Input() <- kafkaMsg:
			case <-m.KafkaProducer.Errors():
				logger.Log.Warn("[MANAGER] Kafka error channel read during send")

			default:
				// [熔断机制]
				// 如果 Kafka 挂了或者网络太慢，导致本地 buffer 满了，
				// 这里会直接丢弃消息，而不是卡死 Manager。
				// 保护了实时弹幕（Redis）不受影响。
				logger.Log.Warn("[MANAGER] Kafka buffer full, dropping message to prevent blocking")
			}

			// Send to Kafka (Sync)
			// partition,offset,err
			// _, _, err = m.KafkaProducer.SendMessage(kafkaMsg)
			// if err != nil {
			// 	logger.Log.Error("[MANAGER]Kafka Produce Error", zap.Error(err))
			// 	// Note: In real world, we might want to save to a local file if Kafka fails (Fallback).
			// }
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
		case <-ticker.C:
			m.broadcastStats()
		}
	}
}

func (m *Manager) handleRegister(client *Client) {
	//write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Initialize room map if it's the first client in this room on THIS server
	if _, ok := m.Rooms[client.RoomID]; !ok {
		m.Rooms[client.RoomID] = make(map[*Client]bool)
		// 2. Start a background goroutine to listen to THIS room's Redis channel
		go m.subscribeToRoom(client.RoomID)
		logger.Log.Info("[MANAGER] New room created on server", zap.String("room", client.RoomID))
	}

	m.Rooms[client.RoomID][client] = true
	// Increment online count in Redis
	key := fmt.Sprintf("room:%s:onlinecount", client.RoomID)
	m.RedisClient.Incr(context.Background(), key)
	logger.Log.Info("[MANAGER] Client registered",
		zap.Uint64("uid", client.UserID),
		zap.String("room", client.RoomID),
		zap.Int("total", len(m.Rooms[client.RoomID])))
}

func (m *Manager) handleUnregister(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if clients, ok := m.Rooms[client.RoomID]; ok {
		if _, ok := clients[client]; ok {
			delete(clients, client)
			close(client.Send)
			logger.Log.Info(
				"[MANAGER] Client disconnected",
				zap.Uint64("userID", client.UserID),
				zap.Int("total", len(clients)),
			)
			// If room is empty on this server, remove the room entry
			if len(clients) == 0 {
				delete(m.Rooms, client.RoomID)
				// Note: In production, you'd also need a way to stop the Redis subscription goroutine
			}
			// Decrement online count in Redis
			key := fmt.Sprintf("room:%s:onlinecount", client.RoomID)
			m.RedisClient.Decr(context.Background(), key)
		}
	}
}

func (m *Manager) publishToRedis(message []byte) {
	// 1. Extract RoomID from the data (assuming DanmakuMessage inside)
	// For simplicity, let's assume we pass RoomID in the Broadcast channel or parse it
	// Here, we use a placeholder logic:
	roomID := "1001" // In reality, parse this from the message or wrap it

	channelName := fmt.Sprintf("room:%s:pubsub", roomID)
	payload, _ := json.Marshal(message)

	// Publish the message to the channelName in Redis.
	err := m.RedisClient.Publish(context.Background(), channelName, payload).Err()
	if err != nil {
		logger.Log.Error("[MANAGER]Error publishing to Redis",
			zap.String("room", roomID),
			zap.Error(err))
	} else {
		logger.Log.Info("[MANAGER]Message published to Redis",
			zap.String("room", roomID),
			zap.ByteString("message", message))
	}
	
}

// subscribeToRoom listens to a specific Redis channel for a room
func (m *Manager) subscribeToRoom(roomID string) {
	// Channel name: e.g., "room:1001:pubsub"
	channelName := fmt.Sprintf("room:%s:pubsub", roomID)
	//context.Background(): a default context that never cancels, never expires, and has no values.
	//or context.WithTimeOut(...,3*time.Second)
	pubsub := m.RedisClient.Subscribe(context.Background(), channelName)
	defer pubsub.Close()
	// Go channel to receive Redis messages(read only)
	ch := pubsub.Channel()

	// Loop over messages received from Redis.
	for msg := range ch {
		// msg.Payload is the JSON string
		// Now we broadcast this message to all local clients in this room.
		m.broadcastToLocalRoom(roomID, []byte(msg.Payload))
	}
}

func (m *Manager) broadcastToLocalRoom(roomID string, payload []byte) {
	//read lock
	m.mu.RLock()
	defer m.mu.RUnlock()

	if clients, ok := m.Rooms[roomID]; ok {
		for client := range clients {
			select {
			case client.Send <- payload:
			default:
				// If client buffer is full, we don't block the whole room
				//close and remove to prevent blocking.
				close(client.Send)
				delete(clients, client)
			}
		}
	}
}

// broadcastStats fetches stats from Redis and broadcasts to LOCAL clients.
func (m *Manager) broadcastStats() {
	logger.Log.Info("[MANAGER]BroadcastStats called")
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
	//takes only a few ms to broadcast to all clients

}

// AddLike increments the like counter in Redis atomically.
func (m *Manager) AddLike(roomID string) {
	// Key format: "room:1001:likes"
	key := fmt.Sprintf("room:%s:likes", roomID)
	m.RedisClient.Incr(context.Background(), key)
}
