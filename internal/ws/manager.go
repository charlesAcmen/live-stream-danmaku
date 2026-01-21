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
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
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
	Broadcast chan *model.WsPacket

	// Rooms maps RoomID to a set of Clients in that room
	// map[RoomID]map[ClientPointer]exists
	Rooms map[string]map[*Client]bool
	mu    sync.RWMutex // Protects the Rooms map

	//Read and write mutex lock
	//RLock() when multiple routines read room map to broadcast simultaneously,
	//non blocking

	// RedisClient: the connection to the Redis server.
	RedisClient *redis.Client

	// KafkaProducer sarama.SyncProducer
	KafkaProducer sarama.AsyncProducer
}

const (
	KafkaTopic        = "danmaku_save_topic" // Topic name for Kafka
	BroadCastInterVal = 3 * time.Second
)

func NewManager() *Manager {
	rdb := infra.InitRedisClient()
	producer := infra.InitKafkaProducer()
	return &Manager{
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Broadcast:     make(chan *model.WsPacket, 1024), // Buffer to handle spikes
		Rooms:         make(map[string]map[*Client]bool),
		RedisClient:   rdb,
		KafkaProducer: producer,
	}
}

// Creates the main loop for the manager.
func (m *Manager) Start() {
	logger.Log.Info("[MANAGER] Multi-room Manager started")

	ticker := time.NewTicker(BroadCastInterVal)
	defer ticker.Stop()

	for {
		select {
		case client := <-m.Register:
			m.handleRegister(client)
		case client := <-m.Unregister:
			m.handleUnregister(client)
		case packet := <-m.Broadcast:
			m.handleBroadcast(packet)
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
func (m *Manager) handleBroadcast(packet *model.WsPacket) {
	// Serialize the envelope for Redis distribution
	payload, _ := json.Marshal(packet)
	// 1. Process local/remote broadcasting via Redis
	infra.PublishToRoom(m.RedisClient, packet.RoomID, payload)
	// 2. Process data persistence via Kafka
	// We only archive specific types
	if packet.Type == model.TypeDanmaku {
		infra.PushToInput(m.KafkaProducer, KafkaTopic, payload)
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

// AddLike increments the like counter in Redis atomically.
func (m *Manager) AddLike(roomID string) {
	// Key format: "room:1001:likes"
	key := fmt.Sprintf("room:%s:likes", roomID)
	m.RedisClient.Incr(context.Background(), key)
}
