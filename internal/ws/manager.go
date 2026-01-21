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

	switch packet.Type {
	// Case A: Danmaku (User generated)
	// Send to Redis (so other servers see it).
	// Send to Kafka (for history storage).
	// Note: We do NOT call m.broadcastToLocalRoom here.
	// Why? Because Redis Sub goroutine will receive it and call broadcastToLocalRoom.
	// If we call it here, the local user will see the message twice!
	case model.TypeDanmaku:
		infra.PublishToRoom(m.RedisClient, packet.RoomID, payload)
		// 2. Process data persistence via Kafka
		// We only archive specific types
		infra.PushToInput(m.KafkaProducer, KafkaTopic, payload)
	// Case B: Like (User generated)
	// Send to Redis (real-time).
	// Skip Kafka (usually likes are just counters, detailed logs might not be needed).
	case model.ActionLike:
		// 1. Business Logic: Increment Redis Counter
		// We need to peek inside the Data to know the "Count"
		var cmd model.Like
		if err := json.Unmarshal(packet.Data, &cmd); err == nil {
			// Call Infra to increment
			infra.IncrRoomLikes(m.RedisClient, packet.RoomID, cmd.Count)
		}
	// Case A: Stats (Generated locally, sent locally)
	// Do NOT send to Redis (avoids broadcast storm).
	// Do NOT send to Kafka (no need to save transient stats).
	case model.TypeStats:
		m.broadcastToLocalRoom(packet.RoomID, payload)
	default:
		logger.Log.Warn("[MANAGER] Unknown packet type", zap.Int("type", packet.Type))
	}
}

// broadcastStats fetches stats for ALL active rooms and sends updates locally.
func (m *Manager) broadcastStats() {
	logger.Log.Info("[MANAGER]BroadcastStats called")
	// 1. Iterate over all active rooms on this server
	// We need to fetch and broadcast stats for EACH room separately.
	m.mu.RLock()
	roomIDs := make([]string, 0, len(m.Rooms))
	for roomID := range m.Rooms {
		roomIDs = append(roomIDs, roomID)
	}
	m.mu.RUnlock()
	for _, roomID := range roomIDs {
		online, likes := infra.GetRoomStats(m.RedisClient, roomID)

		stats := model.StatsData{
			Online: online,
			Likes:  likes,
		}
		dataBytes, _ := json.Marshal(stats)

		// Create packet
		packet := &model.WsPacket{
			Type:   model.TypeStats,
			RoomID: roomID,
			Data:   dataBytes,
		}

		// Non-blocking send to self (avoid deadlock if channel is full)
		select {
		case m.Broadcast <- packet:
		default:
			logger.Log.Warn("[MANAGER] Broadcast channel full, skipping stats", zap.String("room", roomID))
		}
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

// broadcastToLocalRoom sends raw bytes to all clients in a specific room.
func (m *Manager) broadcastToLocalRoom(roomID string, payload []byte) {
	//read lock
	m.mu.RLock()
	defer m.mu.RUnlock()

	if clients, ok := m.Rooms[roomID]; ok {
		for client := range clients {
			select {
			case client.Send <- payload:
			default:
				// FIX: Do NOT modify the map (delete) inside RLock.
				// Instead, just close the channel and let WritePump handle the error
				delete(clients, client)
			}
		}
	}
}
