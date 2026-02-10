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

// Job structure
type BroadcastJob struct {
	Clients []*Client // The slice obtained from Pool
	Payload []byte
}

// Manager is responsible for managing all websocket clients and the Redis connection
type Manager struct {
	// Register channel: when a new client joins, send the client pointer to the channel
	Register chan *Client

	// Unregister channel: when a client disconnects, send the client pointer to the channel
	Unregister chan *Client

	// Broadcast channel: when a new message is to be broadcast, send the []byte to the channel
	Broadcast chan *model.WsPacket

	// Rooms maps RoomID to a set of Clients in that room
	Rooms map[string]map[*Client]struct{}

	//Store cancel functions to stop Redis subscriptions
	cancelSub map[string]context.CancelFunc

	mu sync.RWMutex // Protects the Rooms map

	//Read and write mutex lock
	//RLock() when multiple routines read room map to broadcast simultaneously,
	//non blocking

	// Reducing GC pressure by avoiding frequent make([]*Client).
	clientPool sync.Pool
	// Channel to distribute broadcast tasks to background workers
	broadcastJobChan chan *BroadcastJob

	// RedisClient: the connection to the Redis server.
	RedisClient *redis.Client

	// KafkaProducer sarama.SyncProducer
	KafkaProducer sarama.AsyncProducer
}

const (
	BroadCastInterVal    = 3 * time.Second
	InitClientPoolCap    = 500
	BroadcastChanSize    = 1024
	BroadcastJobChanSize = 1000
)

func NewManager() *Manager {
	brokers := []string{"127.0.0.1:9092"}
	rdb := infra.InitRedisClient()
	producer := infra.InitKafkaProducer(brokers)
	return &Manager{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *model.WsPacket, BroadcastChanSize), // Buffer to handle spikes
		Rooms:      make(map[string]map[*Client]struct{}),
		cancelSub:  make(map[string]context.CancelFunc),
		clientPool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate a reasonable capacity
				// It will grow automatically if needed.
				return make([]*Client, 0, InitClientPoolCap)
			},
		},
		// Initialize Job Channel
		broadcastJobChan: make(chan *BroadcastJob, BroadcastJobChanSize),
		RedisClient:      rdb,
		KafkaProducer:    producer,
	}
}

// Creates the main loop for the manager.
func (m *Manager) Start() {
	logger.Log.Info("[MANAGER] Multi-room Manager started")
	m.StartWorkers()
	statsTicker := time.NewTicker(BroadCastInterVal)
	defer statsTicker.Stop()

	for {
		select {
		case client := <-m.Register:
			m.handleRegister(client)
		case client := <-m.Unregister:
			m.handleUnregister(client)
		case packet := <-m.Broadcast:
			m.handleBroadcast(packet)
		case <-statsTicker.C:
			m.broadcastStats()
		}
	}
}

// StartWorkers should be called inside Manager.Start()
func (m *Manager) StartWorkers() {
	// Start 10 background workers (adjust based on CPU cores)
	workerCount := 10
	for i := 0; i < workerCount; i++ {
		go m.broadcastWorker()
	}
}

// The Worker Logic
func (m *Manager) broadcastWorker() {
	for job := range m.broadcastJobChan {
		// 1. Iterate and Send
		for _, client := range job.Clients {
			select {
			case client.Send <- job.Payload:
			default:
				// Skip if full
			}
		}

		// 2. Return the slice to the Pool HERE
		// The worker is responsible for cleanup
		m.clientPool.Put(job.Clients)
	}
}

func (m *Manager) handleRegister(client *Client) {
	//write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Initialize room map if it's the first client in this room on THIS server
	if _, ok := m.Rooms[client.RoomID]; !ok {
		m.Rooms[client.RoomID] = make(map[*Client]struct{})
		// 1. Create a cancelable context for this room's subscription
		// no timeout:cancel() is called when empty room,server closing or unsubscribe
		ctx, cancel := context.WithCancel(context.Background())
		m.cancelSub[client.RoomID] = cancel
		// 2. Start a background goroutine to listen to THIS room's Redis channel
		// Pass context to subscriber
		go m.subscribeToRoom(ctx, client.RoomID)
		logger.Log.Info("[MANAGER] New room created on server", zap.String("room", client.RoomID))
	}

	//struct{} is type,second {} is initializing instance
	m.Rooms[client.RoomID][client] = struct{}{}

	// Async Redis update: Online Count
	// because Incr involves network I/O,TCP round trip,queue in redis,
	// potentially blocking,timeout,shaking etc.
	go infra.UpdateOnlineCount(m.RedisClient, client.RoomID, 1)
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
			// Clean up room and STOP subscription if last client leaves
			if len(clients) == 0 {
				delete(m.Rooms, client.RoomID)
				if cancel, exists := m.cancelSub[client.RoomID]; exists {
					cancel() // Signals subscribeToRoom to exit
					delete(m.cancelSub, client.RoomID)
					logger.Log.Info("[MANAGER] Room subscription stopped", zap.String("room", client.RoomID))
				}
			}
			// Async Redis update: Decr Online Count
			go infra.UpdateOnlineCount(m.RedisClient, client.RoomID, -1)
		}
	}
}
func (m *Manager) handleBroadcast(packet *model.WsPacket) {
	// Serialize the envelope for distribution
	payload, err := json.Marshal(packet)
	if err != nil {
		logger.Log.Error("[MANAGER] Marshal failed", zap.Error(err))
		return
	}

	switch packet.Type {
	// Case A: Danmaku (User generated)
	// Send to Redis (so other servers see it).
	// Send to Kafka (for history storage).
	// Note: We do NOT call m.broadcastToLocalClients here.
	// Why? Because Redis Sub goroutine will receive it and call broadcastToLocalClients.
	// If we call it here, the local user will see the message twice!
	case model.TypeDanmaku:
		m.processDanmaku(packet.RoomID, payload)
	// Case B: Like (User generated)
	// Send to Redis (real-time).
	// Skip Kafka (usually likes are just counters, detailed logs might not be needed).
	case model.ActionLike:
		m.processLike(packet)
	// Case A: Stats (Generated locally, sent locally)
	// Do NOT send to Redis (avoids broadcast storm).
	// Do NOT send to Kafka (no need to save transient stats).
	case model.TypeStats:
		m.broadcastToLocalClients(packet.RoomID, payload)
	default:
		logger.Log.Warn("[MANAGER] Unknown packet type", zap.Int("type", packet.Type))
	}
}

// processDanmaku handles user messages.
// Strategy: Persist to Kafka + Broadcast to All Servers (via Redis).
func (m *Manager) processDanmaku(roomID string, data []byte) {
	// Async IO to Redis and Kafka

	// 1. Persistence (Kafka)
	go infra.PushToInput(m.KafkaProducer, infra.DanmakuSaveTopic, data)

	// 2. Distribution (Redis Pub/Sub)
	// This ensures clients on other servers also receive this danmaku.
	go infra.PublishToRoom(m.RedisClient, roomID, data)
}

// processLike handles user likes.
// Strategy: Update State (Redis KV). Do NOT broadcast every single like to save bandwidth.
// The Ticker will pick up the new count later.
func (m *Manager) processLike(packet *model.WsPacket) {
	// 1. Business Logic: Increment Redis Counter
	// We need to peek inside the Data to know the "Count"
	var cmd model.Like
	if err := json.Unmarshal(packet.Data, &cmd); err == nil {
		// Only update the counter in Redis.
		go infra.IncrRoomLikes(m.RedisClient, packet.RoomID, cmd.Count)
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

		payload, err := json.Marshal(packet)
		if err != nil {
			logger.Log.Error("[MANAGER] Marshal stats packet failed", zap.Error(err))
			continue
		}
		// Non-blocking send to self (avoid deadlock if channel is full)
		// select {
		// case m.Broadcast <- packet:
		// default:
		// 	logger.Log.Warn("[MANAGER] Broadcast channel full, skipping stats", zap.String("room", roomID))
		// }

		m.broadcastToLocalClients(roomID, payload)
	}
}

// subscribeToRoom listens to a specific Redis channel for a room
func (m *Manager) subscribeToRoom(ctx context.Context, roomID string) {
	// Channel name: e.g., "room:1001:pubsub"
	channelName := fmt.Sprintf(infra.KeyRoomPubSub, roomID)
	//context.Background(): a default context that never cancels, never expires, and has no values.
	//or context.WithTimeOut(...,3*time.Second)
	//here ctx is cancellable
	pubsub := m.RedisClient.Subscribe(ctx, channelName)
	defer pubsub.Close()
	// Go channel to receive Redis messages(read only)
	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			// Graceful exit when cancel() is called
			logger.Log.Info("[MANAGER SUB] unsubscribing to room",
				zap.String("room", roomID),
			)
			return
		// Loop over messages received from Redis.
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// msg.Payload is the JSON string
			// Now we broadcast this message to all local clients in this room.
			m.broadcastToLocalClients(roomID, []byte(msg.Payload))
		}
	}
}

// broadcastToLocalClients sends raw bytes to all clients in a specific room.
func (m *Manager) broadcastToLocalClients(roomID string, payload []byte) {
	//read lock
	m.mu.RLock()
	clients, ok := m.Rooms[roomID]
	if !ok {
		m.mu.RUnlock()
		return
	}
	// Get from pool
	targetClients := m.clientPool.Get().([]*Client)
	targetClients = targetClients[:0] // Reset length
	for c := range clients {
		targetClients = append(targetClients, c)
	}
	m.mu.RUnlock()

	job := &BroadcastJob{
		Clients: targetClients,
		Payload: payload,
	}
	// 4. Non-blocking Dispatch to Workers
	select {
	case m.broadcastJobChan <- job:
		// Success: Worker will handle it
	default:
		// Failsafe: If workers are too slow, we must drop the job
		// and return the slice to pool immediately to prevent leak
		logger.Log.Warn("[MANAGER] Broadcast job queue full, dropping message", zap.String("room", roomID))
		m.clientPool.Put(targetClients)
	}
	// Sending to clients outside of Global Lock
	// for _, client := range targetClients {
	// 	select {
	// 	case client.Send <- payload:
	// 	default:
	// 		// FIX: Do NOT modify the map (delete) inside RLock.
	// 		// Instead, just close the channel and let WritePump handle the error
	// 		// delete(clients, client)
	// 		logger.Log.Warn("[MANAGER] Client buffer full, skipping message",
	// 			zap.Uint64("uid", client.UserID),
	// 		)
	// 	}
	// }
}
