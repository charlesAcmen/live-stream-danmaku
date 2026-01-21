package consumer

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
	"go.uber.org/zap"
)

// DanmakuHandler inherits from sarama.ConsumerGroupHandler interface
// Responsibilities:
// 1. Accumulate messages in a buffer (Batching).
// 2. Flush to DB periodically or when buffer is full.
// 3. Handle Kafka session lifecycle (Setup/Cleanup).
type DanmakuHandler struct {
	repo      *repo.MessageRepo
	buffer    []*model.DanmakuMessage
	batchSize int
	mu        sync.Mutex // protect buffer
}

const (
	// FlushInterval is the time period to trigger a DB flush.
	// This works because time.Second is a constant.
	FlushInterval = 2 * time.Second

	// MaxDBRetries defines the maximum number of retry attempts when persisting messages to the database.
	// This helps improve reliability by allowing for transient errors, such as temporary DB outages.
	MaxDBRetries = 3
)

// NewDanmakuHandler creates a new handler instance.
func NewDanmakuHandler(r *repo.MessageRepo, batchSize int) *DanmakuHandler {
	return &DanmakuHandler{
		repo:      r,
		buffer:    make([]*model.DanmakuMessage, 0, batchSize),
		batchSize: batchSize,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
// Use this to initialize assets or reset state.
func (h *DanmakuHandler) Setup(sarama.ConsumerGroupSession) error {
	logger.Log.Info("[KAFKA HANDLER] Consumer group session started")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h *DanmakuHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// Critical: Flush remaining data in the buffer to DB before exiting.
	// This prevents data loss during rebalancing(when Kafka assigns
	// partitions to other consumers)
	// or during graceful shutdown.
	h.flushDB()
	logger.Log.Info("[KAFKA HANDLER] Consumer group session ended")
	return nil
}

// ConsumeClaim is the main loop for processing messages.
// It runs in a separate goroutine managed by Sarama.
func (h *DanmakuHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Use a ticker to ensure data is flushed periodically even if the buffer isn't full.
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	// The loop continues until the claim is closed or the context is canceled.
	for {
		select {
		// Case 1: Receive a new message from Kafka
		case msg, ok := <-claim.Messages():
			if !ok {
				// Channel closed means the session is ending (Rebalance or Shutdown).
				logger.Log.Warn("[KAFKA HANDLER] Message channel closed")
				return nil
			}
			// Process business logic(parse JSON, add to buffer)
			h.processMessage(msg)

			// Mark the message as consumed.
			// Note: This only marks it in memory.
			// Sarama auto-commits offsets to Kafka periodically.
			session.MarkMessage(msg, "")

		case <-ticker.C:
			h.flushDB()

		// Case 3: Context canceled (Graceful Shutdown)
		case <-session.Context().Done():
			return nil
		}
	}
}

// processMessage parses the raw Kafka message and appends it to the local buffer.
func (h *DanmakuHandler) processMessage(msg *sarama.ConsumerMessage) {
	// 1. Unmarshal outer envelope (WsPacket)
	var packet model.WsPacket
	if err := json.Unmarshal(msg.Value, &packet); err != nil {
		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal WsPacket", zap.Error(err))
		return
	}

	// 2. Filter: Only process Danmaku messages
	if packet.Type != model.TypeDanmaku {
		return
	}

	// 3. Unmarshal inner data (DanmakuMessage)
	var danmaku model.DanmakuMessage
	if err := json.Unmarshal(packet.Data, &danmaku); err != nil {
		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal DanmakuMessage", zap.Error(err))
		return
	}

	// 4. Add to buffer (Thread-Safe)
	h.mu.Lock()
	h.buffer = append(h.buffer, &danmaku)
	shouldFlush := len(h.buffer) >= h.batchSize
	h.mu.Unlock()

	// 5. Trigger flush if buffer is full
	if shouldFlush {
		h.flushDB()
	}
}

// flushDB writes the buffered messages to the database in a single batch.
func (h *DanmakuHandler) flushDB() {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Optimization: Don't query DB if buffer is emptys
	if len(h.buffer) == 0 {
		return
	}

	count := len(h.buffer)
	logger.Log.Debug("[KAFKA HANDLER]Flushing data to DB", zap.Int("count", count))

	// Simple Retry Logic (For transient DB failures)
	maxRetries := MaxDBRetries
	for i := 0; i < maxRetries; i++ {
		// Call Repo layer to execute bulk insert
		err := h.repo.CreateInBatches(h.buffer)
		if err == nil {
			// Success
			logger.Log.Info("[KAFKA HANDLER]Saved messages to MySQL", zap.Int("count", count))
			// Clear buffer (keep capacity)
			h.buffer = h.buffer[:0]
			return
		}
		// Failure
		logger.Log.Warn("[KAFKA HANDLER] Insert failed, retrying...",
			zap.Int("attempt", i+1),
			zap.Error(err))
		time.Sleep(500 * time.Millisecond)
	}

	// If all retries fail, data is dropped here.
	// Future improvement: Write to local file or error topic.
	logger.Log.Error("[KAFKA HANDLER] Final failure: Data dropped", zap.Int("count", count))
	h.buffer = h.buffer[:0] // Force clear to prevent OOM
}
