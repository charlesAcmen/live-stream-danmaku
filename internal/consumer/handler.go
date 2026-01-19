package consumer

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DanmakuHandler inherits from sarama.ConsumerGroupHandler interface
// collect in batch、flush on clock、push to db
type DanmakuHandler struct {
	db        *gorm.DB
	buffer    []*model.DanmakuMessage
	batchSize int
	mu        sync.Mutex // protect buffer
}

// NewDanmakuHandler
func NewDanmakuHandler(db *gorm.DB, batchSize int) *DanmakuHandler {
	return &DanmakuHandler{
		db:        db,
		buffer:    make([]*model.DanmakuMessage, 0, batchSize),
		batchSize: batchSize,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (h *DanmakuHandler) Setup(sarama.ConsumerGroupSession) error {
	logger.Log.Info("[KAFKA HANDLER] Consumer group session started")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h *DanmakuHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// Critical: Flush remaining data in the buffer to DB before exiting.
	// This prevents data loss during rebalancing or shutdown.
	h.flushDB()
	logger.Log.Info("[KAFKA HANDLER] Consumer group session ended")
	return nil
}

// ConsumeClaim
func (h *DanmakuHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Use a ticker to ensure data is flushed periodically even if the buffer isn't full.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// The loop continues until the claim is closed or the context is canceled.
	for {
		select {
		// Case 1: Receive a new message from Kafka
		case msg, ok := <-claim.Messages():
			if !ok {
				// Channel closed, meaning rebalancing or shutdown initiated.
				logger.Log.Warn("[KAFKA HANDLER] Message channel closed")
				return nil
			}
			// Process the message (parse JSON, add to buffer)
			h.processMessage(msg)

			// Mark the message as consumed.
			// Note: This does not commit the offset immediately. Sarama commits periodically in the background.
			session.MarkMessage(msg, "")

		case <-ticker.C:
			h.flushDB()

		// Case 3: External shutdown signal (e.g., Ctrl+C)
		case <-session.Context().Done():
			return nil
		}
	}
}

// processMessage parses the raw Kafka message and appends it to the local buffer.
func (h *DanmakuHandler) processMessage(msg *sarama.ConsumerMessage) {
	// out most Packet
	var packet model.WsPacket
	if err := json.Unmarshal(msg.Value, &packet); err != nil {
		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal WsPacket", zap.Error(err))
		return
	}

	// boolean filter:only danmaku
	if packet.Type != model.TypeDanmaku {
		return
	}

	// resolve inner danmaku
	var danmu model.DanmakuMessage
	if err := json.Unmarshal(packet.Data, &danmu); err != nil {
		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal DanmakuMessage", zap.Error(err))
		return
	}

	h.mu.Lock()
	h.buffer = append(h.buffer, &danmu)
	// Check if buffer is full (Batch processing)
	shouldFlush := len(h.buffer) >= h.batchSize
	h.mu.Unlock()

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

	// GORM
	if err := h.db.CreateInBatches(h.buffer, 100).Error; err != nil {
		logger.Log.Error("[KAFKA HANDLER]Insert DB Failed", zap.Error(err))
		// 工业级思考：这里如果失败了，buffer 里的数据可能会丢失。
		// 进阶做法是重试，或者写入本地文件兜底。目前先打印 Error。
	} else {
		logger.Log.Info("[KAFKA HANDLER]Saved messages to MySQL", zap.Int("count", count))
	}

	// Clear the buffer but keep the underlying capacity to avoid memory reallocation.
	h.buffer = h.buffer[:0]
}
