// package consumer

// /*
// DB is at-most-once
// Kafka is at-least-once:
// mark first flush second
// flush fail->retry
// retry fail->drop
// offset still commits automatically

// does not guarantee flush in db
// */
// import (
// 	"context"
// 	"encoding/json"
// 	"sync" //buffer pool
// 	"time"

// 	"github.com/IBM/sarama"
// 	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
// 	"github.com/charlesAcmen/livestream-danmaku/internal/model"
// 	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
// 	"go.uber.org/zap"
// )

// // DanmakuHandler inherits from sarama.ConsumerGroupHandler interface
// // Responsibilities:
// // 1. Accumulate messages in a buffer (Batching).
// // 2. Flush to DB periodically or when buffer is full.
// // 3. Handle Kafka session lifecycle (Setup/Cleanup).
// type DanmakuHandler struct {
// 	repo       *repo.MessageRepo
// 	buffer     []*model.DanmakuMessage
// 	batchSize  int
// 	mu         sync.Mutex // protect buffer
// 	bufferPool *sync.Pool //pool for memory reuse
// }

// const (
// 	// FlushInterval is the time period to trigger a DB flush.
// 	// This works because time.Second is a constant.
// 	FlushInterval = 2 * time.Second

// 	// MaxDBRetries defines the maximum number of retry attempts when persisting messages to the database.
// 	// This helps improve reliability by allowing for transient errors, such as temporary DB outages.
// 	MaxDBRetries = 3
// )

// // NewDanmakuHandler creates a new handler instance.
// func NewDanmakuHandler(r *repo.MessageRepo, batchSize int) *DanmakuHandler {
// 	return &DanmakuHandler{
// 		repo:      r,
// 		buffer:    make([]*model.DanmakuMessage, 0, batchSize),
// 		batchSize: batchSize,
// 		bufferPool: &sync.Pool{
// 			New: func() interface{} {
// 				return make([]*model.DanmakuMessage, 0, batchSize)
// 			}, //New func for pool
// 		},
// 	}
// }

// // Setup is run at the beginning of a new session, before ConsumeClaim.
// // Use this to initialize assets or reset state.
// func (h *DanmakuHandler) Setup(sarama.ConsumerGroupSession) error {
// 	logger.Log.Info("[KAFKA HANDLER] Consumer group session started")
// 	return nil
// }

// // Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
// func (h *DanmakuHandler) Cleanup(sarama.ConsumerGroupSession) error {
// 	// Critical: Flush remaining data in the buffer to DB before exiting.
// 	// This prevents data loss during rebalancing(when Kafka assigns
// 	// partitions to other consumers)
// 	// or during graceful shutdown.
// 	h.flushDB(context.Background())
// 	logger.Log.Info("[KAFKA HANDLER] Consumer group session ended")
// 	return nil
// }

// // ConsumeClaim is the main loop for processing messages.
// // It runs in a separate goroutine managed by Sarama.
// func (h *DanmakuHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
// 	// Use a ticker to ensure data is flushed periodically even if the buffer isn't full.
// 	ticker := time.NewTicker(FlushInterval)
// 	defer ticker.Stop()

// 	// The loop continues until the claim is closed or the context is canceled.
// 	for {
// 		select {
// 		// Case 1: Receive a new message from Kafka
// 		case msg, ok := <-claim.Messages():
// 			if !ok {
// 				// Channel closed means the session is ending (Rebalance or Shutdown).
// 				logger.Log.Warn("[KAFKA HANDLER] Message channel closed")
// 				return nil
// 			}
// 			// Process business logic(parse JSON, add to buffer)
// 			// Pass session context down
// 			h.processMessage(session.Context(), msg)

// 			// Mark the message as consumed.
// 			// Note: This only marks it in memory.
// 			// Sarama auto-commits offsets to Kafka periodically.
// 			session.MarkMessage(msg, "")

// 		case <-ticker.C:
// 			h.flushDB(session.Context())

// 		// Case 3: Context canceled (Graceful Shutdown)
// 		case <-session.Context().Done():
// 			return nil
// 		}
// 	}
// }

// // processMessage parses the raw Kafka message and appends it to the local buffer.
// func (h *DanmakuHandler) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) {
// 	// 1. Unmarshal outer envelope (WsPacket)
// 	var packet model.WsPacket
// 	if err := json.Unmarshal(msg.Value, &packet); err != nil {
// 		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal WsPacket", zap.Error(err))
// 		return
// 	}

// 	// 2. Filter: Only process Danmaku messages
// 	if packet.Type != model.TypeDanmaku {
// 		return
// 	}

// 	// 3. Unmarshal inner data (DanmakuMessage)
// 	var danmaku model.DanmakuMessage
// 	if err := json.Unmarshal(packet.Data, &danmaku); err != nil {
// 		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal DanmakuMessage", zap.Error(err))
// 		return
// 	}

// 	// 4. Add to buffer (Thread-Safe)
// 	h.mu.Lock()
// 	h.buffer = append(h.buffer, &danmaku)
// 	shouldFlush := len(h.buffer) >= h.batchSize
// 	h.mu.Unlock()

// 	// 5. Trigger flush if buffer is full
// 	if shouldFlush {
// 		h.flushDB(ctx)
// 	}
// }

// // flushDB writes the buffered messages to the database in a single batch.
// func (h *DanmakuHandler) flushDB(ctx context.Context) {
// 	var toFlush []*model.DanmakuMessage

// 	// --- Critical Section: Swap Buffers ---
// 	h.mu.Lock()
// 	// defer h.mu.Unlock()
// 	// Optimization: Don't query DB if buffer is emptys
// 	if len(h.buffer) == 0 {
// 		h.mu.Unlock()
// 		return
// 	}
// 	toFlush = h.buffer
// 	// Replace h.buffer with a fresh slice from the pool (Double Buffering)
// 	// Type assertion is safe here because New() returns correct type
// 	h.buffer = h.bufferPool.Get().([]*model.DanmakuMessage)
// 	h.mu.Unlock()

// 	count := len(toFlush)
// 	logger.Log.Debug("[KAFKA HANDLER]Flushing data to DB", zap.Int("count", count))

// 	// Ensure we return the slice to the pool after processing
// 	defer func() {
// 		// Reset length to 0 but keep capacity
// 		toFlush = toFlush[:0]
// 		h.bufferPool.Put(toFlush)
// 	}()

// 	// Simple Retry Logic (For transient DB failures)
// 	maxRetries := MaxDBRetries
// 	for i := 0; i < maxRetries; i++ {
// 		select {
// 		case <-ctx.Done():
// 			logger.Log.Warn("[KAFKA HANDLER] Context cancelled, aborting retry", zap.Int("dropped_count", count))
// 			return // Exit immediately without retrying
// 		default:
// 			// Call Repo layer to execute bulk insert
// 			err := h.repo.CreateInBatches(toFlush)
// 			if err == nil {
// 				// Success
// 				logger.Log.Info("[KAFKA HANDLER]Saved messages to MySQL", zap.Int("count", count))
// 				// Clear buffer (keep capacity)
// 				// h.buffer = h.buffer[:0]
// 				return
// 			}
// 			// Failure
// 			logger.Log.Warn("[KAFKA HANDLER] Insert failed, retrying...",
// 				zap.Int("attempt", i+1),
// 				zap.Error(err))
// 			// Wait before retry, but listen to context cancellation
// 			select {
// 			case <-ctx.Done():
// 				logger.Log.Warn("[KAFKA HANDLER] Context cancelled during backoff, aborting", zap.Int("dropped_count", count))
// 				return
// 			case <-time.After(500 * time.Millisecond):
// 				// Continue loop
// 			}
// 		}
// 	}

// 	// If all retries fail, data is dropped here.
// 	// Future improvement: Write to local file or error topic.
// 	logger.Log.Error("[KAFKA HANDLER] Final failure: Data dropped", zap.Int("count", count))
// 	// h.buffer = h.buffer[:0] // Force clear to prevent OOM
// }

package consumer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
	"go.uber.org/zap"
)

const (
	// FlushInterval triggers DB write if buffer isn't full after this time.
	FlushInterval = 2 * time.Second

	// MaxDBRetries defines retry attempts for DB persistence.
	MaxDBRetries = 3

	// ChannelBufferSize defines how many messages can be queued before blocking the Kafka consumer.
	// This acts as a backpressure mechanism.
	ChannelBufferSize = 2000

	BackoffDuration = 500 * time.Millisecond
)

// DanmakuHandler handles Kafka messages and asynchronously flushes them to DB.
type DanmakuHandler struct {
	repo      *repo.MessageRepo
	batchSize int

	// msgChan is the pipeline between Kafka consumer and DB worker.
	msgChan chan *model.DanmakuMessage

	// wg ensures the DB worker finishes processing before the app exits.
	wg sync.WaitGroup
}

// NewDanmakuHandler creates a new handler instance.
func NewDanmakuHandler(r *repo.MessageRepo, batchSize int) *DanmakuHandler {
	return &DanmakuHandler{
		repo:      r,
		batchSize: batchSize,
		// Buffered channel to decouple consumption from DB writes.
		msgChan: make(chan *model.DanmakuMessage, ChannelBufferSize),
	}
}

// Setup is run at the beginning of a new session before ConsumeClaim.
// to initialize assets or reset state
// We start the background DB worker here.
func (h *DanmakuHandler) Setup(session sarama.ConsumerGroupSession) error {
	logger.Log.Info("[KAFKA HANDLER] Consumer group session started")

	// Start the background worker that listens to the channel and flushes to DB.
	h.wg.Add(1)
	go h.dbWorker(session.Context())

	return nil
}

// Cleanup is run at the end of a session.once all ConsumeClaim goroutines have exited.
func (h *DanmakuHandler) Cleanup(sarama.ConsumerGroupSession) error {
	logger.Log.Info("[KAFKA HANDLER] Consumer group session ending, shutting down worker...")

	// Close the channel to signal the worker to finish up.
	close(h.msgChan)

	// Wait for the worker to flush remaining data and exit.
	h.wg.Wait()

	logger.Log.Info("[KAFKA HANDLER] Consumer group session ended cleanly")
	return nil
}

// ConsumeClaim reads from Kafka and pushes to the local channel.
// It runs in a separate goroutine managed by Sarama.
// This loop is now extremely fast as it involves no DB I/O.
func (h *DanmakuHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

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

			// 1. Process and parse the message
			danmaku, err := h.parseMessage(msg)
			if err != nil {
				// Log error but don't stop consumption
				// logger.Log.Error("[KAFKA HANDLER] Parse error", zap.Error(err))
				// Mark as consumed so we don't get stuck on a bad message
				session.MarkMessage(msg, "")
				continue
			}

			// 2. Send to the async worker via channel.
			// If channel is full, this will block, providing natural backpressure
			// to slow down Kafka consumption.
			select {
			case h.msgChan <- danmaku:
				// 3. Mark message as consumed (At-Least-Once consideration below)
				// Note: In high-perf async mode, we usually mark here.
				// If the app crashes between here and DB flush, data is lost (trade-off for speed).
				// If strict reliability is required, you must wait for DB ack, which defeats async purpose.
				session.MarkMessage(msg, "")
			//Context canceled (Graceful Shutdown)
			case <-ctx.Done():
				return nil
			}

		case <-ctx.Done():
			return nil
		}
	}
}

// dbWorker is the background goroutine responsible for batching and inserting into DB.
func (h *DanmakuHandler) dbWorker(ctx context.Context) {
	defer h.wg.Done()

	// Local buffer for the worker
	// only make once for each worker
	buffer := make([]*model.DanmakuMessage, 0, h.batchSize)
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	logger.Log.Info("[DB WORKER] Worker started")

	for {
		select {
		// Case 1: Receive message from Consumer
		case msg, ok := <-h.msgChan:
			if !ok {
				// Channel closed by Cleanup, flush remaining and exit
				h.flushBuffer(ctx, buffer)
				return
			}
			// Append message to buffer
			buffer = append(buffer, msg)
			// If buffer is full, flush to DB
			if len(buffer) >= h.batchSize {
				h.flushBuffer(ctx, buffer)
				buffer = buffer[:0] // Reset buffer
			}

		// Case 2: Time to flush
		case <-ticker.C:
			if len(buffer) > 0 {
				h.flushBuffer(ctx, buffer)
				buffer = buffer[:0]
			}

		// Case 3: Context cancelled (Safety net)
		case <-ctx.Done():
			// Try one last flush if context allows, or just log
			if len(buffer) > 0 {
				logger.Log.Warn("[DB WORKER] Context done, attempting final flush")
				h.flushBuffer(context.Background(), buffer) // Use background context to ensure flush
			}
			return
		}
	}
}

// parseMessage encapsulates the parsing logic.
func (h *DanmakuHandler) parseMessage(msg *sarama.ConsumerMessage) (*model.DanmakuMessage, error) {
	var packet model.WsPacket
	if err := json.Unmarshal(msg.Value, &packet); err != nil {
		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal WsPacket", zap.Error(err))
		return nil, err
	}

	//Filter: Only process Danmaku messages
	if packet.Type != model.TypeDanmaku {
		// Return nil without error to signal "skip this message"
		return nil, nil // Or handle specific error types
	}

	var danmaku model.DanmakuMessage
	if err := json.Unmarshal(packet.Data, &danmaku); err != nil {
		logger.Log.Error("[KAFKA HANDLER]Failed to unmarshal DanmakuMessage", zap.Error(err))
		return nil, err
	}
	return &danmaku, nil
}

// flushBuffer performs the actual DB insertion with retries.
func (h *DanmakuHandler) flushBuffer(ctx context.Context, batch []*model.DanmakuMessage) {
	if len(batch) == 0 {
		return
	}

	// Copy slice to avoid race conditions if we were reusing the same backing array in complex ways,
	// though here buffer[:0] is safe as long as insert finishes before next append.
	// For repo calls, usually safe to pass directly.
	count := len(batch)

	// Retry Logic (For transient DB failures)
	for i := 0; i < MaxDBRetries; i++ {
		err := h.repo.CreateInBatches(batch)
		if err == nil {
			logger.Log.Info("[DB WORKER] Saved batch to MySQL", zap.Int("count", count))
			return
		}

		logger.Log.Warn("[DB WORKER] Insert failed, retrying...",
			zap.Int("attempt", i+1),
			zap.Error(err))

		// Backoff
		select {
		case <-time.After(BackoffDuration):
			// Continue loop
		case <-ctx.Done():
			logger.Log.Error("[DB WORKER] Context cancelled during backoff, dropping data", zap.Int("count", count))
			return
		}
	}

	logger.Log.Error("[DB WORKER] Final failure: Data dropped after retries", zap.Int("count", count))
}
