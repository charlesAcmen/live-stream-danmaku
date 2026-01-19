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

// DanmakuHandler 实现了 sarama.ConsumerGroupHandler 接口
// 它负责攒批、定时flush、写入数据库
type DanmakuHandler struct {
	db        *gorm.DB
	buffer    []*model.DanmakuMessage
	batchSize int
	mu        sync.Mutex // 保护 buffer，防止并发读写冲突
}

// NewDanmakuHandler 创建处理器实例
func NewDanmakuHandler(db *gorm.DB, batchSize int) *DanmakuHandler {
	return &DanmakuHandler{
		db:        db,
		buffer:    make([]*model.DanmakuMessage, 0, batchSize),
		batchSize: batchSize,
	}
}

// Setup 在新会话开始前运行（sarama 接口要求）
func (h *DanmakuHandler) Setup(sarama.ConsumerGroupSession) error {
	logger.Log.Info("[KAFKA HANDLER] Consumer group session started")
	return nil
}

// Cleanup 在会话结束后运行（sarama 接口要求）
func (h *DanmakuHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// 退出前，强制把剩余的数据写入数据库
	h.flushDB()
	logger.Log.Info("[KAFKA HANDLER] Consumer group session ended")
	return nil
}

// ConsumeClaim 是核心循环，必须在这里读取消息
func (h *DanmakuHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 定义定时器，防止 buffer 没满导致数据一直不存
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		// 1. 收到 Kafka 消息
		case msg, ok := <-claim.Messages():
			if !ok {
				logger.Log.Warn("[KAFKA HANDLER] Message channel closed")
				return nil
			}

			// 处理这一条消息
			h.processMessage(msg)

			// 标记这条消息已被消费（注意：这里只是标记，Commit 是异步的）
			session.MarkMessage(msg, "")

		// 2. 定时器响了，强制 flush
		case <-ticker.C:
			h.flushDB()

		// 3. 外部通知退出 (比如 Ctrl+C)
		case <-session.Context().Done():
			return nil
		}
	}
}

// processMessage 解析并添加到 buffer
func (h *DanmakuHandler) processMessage(msg *sarama.ConsumerMessage) {
	// 解析外层 Packet
	var packet model.WsPacket
	if err := json.Unmarshal(msg.Value, &packet); err != nil {
		logger.Log.Error("Failed to unmarshal WsPacket", zap.Error(err))
		return
	}

	// 过滤：只处理弹幕消息
	if packet.Type != model.TypeDanmaku {
		return
	}

	// 解析内层数据
	var danmu model.DanmakuMessage
	if err := json.Unmarshal(packet.Data, &danmu); err != nil {
		logger.Log.Error("Failed to unmarshal DanmakuMessage", zap.Error(err))
		return
	}

	h.mu.Lock()
	h.buffer = append(h.buffer, &danmu)
	shouldFlush := len(h.buffer) >= h.batchSize
	h.mu.Unlock()

	// 如果满了，立即写入
	if shouldFlush {
		h.flushDB()
	}
}

// flushDB 将 buffer 数据写入 MySQL
func (h *DanmakuHandler) flushDB() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.buffer) == 0 {
		return
	}

	count := len(h.buffer)
	logger.Log.Debug("Flushing data to DB", zap.Int("count", count))

	// 使用 GORM 批量插入
	if err := h.db.CreateInBatches(h.buffer, 100).Error; err != nil {
		logger.Log.Error("Insert DB Failed", zap.Error(err))
		// 工业级思考：这里如果失败了，buffer 里的数据可能会丢失。
		// 进阶做法是重试，或者写入本地文件兜底。目前先打印 Error。
	} else {
		logger.Log.Info("Saved messages to MySQL", zap.Int("count", count))
	}

	// 清空 buffer，保留容量
	h.buffer = h.buffer[:0]
}
