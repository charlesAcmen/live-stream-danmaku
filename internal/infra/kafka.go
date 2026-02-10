package infra

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"go.uber.org/zap"
)

const (
	DanmakuSaveTopic    = "danmaku_save_topic" // Topic name for danmaku saving
	ProducerChanBufSize = 16384
)

func InitKafkaProducer(brokers []string) sarama.AsyncProducer {
	// Init Kafka Producer
	// Configure Sarama settings
	config := sarama.NewConfig()
	config.ChannelBufferSize = ProducerChanBufSize // Allow more buffering in memory
	// We must wait for the acknowledgment from Kafka to ensure data is safe.
	//   - NoResponse
	//   - WaitForLocal: Leader returns OK after receiving
	//   - WaitForAll: all follower synced
	// For Danmaku, speed > absolute consistency.
	config.Producer.RequiredAcks = sarama.WaitForLocal

	// Snappy is the standard for Kafka logging (Google developed, fast/low CPU).
	// This drastically reduces network bandwidth usage.
	config.Producer.Compression = sarama.CompressionSnappy

	// Improve Batching.
	// Sarama will wait up to 10ms or until batch size is reached.
	// This reduces the number of requests sent to the broker.
	// This prevents sending too many tiny packets.
	config.Producer.Flush.Frequency = 10 * time.Millisecond
	config.Producer.Flush.Bytes = 16 * 1024 // 16KB
	// We need to return success info to avoid errors in SyncProducer.
	// config.Producer.Return.Successes = true

	// performance optimization:no return for successful msgs(reduce channel overhead)
	config.Producer.Return.Successes = false
	//return msgs with error,otherwise have no ability to monitor KAFKA
	config.Producer.Return.Errors = true
	// multiple partitions in one topic of Kafka
	// Use Random partitioner to distribute messages evenly in all partitions
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// Connect to Kafka (running on localhost:9092 (in .yaml) via Docker)1

	// producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		// In production, we might want to retry or fail gracefully.
		// For now, we panic because without Kafka, persistence fails.
		logger.Log.Panic("[KAFKA INFRA]Failed to start Kafka async producer", zap.Error(err))
	}
	//listen from KAFKA producer error channel
	//if not doing so,errors will fill in channel till blocking producer
	go func() {
		for err := range producer.Errors() {
			logger.Log.Error("[KAFKA INFRA] Kafka Async Write Error",
				zap.Error(err.Err),
				zap.Any("msg", err.Msg),
			)
		}
	}()
	return producer
}

// InitKafkaConsumerGroup initializes the consumer group with high-throughput settings.
func InitKafkaConsumerGroup(brokers []string, groupID string) sarama.ConsumerGroup {
	config := sarama.NewConfig()
	// OffsetOldest: If the group is new, start from the very beginning.
	// This ensures we don't miss messages sent while the consumer was down.
	// OffsetNewest: discard history
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Performance: Fetch Settings (Critical for Batch Processing)
	// Default is 1MB. Increase to 5MB to reduce network round-trips.
	// We want to fetch a large chunk of data to feed our batch handler.
	config.Consumer.Fetch.Default = 5 * 1024 * 1024
	// Limit error logging for debugging
	config.Consumer.Return.Errors = true

	// Connect to Brokers
	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		logger.Log.Panic("[KAFKA INFRA] Failed to create consumer group", zap.Error(err))
	}

	// Monitor Errors in background
	// It is safer to handle errors here (logging) than leaving the channel full blocking the consumer.
	go func() {
		for err := range group.Errors() {
			logger.Log.Error("[KAFKA INFRA] Consumer Group Error", zap.Error(err))
		}
	}()

	return group
}

// PushToInput acts as a helper to hide the select-default logic
func PushToInput(producer sarama.AsyncProducer, topic string, payload []byte) {
	// Produce to Kafka (For Storage/Persistence)
	// 'payload' is already a JSON bytes containing user info & content.
	msg := &sarama.ProducerMessage{
		Topic: topic,
		//Kafka is a byte logging system that deal with binary bytes stream
		Value: sarama.ByteEncoder(payload),
	}
	select {
	// Async send (Non-blocking)
	case producer.Input() <- msg:
	default:
		// [CIRCUIT BREAKER]
		// If Sarama's internal buffer (Channel) is full, we DROP the message.
		// This prevents the Manager from hanging and ensures real-time broadcast (Redis)
		// remains unaffected even if Kafka is slow or down.
		// logger.Log.Warn("[KAFKA INFRA] Kafka input buffer full, dropping persistence message")
	}
}
