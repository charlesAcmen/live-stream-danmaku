package infra

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"go.uber.org/zap"
)

func InitKafkaProducer() sarama.AsyncProducer {
	// Init Kafka Producer
	// Configure Sarama settings
	config := sarama.NewConfig()
	config.ChannelBufferSize = 4096 // Allow more buffering in memory
	// We must wait for the acknowledgment from Kafka to ensure data is safe.
	//   - NoResponse
	//   - WaitForLocal: Leader returns OK after receiving
	//   - WaitForAll: all follower synced
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Snappy is the standard for Kafka logging (Google developed, fast/low CPU).
	// This drastically reduces network bandwidth usage.
	config.Producer.Compression = sarama.CompressionSnappy

	// Improve Batching.
	// Sarama will wait up to 10ms or until batch size is reached.
	// This reduces the number of requests sent to the broker.
	config.Producer.Flush.Frequency = 10 * time.Millisecond

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
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
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
		logger.Log.Warn("[KAFKA INFRA] Kafka input buffer full, dropping persistence message")
	}
}
