package infra

import (
	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"go.uber.org/zap"
)

func InitKafkaProducer() sarama.AsyncProducer {
	// 2. Init Kafka Producer
	// Configure Sarama settings
	config := sarama.NewConfig()
	// We must wait for the acknowledgment from Kafka to ensure data is safe.
	//   - NoResponse
	//   - WaitForLocal: Leader returns OK after receiving
	//   - WaitForAll: all follower synced
	config.Producer.RequiredAcks = sarama.WaitForAll
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
		logger.Log.Panic("[INFRA]Failed to start Kafka async producer", zap.Error(err))
	}
	//listen from KAFKA producer error channel
	//if not doing so,errors will fill in channel till blocking producer
	go func() {
		for err := range producer.Errors() {
			logger.Log.Error("[MANAGER] Kafka Async Write Error",
				zap.Error(err.Err),
				zap.Any("msg", err.Msg),
			)
		}
	}()
	return producer
}
