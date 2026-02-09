package main

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"
)

const (
	KafkaTopic    = "danmaku_save_topic"
	TotalMessages = 5000000 // Test with 1M messages
	Goroutines    = 300     // Simulate concurrent clients
)

func sendDanmaku(producer sarama.AsyncProducer, topic string, payload []byte) {
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(payload),
	}
}

func main() {
	// 1. Init Logger
	logger.InitLogger("dev")
	defer logger.Sync()

	// 2. Init Producer (Using your optimized code)
	logger.Log.Info("[BENCHMARK] Initializing Producer...")
	brokers := []string{"127.0.0.1:9092"}
	producer := infra.InitKafkaProducer(brokers)

	// Critical: Close producer on exit to flush remaining buffered messages
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Log.Error("[BENCHMARK]Failed to close producer", zap.Error(err))
		}
	}()

	// 3. Prepare Data
	// Construct a standard packet
	danmaku := model.DanmakuMessage{
		// ID is handled by AutoIncrement in DB, usually 0 here
		RoomID:   "room_benchmark_1",
		UserID:   12345, // Mock User ID
		Content:  "This is a stress test message for Kafka optimization",
		SendTime: time.Now(),
	}

	contentBytes, err := json.Marshal(danmaku)
	if err != nil {
		logger.Log.Fatal("[BENCHMARK] Failed to marshal danmaku", zap.Error(err))
	}

	packet := model.WsPacket{
		Type:   model.TypeDanmaku,
		RoomID: "room_benchmark_1",
		Data:   contentBytes,
	}
	payload, err := json.Marshal(packet)
	if err != nil {
		logger.Log.Fatal("[BENCHMARK] Failed to marshal packet", zap.Error(err))
	}

	// 4. Start Benchmark
	logger.Log.Info("[BENCHMARK] Starting...",
		zap.Int("total", TotalMessages),
		zap.Int("concurrency", Goroutines))

	var sentCount int64
	wg := sync.WaitGroup{}
	startTime := time.Now()

	msgsPerWorker := TotalMessages / Goroutines

	for i := 0; i < Goroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < msgsPerWorker; j++ {
				sendDanmaku(producer, KafkaTopic, payload)
				atomic.AddInt64(&sentCount, 1)
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 5. Report Results
	logger.Log.Info("[BENCHMARK] Finished",
		zap.Int64("sent", sentCount),
		zap.Duration("duration", duration),
		zap.Float64("throughput_qps", float64(TotalMessages)/duration.Seconds()),
	)

	// Give some time for Async Producer to flush remaining background tasks/errors
	logger.Log.Info("[BENCHMARK] Waiting 3 seconds for background flushes...")
	time.Sleep(3 * time.Second)
}
