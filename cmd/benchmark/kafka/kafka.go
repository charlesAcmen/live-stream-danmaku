package main

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"go.uber.org/zap"
)

const (
	KafkaTopic    = "danmaku_save_topic"
	TotalMessages = 100000 // Test with 100k messages
	Goroutines    = 10     // Simulate concurrent clients
)

func main() {
	// 1. Init Logger
	logger.InitLogger("dev")
	defer logger.Sync()

	// 2. Init Producer (Using your optimized code)
	logger.Log.Info("[BENCHMARK] Initializing Producer...")
	producer := infra.InitKafkaProducer()

	// Critical: Close producer on exit to flush remaining buffered messages
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Log.Error("[BENCHMARK]Failed to close producer", zap.Error(err))
		}
	}()

	// 3. Prepare Data
	// Construct a standard packet
	mockContent := make(map[string]interface{})
	mockContent["content"] = "This is a stress test message for Kafka optimization"
	mockContent["send_time"] = time.Now().UnixMilli() // Important for ordering later

	contentBytes, _ := json.Marshal(mockContent)

	packet := model.WsPacket{
		Type:   model.TypeDanmaku,
		RoomID: "room_benchmark_1",
		Data:   contentBytes,
	}
	payload, _ := json.Marshal(packet)

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
				// Assuming your PushToInput signature is (producer, topic, payload)
				infra.PushToInput(producer, KafkaTopic, payload)
				atomic.AddInt64(&sentCount, 1)
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
