package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/charlesAcmen/livestream-danmaku/internal/consumer"
	"github.com/charlesAcmen/livestream-danmaku/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/repo"
)

const (
	KafkaTopic   = "danmaku_save_topic"
	KafkaGroupID = "danmaku_group_v1" // consumer group ID
	BatchSize    = 100
)

func main() {
	// 1. init Logger
	logger.InitLogger("dev")
	defer logger.Sync()

	logger.Log.Info("[KAFKA CONSUMER]Starting Kafka Consumer...")

	// 2. init db
	db := infra.InitDB()
	logger.Log.Info("[KAFKA CONSUMER]Database initialized")

	messageRepo := repo.NewMessageRepo(db)
	handler := consumer.NewDanmakuHandler(messageRepo, BatchSize)

	brokers := []string{"127.0.0.1:9092"}

	group := infra.InitKafkaConsumerGroup(brokers, KafkaGroupID)
	defer group.Close()

	// 5. Setup Graceful Shutdown
	// Create a context that can be canceled.
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Log.Info("[KAFKA CONSUMER]Signal received, shutting down...", zap.String("signal", sig.String()))
		cancel() // cancel context，notify group.Consume to exit
	}()

	// 7. start consuming
	logger.Log.Info("[KAFKA CONSUMER]Ready to process messages")

	for {
		// Consume is a blocking call.
		// It returns when:
		// A) A rebalance occurs (new member joined/left) -> Loop continues
		// B) Context is canceled -> Loop breaks
		err := group.Consume(ctx, []string{KafkaTopic}, handler)
		if err != nil {
			logger.Log.Error("[KAFKA CONSUMER]Consume loop error", zap.Error(err))
			time.Sleep(time.Second) // retry after a bit
		}
		if ctx.Err() != nil {
			break
		}
	}

	logger.Log.Info("[KAFKA CONSUMER]Consumer exited")
}
