package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
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

	// 3. configure Kafka Consumer Group
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// OffsetOldest: 如果是新的消费者组，从最早的消息开始读（防止启动前积压的消息丢失）
	// OffsetNewest: 只要最新的（可能会丢历史）
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := []string{"127.0.0.1:9092"}

	// create consumer group
	group, err := sarama.NewConsumerGroup(brokers, KafkaGroupID, config)
	if err != nil {
		logger.Log.Fatal("[KAFKA CONSUMER]Failed to create consumer group", zap.Error(err))
	}
	defer group.Close()

	// 4. listen from Kafka errors
	go func() {
		for err := range group.Errors() {
			logger.Log.Error("[KAFKA CONSUMER]Kafka consumer error", zap.Error(err))
		}
	}()

	// 5. prepare Context and Handler
	ctx, cancel := context.WithCancel(context.Background())
	messageRepo := repo.NewMessageRepo(db)
	handler := consumer.NewDanmakuHandler(messageRepo, BatchSize)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Log.Info("[KAFKA CONSUMER]Received signal, shutting down...", zap.String("signal", sig.String()))
		cancel() // cancel context，notify group.Consume to exit
	}()

	// 7. start consuming
	logger.Log.Info("[KAFKA CONSUMER]Consumer started, waiting for messages...")

	for {
		// Consume will be blocked till error or ctx being canceleds
		// This is a BLOCKING call.
		// It will not return until:
		// 1. A rebalance happens (e.g., a new consumer joins/leaves).
		// 2. The context is canceled (server shutdown).
		// 3. A serious error occurs.
		err := group.Consume(ctx, []string{KafkaTopic}, handler)
		if err != nil {
			logger.Log.Error("[KAFKA CONSUMER]Error in consumer loop", zap.Error(err))
			time.Sleep(time.Second) // retry after a bit
		}
		if ctx.Err() != nil {
			break
		}
	}

	logger.Log.Info("[KAFKA CONSUMER]Consumer exited")
}
