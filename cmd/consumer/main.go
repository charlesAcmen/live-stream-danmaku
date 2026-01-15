package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/model"

	"github.com/IBM/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	KafkaTopic = "danmu_save_topic"
	BatchSize  = 100             // threshold for per write in db
	BatchTime  = 1 * time.Second // period for trying to write in db
)

// 1. Define the Handler structure
type DanmakuConsumerGroupHandler struct {
	db     *gorm.DB
	buffer []*model.DanmakuMessage
	mu     sync.Mutex // Protect buffer, though usually one routine per claim
}

func main() {
	// 1. init db
	dsn := "root:root@tcp(127.0.0.1:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("[KAFKA CONSUMER]Failed to connect to database:", err)
	}

	// 2. init Kafka Consumer
	config := sarama.NewConfig()
	//errors in Kafka will be write in consumer.Errors() channel
	config.Consumer.Return.Errors = true
	// connect to Kafka cluster based on yaml
	// only one server/broker（经纪人）
	brokers := []string{"127.0.0.1:9092"}
	//maybe more：[]string{"host1:9092", "host2:9092"}
	//the number of consumer should be less than that of partitions for high usage
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatal("[KAFKA CONSUMER]Kafka Consumer new Error:", err)
	}
	defer consumer.Close()

	// 3. all partitions that sub to the topic
	partitionList, err := consumer.Partitions(KafkaTopic)
	if err != nil {
		log.Fatal("[KAFKA CONSUMER]Failed to get partitions:", err)
	} else {
		log.Println("[KAFKA CONSUMER]🚀 Consumer started! Waiting for messages...")
	}

	// 4. start consumer go routine
	// sarama.OffsetNewest: Only receive messages that arrive AFTER this program starts.
	// sarama.OffsetOldest: Read all history messages available in Kafka.
	partitionConsumer, err := consumer.ConsumePartition(KafkaTopic, partitionList[0], sarama.OffsetNewest)
	if err != nil {
		log.Fatal("[KAFKA CONSUMER]Failed to start partition consumer:", err)
	}
	defer partitionConsumer.Close()

	// 5. buffer
	var buffer []*model.DanmakuMessage
	ticker := time.NewTicker(BatchTime)

	sigChan := make(chan os.Signal, 1)
	// syscall.SIGINT: Triggered by Ctrl+C in terminal.
	// syscall.SIGTERM: Triggered by Docker stop or Kubernetes kill commands.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// func to flush in all data in buffer towards db
	flushDB := func() {
		if len(buffer) == 0 {
			return
		}
		// GORM insert in batches
		// split buffer in groups of records with 100 each
		// GORM will insert groups in one shot automatically
		err := db.CreateInBatches(buffer, 100).Error
		if err != nil {
			log.Println("[KAFKA CONSUMER]❌ Insert DB Failed:", err)
		} else {
			log.Printf("[KAFKA CONSUMER]✅ Saved %d messages to MySQL", len(buffer))
		}
		// clear buffer
		buffer = buffer[:0]
	}

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var danmu model.DanmakuMessage
			err := json.Unmarshal(msg.Value, &danmu)
			if err == nil {
				buffer = append(buffer, &danmu)
				if len(buffer) >= BatchSize {
					flushDB()
				}
			}

		case <-ticker.C:
			flushDB()

		case <-sigChan:
			log.Println("[KAFKA CONSUMER]Shutting down consumer...")
			flushDB()
			return
		}
	}
}
