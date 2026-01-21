package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Redis Key Templates (Centralized management to avoid typos)
const (
	KeyRoomPubSub = "room:%s:pubsub" // Channel for Redis Pub/Sub
	KeyRoomLikes  = "room:%s:likes"  // Counter for likes
)

func InitRedisClient() *redis.Client {
	// Initialize Redis client.
	// Ensure Redis is running on localhost:6379 via Docker.
	// Lazy loading: only create Redis client when first time trying to contact
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rdb
}

// PublishToRoom sends a message to a specific room's Redis channel.
func PublishToRoom(rdb *redis.Client, roomID string, payload []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	channel := fmt.Sprintf(KeyRoomPubSub, roomID)

	err := rdb.Publish(ctx, channel, payload).Err()
	if err != nil {
		logger.Log.Error("[REDIS INFRA] Publish failed",
			zap.String("room", roomID),
			zap.Error(err),
		)
	}
}

// IncrRoomLikes increments the like counter for a specific room.
func IncrRoomLikes(rdb *redis.Client, roomID string, count uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key := fmt.Sprintf(KeyRoomLikes, roomID)

	// Use IncrBy to support batch likes (CmdLike.Count)
	err := rdb.IncrBy(ctx, key, int64(count)).Err()
	if err != nil {
		logger.Log.Error("[REDIS INFRA] Increment likes failed",
			zap.String("room", roomID),
			zap.Error(err),
		)
	}
}
