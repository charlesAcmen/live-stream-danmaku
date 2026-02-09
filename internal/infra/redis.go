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
	KeyRoomOnline = "room:%s:online" //Counter for online users
	RedisCancelTimeout = 2 * time.Second
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
func IncrRoomLikes(rdb *redis.Client, roomID string, count uint64) {
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

// GetRoomStats fetches Online count and Likes count in one go.
// This is used by the periodic broadcastStats in Manager.
func GetRoomStats(rdb *redis.Client, roomID string) (uint64, uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	onlineKey := fmt.Sprintf(KeyRoomOnline, roomID)
	likesKey := fmt.Sprintf(KeyRoomLikes, roomID)

	// Use Pipeline (Batch Request)
	// Instead of: Request -> Wait -> Response -> Request -> Wait -> Response
	// We do: Request + Request -> Wait -> Response + Response
	pipe := rdb.Pipeline()

	// Queue commands
	onlineCmd := pipe.Get(ctx, onlineKey)
	likesCmd := pipe.Get(ctx, likesKey)
	// 4. Execute Batch
	_, err := pipe.Exec(ctx)

	// Error Handling:
	// If redis returns redis.Nil (key not found), it means count is 0.
	// We only care about connection errors.
	if err != nil && err != redis.Nil {
		logger.Log.Warn("[REDIS INFRA] Failed to fetch stats pipeline",
			zap.String("room", roomID),
			zap.Error(err),
		)
		// Even if error occurs, we try to return 0s
	}

	// 5. Extract Values
	// .Uint64() automatically returns 0 if the key doesn't exist or error occurred.
	onlineVal, _ := onlineCmd.Uint64()
	likesVal, _ := likesCmd.Uint64()

	return onlineVal, likesVal
}
