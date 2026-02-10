package infra

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Redis Key Templates (Centralized management to avoid typos)
const (
	// Ensure Redis is running on localhost:6379 via Docker.
	RedisAddr           = "127.0.0.1:6379"
	KeyRoomPubSub       = "room:%s:pubsub"    // Channel for Redis Pub/Sub
	KeyRoomLikes        = "room:%s:likes"     // Counter for likes
	KeyRoomOnline       = "room:%s:online:*"  // Counter for online users of a room
	KeyRoomServerOnline = "room:%s:online:%s" // Counter for online users of a server
	RedisCancelTimeout  = 2 * time.Second
)

func InitRedisClient() *redis.Client {
	// Initialize Redis client.
	// Lazy loading: only create Redis client when first time trying to contact
	rdb := redis.NewClient(&redis.Options{
		Addr:     RedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rdb
}

// PublishToRoom sends a message to a specific room's Redis channel.
func PublishToRoom(rdb *redis.Client, roomID string, payload []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
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
	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
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

// UpdateOnlineCount modifies the online counter (delta can be 1 or -1).
// func UpdateOnlineCount(rdb *redis.Client, roomID string, delta int64) {
// 	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
// 	defer cancel()

// 	key := fmt.Sprintf(KeyRoomOnline, roomID)

// 	var err error
// 	if delta > 0 {
// 		err = rdb.Incr(ctx, key).Err()
// 	} else {
// 		err = rdb.Decr(ctx, key).Err()
// 	}

//		if err != nil {
//			logger.Log.Error("[REDIS INFRA] Failed to update online count",
//				zap.String("room", roomID),
//				zap.Int64("delta", delta),
//				zap.Error(err),
//			)
//		}
//	}
//
// UpdateServerOnline sets the absolute count for a specific server instance with a short TTL.
// This acts as a heartbeat. If the server crashes, this key will expire automatically.
func UpdateServerOnline(rdb *redis.Client, roomID string, serverID string, count int) {
	key := fmt.Sprintf(KeyRoomServerOnline, roomID, serverID)

	// SETEX: Set value and Expiration together (Atomic)
	// We set TTL to 5 seconds. Since broadcastStats runs every 3 seconds,
	// this key will be kept alive as long as the server is running.
	err := rdb.Set(context.Background(), key, count, 5*time.Second).Err()
	if err != nil {
		logger.Log.Error("[REDIS] Failed to update server online count",
			zap.String("room", roomID),
			zap.Error(err))
	}
}

// GetTotalOnline aggregates online counts from all active servers for a room.
func GetTotalOnline(rdb *redis.Client, roomID string) int64 {
	ctx := context.Background()
	// Match pattern: room:1001:online:*
	// This finds keys from all servers currently reporting for this room.
	pattern := fmt.Sprintf(KeyRoomOnline, roomID)

	// 1. Find all keys matching the pattern
	// In production with millions of keys, ensure your Redis structure prevents scanning too much.
	// Since we scan specific room prefix, it only returns keys equal to Server Count (e.g., 2-10), which is fast.
	keys, err := rdb.Keys(ctx, pattern).Result()
	if err != nil {
		logger.Log.Error("[REDIS] Failed to scan online keys", zap.Error(err))
		return 0
	}

	if len(keys) == 0 {
		return 0
	}

	// 2. Get values for all these keys
	// MGet is more efficient than looping Get
	values, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		logger.Log.Error("[REDIS] Failed to mget online values", zap.Error(err))
		return 0
	}

	// 3. Sum them up
	var total int64
	for _, val := range values {
		if val == nil {
			continue
		}
		// Redis returns values as string (or int depending on adapter), usually string in Go-Redis
		if s, ok := val.(string); ok {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				total += i
			}
		}
	}

	return total
}

// GetRoomStats fetches Online count and Likes count in one go.
// This is used by the periodic broadcastStats in Manager.
// func GetRoomStats(rdb *redis.Client, roomID string) (uint64, uint64) {
// 	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
// 	defer cancel()
// 	onlineKey := fmt.Sprintf(KeyRoomOnline, roomID)
// 	likesKey := fmt.Sprintf(KeyRoomLikes, roomID)

// 	// Use Pipeline (Batch Request)
// 	// Instead of: Request -> Wait -> Response -> Request -> Wait -> Response
// 	// We do: Request + Request -> Wait -> Response + Response
// 	pipe := rdb.Pipeline()

// 	// Queue commands
// 	onlineCmd := pipe.Get(ctx, onlineKey)
// 	likesCmd := pipe.Get(ctx, likesKey)
// 	// 4. Execute Batch
// 	_, err := pipe.Exec(ctx)

// 	// Error Handling:
// 	// If redis returns redis.Nil (key not found), it means count is 0.
// 	// We only care about connection errors.
// 	if err != nil && err != redis.Nil {
// 		logger.Log.Warn("[REDIS INFRA] Failed to fetch stats pipeline",
// 			zap.String("room", roomID),
// 			zap.Error(err),
// 		)
// 		// Even if error occurs, we try to return 0s
// 	}

// 	// 5. Extract Values
// 	// .Uint64() automatically returns 0 if the key doesn't exist or error occurred.
// 	onlineVal, _ := onlineCmd.Uint64()
// 	likesVal, _ := likesCmd.Uint64()

// 	return onlineVal, likesVal
// }
