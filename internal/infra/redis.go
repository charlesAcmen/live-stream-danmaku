package infra

import "github.com/redis/go-redis/v9"

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
