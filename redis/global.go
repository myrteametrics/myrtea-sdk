package redis

import (
	"fmt"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

var (
	_globalC = NewDefaultRedisClient()
)

// C returns a pointer the singleton instance of statsd.Client
func C() *redis.Client {
	return _globalC
}

// ReplaceGlobals replaces the global Logger and SugaredLogger, and returns a
// function to restore the original values. It's safe for concurrent use.
func ReplaceGlobals(redis *redis.Client) func() {
	prev := _globalC
	_globalC = redis
	return func() { ReplaceGlobals(prev) }
}

// NewDefaultRedisClient returns a pointer to a new redis.Client instance with default configuration
func NewDefaultRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := client.Ping().Result()
	if err != nil {
		zap.L().Error("Redis NewClient()", zap.Error(err))
	}
	zap.L().Info("Default Redis client initialized")
	return client
}

// NewRedisClient returns a pointer to a new redis.Client instance
func NewRedisClient(url string, port int, password string, db int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", url, port),
		Password: password,
		DB:       db,
	})
	_, err := client.Ping().Result()
	if err != nil {
		zap.L().Error("Redis NewClient()", zap.Error(err))
	}
	zap.L().Info("Redis client initialized")
	return client, nil
}
