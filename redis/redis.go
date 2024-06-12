package redis

import (
	"github.com/redis/rueidis"

	"go.uber.org/zap"
)

var _globalC *rueidis.Client

// C returns a pointer the singleton instance of rueidis.Client
func C() *rueidis.Client {
	return _globalC
}

// ReplaceGlobals replaces the global Logger and SugaredLogger, and returns a
// function to restore the original values. It's safe for concurrent use.
func ReplaceGlobals(redis *rueidis.Client) func() {
	prev := _globalC
	_globalC = redis
	return func() { ReplaceGlobals(prev) }
}

// NewRedisClient returns a pointer to a new redis.Client instance
func NewRedisClient(urls []string, password string, resp3, cache bool) (rueidis.Client, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  urls,
		Password:     password,
		AlwaysRESP2:  !resp3,           // must be true when using redis-proxy, otherwise it will not work (local)
		DisableCache: !resp3 || !cache, // cannot work with resp2
		ShuffleInit:  true,
	})
	if err != nil {
		zap.L().Error("Redis NewClient()", zap.Error(err))
		return nil, err
	}
	zap.L().Info("Redis client initialized")
	return client, nil
}
