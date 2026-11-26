package redis

import (
	"time"

	"github.com/redis/rueidis"

	"go.uber.org/zap"
)

// DefaultShardsRefreshInterval is the default interval of the periodic cluster topology scan
const DefaultShardsRefreshInterval = 30 * time.Second

var _globalC rueidis.Client

// C returns a pointer the singleton instance of rueidis.Client
func C() rueidis.Client {
	return _globalC
}

// ReplaceGlobals replaces the global Logger and SugaredLogger, and returns a
// function to restore the original values. It's safe for concurrent use.
func ReplaceGlobals(redis rueidis.Client) func() {
	prev := _globalC
	_globalC = redis
	return func() { ReplaceGlobals(prev) }
}

// Options configures the Redis client
type Options struct {
	URLs     []string
	Password string
	// Resp3 must be false when using redis-proxy
	Resp3 bool
	// Cache enables client side caching, only available with RESP3
	Cache bool
	// ShardsRefreshInterval is the interval of the periodic cluster topology scan, 0 disables it.
	// rueidis already refreshes the topology on redirections and connection errors, the periodic
	// scan is a safeguard for a node replaced while the client saw no error.
	ShardsRefreshInterval time.Duration
	// ConnWriteTimeout bounds the wait for a response on a connection, 0 keeps the rueidis default (10s)
	ConnWriteTimeout time.Duration
}

// NewRedisClient returns a pointer to a new redis.Client instance, scanning the cluster topology
// every DefaultShardsRefreshInterval
func NewRedisClient(urls []string, password string, resp3, cache bool) (rueidis.Client, error) {
	return NewRedisClientWithOptions(Options{
		URLs:                  urls,
		Password:              password,
		Resp3:                 resp3,
		Cache:                 cache,
		ShardsRefreshInterval: DefaultShardsRefreshInterval,
	})
}

// NewRedisClientWithOptions returns a new rueidis.Client built from the options
func NewRedisClientWithOptions(opts Options) (rueidis.Client, error) {
	client, err := rueidis.NewClient(clientOption(opts))
	if err != nil {
		zap.L().Error("Redis NewClient()", zap.Error(err))
		return nil, err
	}
	zap.L().Info("Redis client initialized",
		zap.Duration("shardsRefreshInterval", opts.ShardsRefreshInterval),
		zap.Duration("connWriteTimeout", opts.ConnWriteTimeout))
	return client, nil
}

func clientOption(opts Options) rueidis.ClientOption {
	return rueidis.ClientOption{
		InitAddress:      opts.URLs,
		Password:         opts.Password,
		AlwaysRESP2:      !opts.Resp3,                // must be true when using redis-proxy, otherwise it will not work (local)
		DisableCache:     !opts.Resp3 || !opts.Cache, // cannot work with resp2
		ShuffleInit:      true,
		ConnWriteTimeout: max(opts.ConnWriteTimeout, 0),
		ClusterOption: rueidis.ClusterOption{
			// rueidis rejects a negative interval
			ShardsRefreshInterval: max(opts.ShardsRefreshInterval, 0),
		},
	}
}
