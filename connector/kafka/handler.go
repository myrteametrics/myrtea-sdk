package kafka

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// ConsumerConfig holds the configuration of a consumer group Handler. Zero
// values for the tuning/retry fields are replaced by sane defaults in
// NewHandler, so callers only need to set the connection fields.
type ConsumerConfig struct {
	Brokers      []string
	ClientID     string
	GroupID      string
	Topics       []string
	OffsetOldest bool
	Verbose      bool
	SASL         SASLConfig

	// Fetch / rebalance tuning. Defaults target a ~1 GB memory footprint.
	RebalanceTimeout       time.Duration
	BrokerMaxReadBytes     int32
	FetchMinBytes          int32
	FetchMaxWait           time.Duration
	FetchMaxPartitionBytes int32
	FetchMaxBytes          int32
	MaxConcurrentFetches   int
	PollMaxRecords         int

	// Retry / backoff behaviour for the poll loop.
	MaxRetries        int
	RetryDelay        time.Duration
	MaxRetryDelay     time.Duration
	SuccessResetDelay time.Duration

	// PingTimeout bounds the initial connectivity check. Defaults to 10s.
	PingTimeout time.Duration

	// ExtraOpts are appended last, allowing callers to override any option.
	ExtraOpts []kgo.Opt
}

func (c *ConsumerConfig) withDefaults() {
	if c.RebalanceTimeout == 0 {
		c.RebalanceTimeout = 90 * time.Second
	}
	if c.BrokerMaxReadBytes == 0 {
		c.BrokerMaxReadBytes = 128 * 1024 * 1024
	}
	if c.FetchMinBytes == 0 {
		c.FetchMinBytes = 1024 * 100
	}
	if c.FetchMaxWait == 0 {
		c.FetchMaxWait = 500 * time.Millisecond
	}
	if c.FetchMaxPartitionBytes == 0 {
		c.FetchMaxPartitionBytes = 5 * 1024 * 1024
	}
	if c.FetchMaxBytes == 0 {
		c.FetchMaxBytes = 100 * 1024 * 1024
	}
	if c.MaxConcurrentFetches == 0 {
		c.MaxConcurrentFetches = 5
	}
	if c.PollMaxRecords == 0 {
		c.PollMaxRecords = 1000
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 10
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 1 * time.Second
	}
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = 30 * time.Second
	}
	if c.SuccessResetDelay == 0 {
		c.SuccessResetDelay = 5 * time.Minute
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = 10 * time.Second
	}
}

// ConsumerConfigFromViper builds a ConsumerConfig from the standard myrtea
// connector keys, mirroring the historical sarama setup:
//
//	KAFKA_CONSUMER_VERBOSE        (bool)
//	KAFKA_CONSUMER_BROKERS        ([]string)
//	KAFKA_CONSUMER_CLIENTID       (string)
//	KAFKA_CONSUMER_GROUPID        (string)
//	KAFKA_CONSUMER_TOPICS         ([]string)
//	KAFKA_CONSUMER_OFFSET_OLDEST  (bool)
//	KAFKA_SASL_*                  (see SASLFromViper)
func ConsumerConfigFromViper() ConsumerConfig {
	return ConsumerConfig{
		Brokers:      viper.GetStringSlice("KAFKA_CONSUMER_BROKERS"),
		ClientID:     viper.GetString("KAFKA_CONSUMER_CLIENTID"),
		GroupID:      viper.GetString("KAFKA_CONSUMER_GROUPID"),
		Topics:       viper.GetStringSlice("KAFKA_CONSUMER_TOPICS"),
		OffsetOldest: viper.GetBool("KAFKA_CONSUMER_OFFSET_OLDEST"),
		Verbose:      viper.GetBool("KAFKA_CONSUMER_VERBOSE"),
		SASL:         SASLFromViper(),
	}
}

// Handler drives a franz-go consumer group: it builds the client, runs the poll
// loop with retry/backoff and dispatches records through a DefaultMultiConsumer.
type Handler struct {
	cm      ProcessorConsumerMap
	cfg     ConsumerConfig
	done    chan os.Signal
	wg      *sync.WaitGroup
	client  *kgo.Client
	started bool
}

// NewHandler creates a consumer group Handler. The done channel receives
// os.Interrupt when the consumer gives up after MaxRetries.
func NewHandler(consumerMap ProcessorConsumerMap, cfg ConsumerConfig, done chan os.Signal) *Handler {
	cfg.withDefaults()
	return &Handler{
		cm:   consumerMap,
		cfg:  cfg,
		done: done,
		wg:   &sync.WaitGroup{},
	}
}

// NewHandlerFromViper is a convenience constructor equivalent to
// NewHandler(consumerMap, ConsumerConfigFromViper(), done). It eases migration
// from connectors that previously relied on viper directly.
func NewHandlerFromViper(consumerMap ProcessorConsumerMap, done chan os.Signal) *Handler {
	return NewHandler(consumerMap, ConsumerConfigFromViper(), done)
}

// Client exposes the underlying kgo.Client (nil until Start is called).
func (h *Handler) Client() *kgo.Client {
	return h.client
}

// Start builds the client and launches the poll loop in a goroutine.
func (h *Handler) Start(ctx context.Context) {
	if h.started {
		zap.L().Warn("Kafka consumer group already started")
		return
	}
	h.started = true

	consumer := NewDefaultMultiConsumer(h.cm, NewDefaultConsumerParams(h.done))

	opts := []kgo.Opt{
		kgo.SeedBrokers(h.cfg.Brokers...),
		kgo.ClientID(h.cfg.ClientID),
		kgo.ConsumerGroup(h.cfg.GroupID),
		kgo.ConsumeTopics(h.cfg.Topics...),
		kgo.RebalanceTimeout(h.cfg.RebalanceTimeout),
		// NOTE: BrokerMaxReadBytes must always be >= FetchMaxBytes.
		kgo.BrokerMaxReadBytes(h.cfg.BrokerMaxReadBytes),
		kgo.FetchMinBytes(h.cfg.FetchMinBytes),
		kgo.FetchMaxWait(h.cfg.FetchMaxWait),
		kgo.FetchMaxPartitionBytes(h.cfg.FetchMaxPartitionBytes),
		kgo.FetchMaxBytes(h.cfg.FetchMaxBytes),
		kgo.MaxConcurrentFetches(h.cfg.MaxConcurrentFetches),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(consumer.OnPartitionsAssigned),
		kgo.OnPartitionsRevoked(consumer.OnPartitionsRevoked),
		kgo.OnPartitionsLost(consumer.OnPartitionsLost),
	}

	if h.cfg.OffsetOldest {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	opts = h.cfg.SASL.apply(opts)

	if h.cfg.Verbose {
		opts = append(opts, kgo.WithLogger(NewKgoLogger(zap.L())))
	}

	opts = append(opts, h.cfg.ExtraOpts...)

	zap.L().Info("Creating franz-go client",
		zap.String("clientID", h.cfg.ClientID),
		zap.String("groupID", h.cfg.GroupID))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		zap.L().Panic("error creating consumer group client", zap.Error(err))
	}

	h.client = client
	consumer.SetClient(client)

	pingCtx, cancel := context.WithTimeout(context.Background(), h.cfg.PingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		zap.L().Panic("failed to connect to Kafka brokers", zap.Error(err))
	}

	close(consumer.Ready)
	zap.L().Info("consumer ready")

	h.wg.Add(1)
	go h.pollLoop(ctx, client, &consumer)
}

func (h *Handler) pollLoop(ctx context.Context, client *kgo.Client, consumer *DefaultMultiConsumer) {
	defer h.wg.Done()

	retryCount := 0
	currentRetryDelay := h.cfg.RetryDelay
	lastErrorTime := time.Time{}

	for {
		if ctx.Err() != nil {
			zap.L().Info("Context cancelled, stopping consumer")
			return
		}

		// PollRecords is strongly recommended when using BlockRebalanceOnPoll.
		fetches := client.PollRecords(ctx, h.cfg.PollMaxRecords)
		if fetches.IsClientClosed() {
			zap.L().Info("Client closed, stopping consumer")
			return
		}

		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				zap.L().Info("Context cancelled during fetch, stopping consumer")
				client.AllowRebalance()
				return
			}

			lastErrorTime = time.Now()
			retryCount++
			zap.L().Error("Error from kafka consumer",
				zap.Error(err),
				zap.Int("retryCount", retryCount),
				zap.Int("maxRetries", h.cfg.MaxRetries))

			if retryCount >= h.cfg.MaxRetries {
				zap.L().Error("Max retries exceeded, shutting down consumer")
				client.AllowRebalance()
				h.done <- os.Interrupt
				return
			}

			zap.L().Info("Retrying kafka consumer",
				zap.Duration("delay", currentRetryDelay),
				zap.Int("attempt", retryCount))

			select {
			case <-time.After(currentRetryDelay):
			case <-ctx.Done():
				zap.L().Info("Context cancelled during retry wait, stopping consumer")
				client.AllowRebalance()
				return
			}

			currentRetryDelay *= 2
			if currentRetryDelay > h.cfg.MaxRetryDelay {
				currentRetryDelay = h.cfg.MaxRetryDelay
			}

			client.AllowRebalance()
			continue
		}

		// Process records per partition (preserves per-partition ordering).
		consumer.ProcessPartitions(ctx, fetches)

		// Required when using BlockRebalanceOnPoll. AutoCommitMarks commits in
		// the background.
		client.AllowRebalance()

		// Reset retry counters after a sustained period without errors.
		if retryCount > 0 && !lastErrorTime.IsZero() {
			if time.Since(lastErrorTime) >= h.cfg.SuccessResetDelay {
				zap.L().Info("Resetting retry counters after successful operation period",
					zap.Int("previousRetryCount", retryCount))
				retryCount = 0
				currentRetryDelay = h.cfg.RetryDelay
			}
		}

		if ctx.Err() != nil {
			zap.L().Info("Context cancelled after successful poll, stopping consumer")
			return
		}
	}
}

// CloseWaitFinish closes the client and waits for the poll loop to drain.
func (h *Handler) CloseWaitFinish() {
	if !h.started {
		zap.L().Warn("Kafka consumer group not started")
		return
	}
	zap.L().Info("Closing kafka consumer group")
	if h.client != nil {
		h.client.Close()
	}
	h.started = false

	h.wg.Wait()
}
