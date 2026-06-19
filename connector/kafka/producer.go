package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// ProducerConfig configures a generic Kafka Producer. Zero values are replaced
// by sane defaults in NewProducer, so callers typically only set Brokers and
// ClientID.
type ProducerConfig struct {
	ClientID string
	Brokers  []string
	SASL     SASLConfig

	// RequiredAcks controls the durability guarantee. Defaults to
	// kgo.AllISRAcks() (wait for all in-sync replicas).
	RequiredAcks *kgo.Acks
	// Linger batches records for up to this duration. Defaults to 500ms.
	Linger time.Duration
	// Compression is the batch compression preference list. Defaults to LZ4.
	Compression []kgo.CompressionCodec
	// PingTimeout bounds the initial connectivity check. Defaults to 10s.
	PingTimeout time.Duration
	// DisablePing skips the connectivity check at creation time.
	DisablePing bool

	// ExtraOpts are appended last, allowing callers to override any option
	// (e.g. kgo.RecordPartitioner, kgo.ProducerBatchMaxBytes, ...).
	ExtraOpts []kgo.Opt
}

// Producer is a thin, reusable wrapper around a franz-go client dedicated to
// producing records. It is safe for concurrent use.
type Producer struct {
	client *kgo.Client
}

// NewProducer creates a Producer from the given configuration.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if cfg.Linger == 0 {
		cfg.Linger = 500 * time.Millisecond
	}
	if cfg.Compression == nil {
		cfg.Compression = []kgo.CompressionCodec{kgo.Lz4Compression()}
	}
	if cfg.PingTimeout == 0 {
		cfg.PingTimeout = 10 * time.Second
	}
	acks := kgo.AllISRAcks()
	if cfg.RequiredAcks != nil {
		acks = *cfg.RequiredAcks
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(acks),
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchCompression(cfg.Compression...),
	}

	opts = cfg.SASL.apply(opts)
	opts = append(opts, cfg.ExtraOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if !cfg.DisablePing {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
		defer cancel()
		if err := client.Ping(ctx); err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to connect to Kafka brokers: %w", err)
		}
	}

	return &Producer{client: client}, nil
}

// NewProducerFromViper is a drop-in constructor matching the historical
// connector signature: it reads KAFKA_SASL_* from viper and applies the default
// tuning.
func NewProducerFromViper(clientID string, brokers []string) (*Producer, error) {
	return NewProducer(ProducerConfig{
		ClientID: clientID,
		Brokers:  brokers,
		SASL:     SASLFromViper(),
	})
}

// Client exposes the underlying kgo.Client for advanced use cases.
func (p *Producer) Client() *kgo.Client {
	return p.client
}

// Produce asynchronously produces a record, invoking promise on completion. It
// is the lowest-level primitive; Send and SendSync are usually more convenient.
func (p *Producer) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	p.client.Produce(ctx, record, promise)
}

// Send asynchronously produces a single key/value record to topic. Delivery
// errors are logged via zap. Use SendSync when you need to wait for the result.
func (p *Producer) Send(topic string, key, value []byte) error {
	record := &kgo.Record{Topic: topic, Key: key, Value: value}

	p.client.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			zap.L().Error("Failed to produce message",
				zap.Error(err),
				zap.String("topic", r.Topic),
				zap.Int32("partition", r.Partition))
			return
		}
		zap.L().Debug("Successfully produced message",
			zap.String("topic", r.Topic),
			zap.Int32("partition", r.Partition),
			zap.Int64("offset", r.Offset))
	})

	return nil
}

// SendSync synchronously produces one or more records and returns the first
// error encountered, if any.
func (p *Producer) SendSync(ctx context.Context, records ...*kgo.Record) error {
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

// SendAvro is kept for backward compatibility with the historical connector
// producer.
//
// Deprecated: use Send, which is identical. Avro encoding (schema registry wire
// format) is the caller's responsibility and unrelated to transport.
func (p *Producer) SendAvro(topic string, key, value []byte) error {
	return p.Send(topic, key, value)
}

// Close flushes any buffered records and closes the underlying client.
func (p *Producer) Close() {
	zap.L().Info("Closing producer...")
	p.client.Flush(context.Background())
	p.client.Close()
	zap.L().Info("Successfully closed producer")
}
