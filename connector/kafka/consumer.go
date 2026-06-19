package kafka

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const DefaultMaxPermittedPanics = 10

// ConsumerProcessor processes a single Kafka record.
//
// It is the franz-go counterpart of connector.ConsumerProcessor (which operates
// on *sarama.ConsumerMessage).
type ConsumerProcessor interface {
	Process(*kgo.Record)
}

// ConsumerParams tunes the behaviour of the consumers.
type ConsumerParams struct {
	MaxPermittedPanics int
	Done               *chan os.Signal
}

// NewDefaultConsumerParams returns ConsumerParams with sane defaults wired to
// the given done channel.
func NewDefaultConsumerParams(done chan os.Signal) ConsumerParams {
	return ConsumerParams{
		MaxPermittedPanics: DefaultMaxPermittedPanics,
		Done:               &done,
	}
}

// topicPartition represents a unique topic-partition pair.
type topicPartition struct {
	topic     string
	partition int32
}

// partitionConsumer handles consuming from a single partition in its own
// goroutine, preserving per-partition ordering.
type partitionConsumer struct {
	cl         *kgo.Client
	processor  ConsumerProcessor
	topic      string
	partition  int32
	quit       chan struct{}
	done       chan struct{}
	recs       chan kgo.FetchTopicPartition
	params     ConsumerParams
	panicCount *atomic.Int64 // shared across all partition consumers
}

func (pc *partitionConsumer) consume() {
	defer close(pc.done)

	zap.L().Info("Starting partition consumer",
		zap.String("topic", pc.topic),
		zap.Int32("partition", pc.partition))

	defer zap.L().Info("Stopped partition consumer",
		zap.String("topic", pc.topic),
		zap.Int32("partition", pc.partition))

	for {
		select {
		case <-pc.quit:
			return
		case p := <-pc.recs:
			for _, record := range p.Records {
				pc.process(record)
			}
			// Mark all records for commit after processing; AutoCommitMarks
			// handles the actual commit. A record whose Process panicked is
			// recovered (see process) and still marked, so the partition is not
			// blocked by a single poison message.
			pc.cl.MarkCommitRecords(p.Records...)
		}
	}
}

// process runs the processor for a single record, recovering from panics so a
// faulty message cannot crash the whole connector. Recovery is bounded by
// ConsumerParams.MaxPermittedPanics (see handlePanic).
func (pc *partitionConsumer) process(record *kgo.Record) {
	defer func() {
		if r := recover(); r != nil {
			handlePanic(r, pc.params, pc.panicCount)
		}
	}()
	pc.processor.Process(record)
}

// DefaultMultiConsumer is a franz-go consumer group handler that dispatches
// records to a per-topic ConsumerProcessor, running one goroutine per assigned
// partition. It is the franz-go counterpart of connector.DefaultMultiConsumer.
type DefaultMultiConsumer struct {
	Ready      chan bool
	processors map[string]ConsumerProcessor // topic -> processor
	consumers  map[topicPartition]*partitionConsumer
	cl         *kgo.Client
	panicCount *atomic.Int64 // shared recovered-panic counter across partitions
	ConsumerParams
}

// NewDefaultMultiConsumer creates a DefaultMultiConsumer for the given topic to
// processor mapping.
func NewDefaultMultiConsumer(processors map[string]ConsumerProcessor, params ConsumerParams) DefaultMultiConsumer {
	if params.MaxPermittedPanics < 0 {
		params.MaxPermittedPanics = DefaultMaxPermittedPanics
	}

	return DefaultMultiConsumer{
		Ready:          make(chan bool),
		processors:     processors,
		consumers:      make(map[topicPartition]*partitionConsumer),
		panicCount:     &atomic.Int64{},
		ConsumerParams: params,
	}
}

// SetClient sets the kgo.Client (called after client creation).
func (consumer *DefaultMultiConsumer) SetClient(cl *kgo.Client) {
	consumer.cl = cl
}

// OnPartitionsAssigned starts a partition consumer for each newly assigned
// partition. Wire it to kgo.OnPartitionsAssigned.
func (consumer *DefaultMultiConsumer) OnPartitionsAssigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		processor, found := consumer.processors[topic]
		if !found {
			zap.L().Warn("No processor found for assigned topic", zap.String("topic", topic))
			continue
		}

		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}

			pc := &partitionConsumer{
				cl:         cl,
				processor:  processor,
				topic:      topic,
				partition:  partition,
				quit:       make(chan struct{}),
				done:       make(chan struct{}),
				recs:       make(chan kgo.FetchTopicPartition, 10),
				params:     consumer.ConsumerParams,
				panicCount: consumer.panicCount,
			}

			consumer.consumers[tp] = pc
			go pc.consume()

			zap.L().Info("Assigned partition",
				zap.String("topic", topic),
				zap.Int32("partition", partition))
		}
	}
}

// OnPartitionsRevoked stops the relevant partition consumers and commits the
// marked offsets before the rebalance proceeds. Wire it to
// kgo.OnPartitionsRevoked.
func (consumer *DefaultMultiConsumer) OnPartitionsRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	zap.L().Info("Partitions revoked", zap.Any("partitions", revoked))

	consumer.killConsumers(revoked)

	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		zap.L().Error("Failed to commit marked offsets on revoke", zap.Error(err))
	} else {
		zap.L().Info("Successfully committed marked offsets on revoke")
	}
}

// OnPartitionsLost stops the relevant partition consumers without committing
// (an error happened). Wire it to kgo.OnPartitionsLost.
func (consumer *DefaultMultiConsumer) OnPartitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	zap.L().Info("Partitions lost", zap.Any("partitions", lost))

	consumer.killConsumers(lost)

	zap.L().Warn("Partitions lost, cannot commit offsets")
}

func (consumer *DefaultMultiConsumer) killConsumers(partitions map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitionList := range partitions {
		for _, partition := range partitionList {
			tp := topicPartition{topic: topic, partition: partition}
			pc, exists := consumer.consumers[tp]
			if !exists {
				continue
			}

			delete(consumer.consumers, tp)
			close(pc.quit)

			zap.L().Info("Waiting for partition consumer to finish",
				zap.String("topic", topic),
				zap.Int32("partition", partition))

			wg.Add(1)
			go func(done chan struct{}) {
				<-done
				wg.Done()
			}(pc.done)
		}
	}
}

// ProcessPartitions dispatches fetched partitions to their per-partition
// goroutines. Call it from the poll loop (see Handler) while using
// BlockRebalanceOnPoll.
func (consumer *DefaultMultiConsumer) ProcessPartitions(_ context.Context, fetches kgo.Fetches) {
	fetches.EachError(func(t string, p int32, err error) {
		zap.L().Error("Fetch error", zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
	})

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		tp := topicPartition{topic: p.Topic, partition: p.Partition}

		// With BlockRebalanceOnPoll, the partition consumer is guaranteed to
		// exist: OnPartitionsAssigned runs before fetches for new partitions,
		// and OnPartitionsRevoked waits for consumers to quit before re-allowing
		// polling.
		pc, exists := consumer.consumers[tp]
		if !exists {
			zap.L().Warn("No consumer found for partition",
				zap.String("topic", p.Topic),
				zap.Int32("partition", p.Partition))
			return
		}

		pc.recs <- p
	})
}

// SingleProcessorMap builds a ProcessorConsumerMap that routes every topic to
// the same processor. It eases migration from connector.DefaultConsumer (which
// used a single processor for all consumed topics).
func SingleProcessorMap(processor ConsumerProcessor, topics ...string) ProcessorConsumerMap {
	cm := make(ProcessorConsumerMap, len(topics))
	for _, topic := range topics {
		cm[topic] = processor
	}
	return cm
}

// handlePanic records a recovered processing panic and decides whether the
// connector should keep running. While the number of recovered panics stays at
// or below MaxPermittedPanics, the panic is logged and processing continues. As
// soon as that budget is exceeded, it signals shutdown via the Done channel (or
// calls zap.Fatal if no Done channel was provided).
//
// The counter is shared across all partition consumers, so a process flapping
// across many partitions still trips the global budget.
func handlePanic(reason any, params ConsumerParams, panicCount *atomic.Int64) {
	count := int(panicCount.Add(1))

	if count > params.MaxPermittedPanics {
		if params.Done != nil {
			zap.L().Error("Kafka consumer panic (maxPermittedPanics reached)",
				zap.Int("maxPermittedPanics", params.MaxPermittedPanics),
				zap.Int("panicCount", count),
				zap.Any("reason", reason),
				zap.Stack("stack"))
			*params.Done <- os.Interrupt
			return
		}
		zap.L().Fatal("Kafka consumer panic (maxPermittedPanics reached)",
			zap.Int("maxPermittedPanics", params.MaxPermittedPanics),
			zap.Int("panicCount", count),
			zap.Any("reason", reason),
			zap.Stack("stack"))
		return
	}

	zap.L().Error("Kafka consumer recovered from panic",
		zap.Int("panicCount", count),
		zap.Int("maxPermittedPanics", params.MaxPermittedPanics),
		zap.Any("reason", reason),
		zap.Stack("stack"))
}
