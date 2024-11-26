package connector

import (
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"os"
)

const DefaultMaxPermittedPanics = 10

type ConsumerProcessor interface {
	Process(message *sarama.ConsumerMessage)
}

type ConsumerParams struct {
	MaxPermittedPanics int
	Done               *chan os.Signal
}

// DefaultConsumer represents a Sarama consumer group consumer
type DefaultConsumer struct {
	Ready      chan bool
	processor  ConsumerProcessor
	panicCount int
	ConsumerParams
}

func NewDefaultConsumer(processor ConsumerProcessor, params ConsumerParams) DefaultConsumer {
	if params.MaxPermittedPanics < 0 {
		params.MaxPermittedPanics = DefaultMaxPermittedPanics
	}

	return DefaultConsumer{
		Ready:          make(chan bool),
		processor:      processor,
		panicCount:     0,
		ConsumerParams: params,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *DefaultConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as Ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *DefaultConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *DefaultConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	defer func() {
		if r := recover(); r != nil {
			handlePanic(r, consumer.ConsumerParams, &consumer.panicCount)
		}
	}()

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				zap.L().Warn("Message channel was closed")
				return nil
			}
			consumer.processor.Process(message)
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// DefaultMultiConsumer represents a Sarama consumer group consumer
type DefaultMultiConsumer struct {
	Ready      chan bool
	processors map[string]ConsumerProcessor
	panicCount int
	ConsumerParams
}

func NewDefaultMultiConsumer(processors map[string]ConsumerProcessor, params ConsumerParams) DefaultMultiConsumer {
	if params.MaxPermittedPanics < 0 {
		params.MaxPermittedPanics = DefaultMaxPermittedPanics
	}

	return DefaultMultiConsumer{
		Ready:          make(chan bool),
		processors:     processors,
		panicCount:     0,
		ConsumerParams: params,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *DefaultMultiConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as Ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *DefaultMultiConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *DefaultMultiConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	defer func() {
		if r := recover(); r != nil {
			handlePanic(r, consumer.ConsumerParams, &consumer.panicCount)
		}
	}()

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				zap.L().Warn("Message channel was closed")
				return nil
			}
			if processor, found := consumer.processors[message.Topic]; found {
				processor.Process(message)
				session.MarkMessage(message, "")
			} else {
				zap.L().Warn("Processor not found for topic", zap.String("topic", message.Topic))
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func handlePanic(reason any, params ConsumerParams, panicCount *int) {
	if *panicCount >= params.MaxPermittedPanics {
		// Send done signal if given
		if params.Done != nil {
			zap.L().Error("Kafka consumer panic (maxPermittedPanics reached)",
				zap.Int("maxPermittedPanics", params.MaxPermittedPanics), zap.Any("reason", reason))

			*params.Done <- os.Interrupt
		} else {
			zap.L().Fatal("Kafka consumer panic (maxPermittedPanics reached)",
				zap.Int("maxPermittedPanics", params.MaxPermittedPanics), zap.Any("reason", reason))
		}
		return
	}

	*panicCount++
	zap.L().Warn("Kafka consumer recovered from panic", zap.Any("reason", reason))
}
