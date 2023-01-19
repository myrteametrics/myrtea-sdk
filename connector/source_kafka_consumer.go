package connector

import "github.com/Shopify/sarama"

type ConsumerProcessor interface {
	Process(message *sarama.ConsumerMessage)
}

// DefaultConsumer represents a Sarama consumer group consumer
type DefaultConsumer struct {
	ready     chan bool
	processor ConsumerProcessor
}

func NewDefaultConsumer(processor ConsumerProcessor) DefaultConsumer {
	return DefaultConsumer{
		ready:     make(chan bool),
		processor: processor,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *DefaultConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *DefaultConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *DefaultConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			consumer.processor.Process(message)
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// DefaultMultiConsumer represents a Sarama consumer group consumer
type DefaultMultiConsumer struct {
	ready      chan bool
	processors map[string]ConsumerProcessor
}

func NewDefaultMultiConsumer(processors map[string]ConsumerProcessor) DefaultMultiConsumer {
	return DefaultMultiConsumer{
		ready:      make(chan bool),
		processors: processors,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *DefaultMultiConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *DefaultMultiConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *DefaultMultiConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			consumer.processors[message.Topic].Process(message)
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
