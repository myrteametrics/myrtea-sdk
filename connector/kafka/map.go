package kafka

// ProcessorConsumerMap maps a topic name to the ConsumerProcessor responsible
// for handling its records.
type ProcessorConsumerMap map[string]ConsumerProcessor
