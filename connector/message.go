package connector

import "time"

// Message ...
type Message interface {
	String() string
}

// KafkaMessage ...
type KafkaMessage struct {
	Data []byte
}

func (kMessage KafkaMessage) String() string {
	return string(kMessage.Data)
}

// GetData Getter for the message data
func (kMessage KafkaMessage) GetData() []byte {
	return kMessage.Data
}

// DecodedKafkaMessage holds a json decoded version of the kafka message
// It can be used to avoid decoding data multiple time (which is very consuming)
type DecodedKafkaMessage struct {
	Data map[string]interface{}
}

func (msg DecodedKafkaMessage) String() string { return "" }

// FilteredJsonMessage output once we've filtered the myrtea fields from the kafka messages
type FilteredJsonMessage struct {
	Data map[string]interface{}
}

func (msg FilteredJsonMessage) String() string {
	return ""
}

// MessageWithOptions output once we've filtered the myrtea fields from the kafka messages
type MessageWithOptions struct {
	Data    map[string]interface{}
	Options map[string]interface{}
}

func (msg MessageWithOptions) String() string {
	return ""
}

type TypedDataMessage struct {
	Strings map[string]string
	Ints    map[string]int64
	Bools   map[string]bool
	Times   map[string]time.Time
}

func (m TypedDataMessage) String() string {
	return ""
}
