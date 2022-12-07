package connector

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
