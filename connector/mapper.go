package connector

type Mapper interface {
	FilterDocument(msg KafkaMessage) (bool, string)
	MapToDocument(msg KafkaMessage) (FilteredJsonMessage, error)
}
