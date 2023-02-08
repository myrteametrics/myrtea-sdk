package connector

type Mapper interface {
	FilterDocument(msg Message) (bool, string)
	MapToDocument(msg Message) (Message, error)
}
