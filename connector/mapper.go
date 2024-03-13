package connector

type Mapper interface {
	// FilterDocument checks if document is filtered or not, returns if documents valid and if invalid,
	// the following reason. It is using the given filters.
	FilterDocument(msg Message) (bool, string)

	// MapToDocument Maps data to document
	MapToDocument(msg Message) (Message, error)

	// DecodeDocument is a function that just decodes a document and returns it
	// You can use it if you want to decode a message only "once" and not in each
	// individual function
	DecodeDocument(msg Message) (Message, error)
}
