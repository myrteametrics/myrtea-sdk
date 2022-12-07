package connector

// Processor is a general interface to processor message
type Processor interface {
	Process(msg Message) ([]Message, error)
}

type Lookup func(path string, value string, index string) ([]string, error)
