package connector

type Sink interface {
	AddMessageToQueue(message Message)
}

type BulkIngestRequest struct {
	UUID         string     `json:"uuid"`
	DocumentType string     `json:"documentType"`
	MergeConfig  []Config   `json:"merge"`
	Docs         []Document `json:"docs"`
}

// Document represent an es document
type Document struct {
	ID        string      `json:"id"`
	Index     string      `json:"index"`
	IndexType string      `json:"type"`
	Source    interface{} `json:"source"`
}
