package models

// Document represent an es document
type Document struct {
	ID        string      `json:"id"`
	Index     string      `json:"index"`
	IndexType string      `json:"type"`
	Source    interface{} `json:"source"`
}

// NewDocument Construct a new Document
func NewDocument(id string, index string, indexType string, source interface{}) *Document {
	return &Document{id, index, indexType, source}
}
