package connector

import "context"

type Sink interface {
	Start(context.Context)
	Stop()
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
	ID        string                 `json:"id"`
	Index     string                 `json:"index"`
	IndexType string                 `json:"type"`
	Source    map[string]interface{} `json:"source"`
}
