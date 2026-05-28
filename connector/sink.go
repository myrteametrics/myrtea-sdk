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
	// AppendOnly disables any document lookup (mget) before indexing.
	// When true, documents are inserted directly without merging with existing ones.
	// This is ideal for append-only workloads such as logs. Defaults to false.
	AppendOnly bool `json:"appendOnly"`
}

// Document represent an es document
type Document struct {
	ID        string                 `json:"id"`
	Index     string                 `json:"index"`
	IndexType string                 `json:"type"`
	Source    map[string]interface{} `json:"source"`
}
