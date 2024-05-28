package elasticsearch

// BulkIndexResponseItem represents the Elasticsearch response item.
type BulkIndexResponseItem struct {
	Index      string `json:"_index"`
	DocumentID string `json:"_id"`
	Version    int64  `json:"_version"`
	Result     string `json:"result"`
	Status     int    `json:"status"`
	SeqNo      int64  `json:"_seq_no"`
	PrimTerm   int64  `json:"_primary_term"`

	Shards struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`

	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Cause  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"caused_by"`
	} `json:"error,omitempty"`
}

type BulkIndexResponse struct {
	Took   int                                 `json:"took,omitempty"`
	Errors bool                                `json:"errors,omitempty"`
	Items  []map[string]*BulkIndexResponseItem `json:"items,omitempty"`
}

// Failed returns those items of a bulkIndex response that have errors,
// i.e. those that don't have a status code between 200 and 299.
func (r *BulkIndexResponse) Failed() []*BulkIndexResponseItem {
	if r.Items == nil {
		return nil
	}
	var errors []*BulkIndexResponseItem
	for _, item := range r.Items {
		for _, result := range item {
			if !(result.Status >= 200 && result.Status <= 299) {
				errors = append(errors, result)
			}
		}
	}
	return errors
}
