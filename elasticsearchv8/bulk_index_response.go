package elasticsearchv8

type BulkIndexResponseItem struct {
	Status int `json:"status,omitempty"`
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
