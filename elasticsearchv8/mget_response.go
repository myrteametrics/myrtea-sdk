package elasticsearchv8

type MGetResponse struct {
	Docs []MGetResponseItem `json:"docs"`
}

type MGetResponseItem struct {
	ID     string                 `json:"_id"`
	Index  string                 `json:"_index"`
	Source map[string]interface{} `json:"_source"`
	Found  bool                   `json:"found"`
	Error  map[string]interface{} `json:"error"`
}
