package elasticsearch

type BulkIndexMeta struct {
	Index BulkIndexMetaDetail `json:"index"`
}

type BulkIndexMetaDetail struct {
	S_Index string `json:"_index"`
	S_Id    string `json:"_id"`
}
