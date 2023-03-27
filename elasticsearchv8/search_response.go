package elasticsearchv8

import (
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type SearchResponse struct {
	Aggregations    map[string]interface{}     `json:"aggregations,omitempty"`
	Clusters_       *types.ClusterStatistics   `json:"_clusters,omitempty"`
	Fields          map[string]json.RawMessage `json:"fields,omitempty"`
	Hits            types.HitsMetadata         `json:"hits"`
	MaxScore        float64                    `json:"max_score,omitempty"`
	NumReducePhases *int64                     `json:"num_reduce_phases,omitempty"`
	PitId           *string                    `json:"pit_id,omitempty"`
	Profile         *types.Profile             `json:"profile,omitempty"`
	ScrollId_       *string                    `json:"_scroll_id,omitempty"`
	Shards_         types.ShardStatistics      `json:"_shards"`
	Suggest         map[string][]types.Suggest `json:"suggest,omitempty"`
	TerminatedEarly *bool                      `json:"terminated_early,omitempty"`
	TimedOut        bool                       `json:"timed_out"`
	Took            int64                      `json:"took"`
	// MaxScore        *types.Float64             `json:"max_score,omitempty"`
	// Aggregations    map[string]types.Aggregation     `json:"aggregations,omitempty"`

}
