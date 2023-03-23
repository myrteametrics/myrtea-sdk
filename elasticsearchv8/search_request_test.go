package elasticsearchv8

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v4/engine"
)

func TestBuildAgg(t *testing.T) {
	f1 := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Avg,
			Term:     "myintentterm",
		},
		Dimensions: []*engine.DimensionFragment{
			{
				Name:     "mydim1",
				Operator: engine.By,
				Term:     "mydimterm1",
				Size:     10,
			}, {
				Name:     "mydim2",
				Operator: engine.Histogram,
				Term:     "mydimterm2",
				Interval: 100,
			}, {
				Name:         "mydim3",
				Operator:     engine.DateHistogram,
				Term:         "mydimterm3",
				DateInterval: "1h",
			},
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{Operator: engine.For, Field: "myfield", Value: "myvalue"},
				&engine.BooleanFragment{
					Operator: engine.Not,
					Fragments: []engine.ConditionFragment{
						&engine.LeafConditionFragment{Operator: engine.Exists, Field: "myfield"},
					},
				},
				&engine.BooleanFragment{
					Operator: engine.Or,
					Fragments: []engine.ConditionFragment{
						&engine.LeafConditionFragment{Operator: engine.Between, Field: "myfield", Value: 10, Value2: 1000},
						&engine.LeafConditionFragment{Operator: engine.From, Field: "myfield", Value: 10},
						&engine.LeafConditionFragment{Operator: engine.To, Field: "myfield", Value: 10},
						&engine.LeafConditionFragment{Operator: engine.Between, Field: "myfield", Value: "myvalue", Value2: "myvalue"},
						&engine.LeafConditionFragment{Operator: engine.From, Field: "myfield", Value: "myvalue"},
						&engine.LeafConditionFragment{Operator: engine.To, Field: "myfield", Value: "myvalue"},
					},
				},
			},
		},
	}

	b, err := json.Marshal(f1)
	if err != nil {
		t.Error(err)
	}
	var f engine.Fact
	err = json.Unmarshal(b, &f)
	if err != nil {
		t.Error(err)
	}

	query, err := buildElasticFilter(f.Condition, make(map[string]interface{}))
	if err != nil {
		t.Error(err)
	}
	b, _ = json.MarshalIndent(query, "", " ")
	t.Log(string(b))
	t.Fail()

	name, agg, err := buildElasticAgg(f.Intent)
	if err != nil {
		t.Error(err)
	}
	b, _ = json.MarshalIndent(agg, "", " ")
	t.Log(string(b))
	t.Fail()

	name2, agg2, err := buildElasticBucket(name, agg, f.Dimensions)
	if err != nil {
		t.Error(err)
	}
	b, _ = json.MarshalIndent(agg2, "", " ")
	t.Log(name2, string(b))
	t.Fail()

	search := buildSearchRequest(query, map[string]types.Aggregations{name2: agg2})
	b, _ = json.MarshalIndent(search, "", " ")
	t.Log(string(b))
	t.Fail()

	search2, err := ConvertFactToSearchRequestV8(f, time.Now(), make(map[string]string))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(search2, "", " ")
	t.Log(string(b))
	t.Fail()
}
