package elasticsearch

import (
	"encoding/json"
	"testing"
	"time"

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
	// t.Log(string(b))
	// t.Fail()

	name, agg, err := buildElasticAgg(f.Intent)
	if err != nil {
		t.Error(err)
	}
	b, _ = json.MarshalIndent(agg, "", " ")
	// t.Log(string(b))
	// t.Fail()

	name2, agg2, err := buildElasticBucket(name, agg, f.Dimensions)
	if err != nil {
		t.Error(err)
	}
	_ = name2
	b, _ = json.MarshalIndent(agg2, "", " ")
	// t.Log(name2, string(b))
	// t.Fail()

	search2, err := ConvertFactToSearchRequestV8(f, time.Now(), make(map[string]string))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(search2, "", " ")
	// t.Log(string(b))
	// t.Fail()
}

func TestBuildSelect(t *testing.T) {
	f1 := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Select,
			Term:     "myintentterm",
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{Operator: engine.For, Field: "myfield", Value: "myvalue"},
			},
		},
	}

	f2 := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Select,
			Term:     "myintentterm",
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{Operator: engine.For, Field: "myfield", Value: []string{"myvalue", "mayvale2", "myvalue3"}},
			},
		},
	}

	f3 := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Select,
			Term:     "myintentterm",
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{Operator: engine.For, Field: "myfield", Value: []string{"myvalue"}},
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
	// t.Log(string(b))
	// t.Fail()

	search2, err := ConvertFactToSearchRequestV8(f, time.Now(), make(map[string]string))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(search2, "", " ")
	// t.Log(string(b))
	// t.Fail()

	b2, err := json.Marshal(f2)
	if err != nil {
		t.Error(err)
	}
	var ff engine.Fact
	err = json.Unmarshal(b2, &ff)
	if err != nil {
		t.Error(err)
	}

	query, err = buildElasticFilter(ff.Condition, make(map[string]interface{}))
	if err != nil {
		t.Error(err)
	}
	b2, _ = json.MarshalIndent(query, "", " ")
	// t.Log(string(b))
	// t.Fail()

	search2, err = ConvertFactToSearchRequestV8(ff, time.Now(), make(map[string]string))
	if err != nil {
		t.Error(err)
	}

	b2, _ = json.MarshalIndent(search2, "", " ")
	// t.Log(string(b2))
	// t.Fail()

	b3, err := json.Marshal(f3)
	if err != nil {
		t.Error(err)
	}
	var fff engine.Fact
	err = json.Unmarshal(b3, &fff)
	if err != nil {
		t.Error(err)
	}

	query3, err := buildElasticFilter(fff.Condition, make(map[string]interface{}))
	if err != nil {
		t.Error(err)
	}
	b, _ = json.MarshalIndent(query3, "", " ")
	// t.Log(string(b))
	// t.Fail()

	search3, err := ConvertFactToSearchRequestV8(fff, time.Now(), make(map[string]string))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(search3, "", " ")
	t.Log(string(b))
	// t.Fail()
}

func TestBuildElasticFilterWithRegexp(t *testing.T) {
	fact := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Select,
			Term:     "myintentterm",
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{
					Operator: engine.Regexp,
					Field:    "monChamp",
					Value:    "ma.*expression",
				},
			},
		},
	}

	b, err := json.Marshal(fact)
	if err != nil {
		t.Error(err)
	}

	var deserializedFact engine.Fact
	err = json.Unmarshal(b, &deserializedFact)
	if err != nil {
		t.Error(err)
	}

	query, err := buildElasticFilter(deserializedFact.Condition, make(map[string]interface{}))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(query, "", " ")
	//t.Log(string(b))

	mustQueries := query.Bool.Must
	if len(mustQueries) == 0 {
		t.Errorf("No must queries found")
		return
	}

	found := false
	for _, q := range mustQueries {
		if regexpQuery, ok := q.Regexp["monChamp"]; ok {
			found = true
			if regexpQuery.Value != "ma.*expression" {
				t.Errorf("Expected regexp value 'ma.*expression', got '%s'", regexpQuery.Value)
			}
			break
		}
	}

	if !found {
		t.Errorf("Regexp query for 'monChamp' not found")
	}
}

func TestBuildElasticFilterWithOptionalRegexp(t *testing.T) {
	fact := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Select,
			Term:     "myintentterm",
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{
					Operator: engine.OptionalRegexp,
					Field:    "monChamp",
					Value:    "ma.*expression",
				},
			},
		},
	}

	b, err := json.Marshal(fact)
	if err != nil {
		t.Error(err)
	}

	var deserializedFact engine.Fact
	err = json.Unmarshal(b, &deserializedFact)
	if err != nil {
		t.Error(err)
	}

	query, err := buildElasticFilter(deserializedFact.Condition, make(map[string]interface{}))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(query, "", " ")
	//t.Log(string(b))

	mustQueries := query.Bool.Must
	if len(mustQueries) == 0 {
		t.Errorf("No must queries found")
		return
	}

	found := false
	for _, q := range mustQueries {
		if regexpQuery, ok := q.Regexp["monChamp"]; ok {
			found = true
			if regexpQuery.Value != "ma.*expression" {
				t.Errorf("Expected regexp value 'ma.*expression', got '%s'", regexpQuery.Value)
			}
			break
		}
	}

	if !found {
		t.Errorf("Regexp query for 'monChamp' not found")
	}
}
func TestBuildElasticFilterWithWildCard(t *testing.T) {
	fact := engine.Fact{
		ID:   1,
		Name: "test",
		Intent: &engine.IntentFragment{
			Name:     "myintent",
			Operator: engine.Select,
			Term:     "myintentterm",
		},
		Condition: &engine.BooleanFragment{
			Operator: engine.And,
			Fragments: []engine.ConditionFragment{
				&engine.LeafConditionFragment{
					Operator: engine.OptionalWildcard,
					Field:    "monChamp",
					Value:    "",
				},
				&engine.LeafConditionFragment{
					Operator: engine.Wildcard,
					Field:    "monChamp",
					Value:    "ma.*expression",
				},
			},
		},
	}

	b, err := json.Marshal(fact)
	if err != nil {
		t.Error(err)
	}

	var deserializedFact engine.Fact
	err = json.Unmarshal(b, &deserializedFact)
	if err != nil {
		t.Error(err)
	}

	query, err := buildElasticFilter(deserializedFact.Condition, make(map[string]interface{}))
	if err != nil {
		t.Error(err)
	}

	b, _ = json.MarshalIndent(query, "", " ")
	t.Log(string(b))

	mustQueries := query.Bool.Must
	if len(mustQueries) == 0 {
		t.Errorf("No must queries found")
		return
	}

	found := false
	for _, q := range mustQueries {
		if wildcardQuery, ok := q.Wildcard["monChamp"]; ok {
			found = true
			if *wildcardQuery.Value != "ma.*expression" {
				t.Errorf("Expected Wildcard value 'ma.*expression', got '%s'", *wildcardQuery.Value)
			}
			break
		}
	}

	if !found {
		t.Errorf("Wildcard query for 'monChamp' not found")
	}
}
