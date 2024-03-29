package builder

import (
	"encoding/json"
	"testing"
)

func Test_NewEsQuery(t *testing.T) {
	termQuery := &TermQuery{"term", "in-channel", "kifkif"}
	//RangeQuery := &RangeQuery{"range", "in-timestamp", "begin", nil}
	b := []Query{termQuery}
	boolQuery := &BoolQuery{Type: "bool", Filter: b, Must: nil, Should: nil, MustNot: nil}
	esSearch := &EsSearch{Indices: []string{"index-1"}, Size: 10, Offset: 0, Order: true, Query: boolQuery, Aggs: nil}
	es, _ := json.Marshal(esSearch)
	var esS *EsSearch
	err := json.Unmarshal(es, &esS)
	if err != nil {
		t.Error(err)
	}
	t.Log(string(es))
}
func Test_NewEsQuery2(t *testing.T) {
	termQuery := &TermQuery{Type: "term", Field: "in-channel", Value: "kifkif"}
	boolQuery := &BoolQuery{Type: "bool", Filter: []Query{termQuery}, Must: nil, Should: nil, MustNot: nil}

	cardAgg := &CardinalityAgg{Type: "cardinality", Name: "cardinality", Field: "projet", Script: false}
	termAgg := &TermAgg{Type: "term", Name: "term-agg", Field: "program", Size: 100, SubAggs: []Aggregation{cardAgg}}

	esSearch := &EsSearch{Indices: []string{"index-1"}, Size: 10, Offset: 0, Order: true, Query: boolQuery, Aggs: []Aggregation{termAgg}}
	t.Log("before", esSearch)
	es, _ := json.Marshal(esSearch)
	var esS *EsSearch
	err := json.Unmarshal(es, &esS)
	if err != nil {
		t.Error(err)
	}
	t.Log("after", esS)
}

func Test_NewEsQueryRegexp(t *testing.T) {
	RegexpQuery := &RegexpQuery{"regexp", "in-channel", "kifkif"}
	b := []Query{RegexpQuery}
	boolQuery := &BoolQuery{Type: "bool", Filter: b, Must: nil, Should: nil, MustNot: nil}
	esSearch := &EsSearch{Indices: []string{"index-1"}, Size: 10, Offset: 0, Order: true, Query: boolQuery, Aggs: nil}
	es, _ := json.Marshal(esSearch)
	var esS *EsSearch
	err := json.Unmarshal(es, &esS)

	if err != nil {
		t.Error(err)
	}
	t.Log(string(es))
}

func Test_NewEsQueryOptionalRegexp(t *testing.T) {
	RegexpQuery := &RegexpQuery{"optionalregexp", "in-channel", "k*k"}
	b := []Query{RegexpQuery}
	boolQuery := &BoolQuery{Type: "bool", Filter: b, Must: nil, Should: nil, MustNot: nil}
	esSearch := &EsSearch{Indices: []string{"index-1"}, Size: 10, Offset: 0, Order: true, Query: boolQuery, Aggs: nil}
	es, _ := json.Marshal(esSearch)
	var esS *EsSearch
	err := json.Unmarshal(es, &esS)

	t.Log(string(es))
	
	if err != nil {
		t.Error(err)
	}
	
}

func Test_NewEsQueryWildCard(t *testing.T) {
	WildcardQuery := &WildcardQuery{"wildcard", "parcel_number", "dz*fr"}
	b := []Query{WildcardQuery}
	boolQuery := &BoolQuery{Type: "bool", Filter: b, Must: nil, Should: nil, MustNot: nil}
	esSearch := &EsSearch{Indices: []string{"index-1"}, Size: 10, Offset: 0, Order: true, Query: boolQuery, Aggs: nil}
	es, _ := json.Marshal(esSearch)
	var esS *EsSearch
	err := json.Unmarshal(es, &esS)

	if err != nil {
		t.Error(err)
	}
	t.Log(string(es))
}

func Test_NewEsQueryOptionalWildCard(t *testing.T) {
	WildcardQuery := &WildcardQuery{"optionalwildcard", "parcel_number", "dz*fr"}
	b := []Query{WildcardQuery}
	boolQuery := &BoolQuery{Type: "bool", Filter: b, Must: nil, Should: nil, MustNot: nil}
	esSearch := &EsSearch{Indices: []string{"index-1"}, Size: 10, Offset: 0, Order: true, Query: boolQuery, Aggs: nil}
	es, _ := json.Marshal(esSearch)
	var esS *EsSearch
	err := json.Unmarshal(es, &esS)

	if err != nil {
		t.Error(err)
	}
	t.Log(string(es))
}

