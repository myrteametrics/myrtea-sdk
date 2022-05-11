package builder

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/olivere/elastic"
)

// Query is an interface for all parts of an elasticsearch query clause
type Query interface {
	Source() elastic.Query
	Contextualize(key string, value string)
}

func unMarshallQueries(queriesJSON []*json.RawMessage) ([]Query, error) {
	var queries []Query
	for _, raw := range queriesJSON {
		var m map[string]interface{}
		err := json.Unmarshal(*raw, &m)
		if err != nil {
			fmt.Println("ERROR ", err)
			return nil, err
		}

		switch m["type"] {
		case "term":
			var t TermQuery
			err := json.Unmarshal(*raw, &t)
			if err != nil {
				return nil, err
			}
			queries = append(queries, &t)

		case "range":
			var r RangeQuery
			err := json.Unmarshal(*raw, &r)
			if err != nil {
				return nil, err
			}
			queries = append(queries, &r)

		case "bool":
			var bq BoolQuery
			err := json.Unmarshal(*raw, &bq)
			if err != nil {
				return nil, err
			}
			queries = append(queries, &bq)

		case "exists":
			var eq ExistsQuery
			err := json.Unmarshal(*raw, &eq)
			if err != nil {
				return nil, err
			}
			queries = append(queries, &eq)

		case "script":
			var sq ScriptQuery
			err := json.Unmarshal(*raw, &sq)
			if err != nil {
				return nil, err
			}
			queries = append(queries, &sq)
		}
	}
	return queries, nil
}

// BoolQuery represents an elasticsearch bool query clause
type BoolQuery struct {
	Type    string  `json:"type"`
	Filter  []Query `json:"filter,omitempty"`
	Must    []Query `json:"must,omitempty"`
	Should  []Query `json:"should,omitempty"`
	MustNot []Query `json:"mustNot,omitempty"`
}

// Source convert the query to a elasticsearch query interface
func (q *BoolQuery) Source() elastic.Query {
	var boolQuery *elastic.BoolQuery
	boolQuery = elastic.NewBoolQuery()

	var filterQueries []elastic.Query
	for _, q := range q.Filter {
		filterQueries = append(filterQueries, q.Source())
	}
	boolQuery.Filter(filterQueries...)

	var mustQueries []elastic.Query
	for _, q := range q.Must {
		mustQueries = append(mustQueries, q.Source())
	}
	boolQuery.Must(mustQueries...)

	var mustNotQueries []elastic.Query
	for _, q := range q.MustNot {
		mustNotQueries = append(mustNotQueries, q.Source())
	}
	boolQuery.MustNot(mustNotQueries...)

	var shouldQueries []elastic.Query
	for _, q := range q.Should {
		shouldQueries = append(shouldQueries, q.Source())
	}
	boolQuery.Should(shouldQueries...)

	return boolQuery
}

// Contextualize contextualize the current query filter with key-value system
func (q *BoolQuery) Contextualize(key string, value string) {}

// UnmarshalJSON unmarshal a bool query interface content
func (q *BoolQuery) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	var ty string
	err = json.Unmarshal(*objMap["type"], &ty)
	if err != nil {
		return err
	}
	q.Type = ty

	if _, ok := objMap["filter"]; ok {
		var rawMustQueriesMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["filter"], &rawMustQueriesMessage)
		if err != nil {
			return err
		}
		filterQueries, err := unMarshallQueries(rawMustQueriesMessage)
		if err != nil {
			return err
		}
		q.Filter = filterQueries
	}
	if _, ok := objMap["must"]; ok {
		var rawMustQueriesMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["must"], &rawMustQueriesMessage)
		if err != nil {
			return err
		}
		mustQueries, err := unMarshallQueries(rawMustQueriesMessage)
		if err != nil {
			return err
		}
		q.Must = mustQueries
	}
	if _, ok := objMap["must_not"]; ok {
		var rawMustNotQueriesMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["must_not"], &rawMustNotQueriesMessage)
		if err != nil {
			return err
		}
		mustNotQueries, err := unMarshallQueries(rawMustNotQueriesMessage)
		if err != nil {
			return err
		}
		q.MustNot = mustNotQueries
	}
	if _, ok := objMap["should"]; ok {
		var rawShouldQueriesMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["should"], &rawShouldQueriesMessage)
		if err != nil {
			return err
		}
		shouldQueries, err := unMarshallQueries(rawShouldQueriesMessage)
		if err != nil {
			return err
		}
		q.Should = shouldQueries
	}
	return nil
}

// ExistsQuery represents an elasticsearch exists query clause
type ExistsQuery struct {
	Type  string `json:"type"`
	Field string `json:"field"`
}

// Source convert the query to a elasticsearch query interface
func (q *ExistsQuery) Source() elastic.Query {
	return elastic.NewExistsQuery(q.Field)
}

// Contextualize contextualize the current query filter with key-value system
func (q *ExistsQuery) Contextualize(key string, value string) {}

// TermQuery represents an elasticsearch term query clause
type TermQuery struct {
	Type  string      `json:"type"`
	Field string      `json:"field"`
	Value interface{} `json:"value"`
}

// Source convert the query to a elasticsearch query interface
func (q *TermQuery) Source() elastic.Query {
	if reflect.TypeOf(q.Value).Kind() == reflect.Slice {
		return elastic.NewTermsQuery(q.Field, q.Value.([]interface{})...)
	}
	return elastic.NewTermQuery(q.Field, q.Value)
}

// Contextualize contextualize the current query filter with key-value system
func (q *TermQuery) Contextualize(key string, value string) {}

// RangeQuery represents an elasticsearch range query clause
type RangeQuery struct {
	Type        string      `json:"type"`
	Field       string      `json:"field"`
	From        interface{} `json:"from,omitempty"`
	IncludeFrom bool        `json:"includeFrom"`
	To          interface{} `json:"to,omitempty"`
	IncludeTo   bool        `json:"includeTo"`
	TimeZone    string      `json:"timezone,omitempty"`
}

// Source convert the query to a elasticsearch query interface
func (q *RangeQuery) Source() elastic.Query {
	var rangeQuery *elastic.RangeQuery
	rangeQuery = elastic.NewRangeQuery(q.Field)
	if q.From != nil {
		rangeQuery.From(q.From)
		rangeQuery.IncludeLower(q.IncludeFrom)
	}
	if q.To != nil {
		rangeQuery.To(q.To)
		rangeQuery.IncludeUpper(q.IncludeTo)
	}
	if q.TimeZone != "" {
		rangeQuery.TimeZone(q.TimeZone)
	}
	return rangeQuery
}

// Contextualize contextualize the current query filter with key-value system
func (q *RangeQuery) Contextualize(key string, value string) {}

// ScriptQuery represents an elasticsearch script query clause
type ScriptQuery struct {
	Type   string `json:"type"`
	Script string `json:"script"`
}

// Source convert the query to a elasticsearch query interface
func (q *ScriptQuery) Source() elastic.Query {
	return elastic.NewScriptQuery(elastic.NewScriptInline(q.Script))
}

// Contextualize contextualize the current query filter with key-value system
func (q *ScriptQuery) Contextualize(key string, value string) {}
