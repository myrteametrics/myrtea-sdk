package builder

import (
	"encoding/json"
)

// EsQuery is related directly to one KPI
type EsQuery struct {
	Alias      string    `json:"alias,omitempty"`
	Evaluation *Calendar `json:"evaluation,omitempty"`
	ID         int64     `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	Search     *EsSearch `json:"search"`
}

// NewEsQuery build and returns a pointer to a new EsQuery
func NewEsQuery(alias string, id int64, search *EsSearch, evaluation *Calendar, name string) *EsQuery {
	return &EsQuery{
		Alias:      alias,
		Evaluation: evaluation,
		ID:         id,
		Name:       name,
		Search:     search,
	}
}

// EsSearch represents a search definition
type EsSearch struct {
	Indices []string      `json:"indices"`
	Size    int           `json:"size"`
	Offset  int           `json:"offset"`
	Order   bool          `json:"order"`
	Query   Query         `json:"query,omitempty"`
	Aggs    []Aggregation `json:"aggs,omitempty"`
	Source  string        `json:"source,omitempty"`
}

// UnmarshalJSON unmarshal a json byte slice in an EsSearch object
func (es *EsSearch) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	var indices []string
	err = json.Unmarshal(*objMap["indices"], &indices)
	if err != nil {
		return err
	}
	es.Indices = indices

	var size int
	err = json.Unmarshal(*objMap["size"], &size)
	if err != nil {
		return err
	}
	es.Size = size

	var order bool
	err = json.Unmarshal(*objMap["order"], &order)
	if err != nil {
		return err
	}
	es.Order = order

	if _, ok := objMap["aggs"]; ok {
		queryRawMessage := objMap["aggs"]
		var arrayQueryRawMessage []*json.RawMessage
		err = json.Unmarshal(*queryRawMessage, &arrayQueryRawMessage)
		if err != nil {
			return err
		}

		aggs, err := unMarshallAggregations(arrayQueryRawMessage)
		if err != nil {
			return err
		}
		es.Aggs = aggs
	}
	if _, ok := objMap["query"]; ok {
		queryRawMessage := objMap["query"]
		var m map[string]interface{}
		err = json.Unmarshal(*queryRawMessage, &m)
		if err != nil {
			return err
		}

		switch m["type"] {
		case "term":
			var t TermQuery
			err := json.Unmarshal(*queryRawMessage, &t)
			if err != nil {
				return err
			}
			es.Query = &t

		case "range":
			var r RangeQuery
			err := json.Unmarshal(*queryRawMessage, &r)
			if err != nil {
				return err
			}
			es.Query = &r

		case "bool":
			var bq BoolQuery
			err := json.Unmarshal(*queryRawMessage, &bq)
			if err != nil {
				return err
			}
			es.Query = &bq

		case "exists":
			var eq ExistsQuery
			err := json.Unmarshal(*queryRawMessage, &eq)
			if err != nil {
				return err
			}
			es.Query = &eq

		case "script":
			var sq ScriptQuery
			err := json.Unmarshal(*queryRawMessage, &sq)
			if err != nil {
				return err
			}
			es.Query = &sq
		}
	}
	return nil
}

/*Calendar constains the thresholds for a scope*/
type Calendar struct {
	Scope      string       `json:"scope"`
	Thresholds []Thresholds `json:"thresholds"`
}

//ThresholdContent type value.
type ThresholdContent struct {
	ID   int         `json:"id,omitempty"`
	From interface{} `json:"from"`
	To   interface{} `json:"to"`
	Name string      `json:"name"`
}

//ThresholdState type value.
// type ThresholdState struct {
// 	ID   int               `json:"id,omitempty"`
// 	From interface{}       `json:"from"`
// 	To   interface{}       `json:"to"`
// 	Name *rootcauses.State `json:"state"`
// }

// Threshold type.
// @Deprecated
type Threshold struct {
	From string             `json:"from"`
	To   string             `json:"to"`
	Val  []ThresholdContent `json:"value"`
}

// Thresholds is a struct representing a KPI threshold
// @Deprecated
type Thresholds struct {
	Label string    `json:"label,omitempty"`
	Value Threshold `json:"value"`
}
