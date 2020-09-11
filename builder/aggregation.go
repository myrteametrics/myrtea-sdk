package builder

import (
	"encoding/json"

	"github.com/olivere/elastic"
)

// Aggregation is an interface for every type of elasticsearch aggregation
type Aggregation interface {
	GetName() string
	AggSource() elastic.Aggregation
}

func unMarshallAggregations(queriesJSON []*json.RawMessage) ([]Aggregation, error) {
	var aggregations []Aggregation
	for _, raw := range queriesJSON {
		var m map[string]interface{}
		err := json.Unmarshal(*raw, &m)
		if err != nil {
			return nil, err
		}
		switch {
		case m["type"] == "term":
			var a TermAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)

		case m["type"] == "histogram":
			var a HistogramAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)

		case m["type"] == "datehistogram":
			var a DateHistogramAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)

		case m["type"] == "cardinality":
			var a CardinalityAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)

		case m["type"] == "sum":
			var a SumAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)

		case m["type"] == "min":
			var a MinAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)

		case m["type"] == "max":
			var a MaxAgg
			err := json.Unmarshal(*raw, &a)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, &a)
		}
	}
	return aggregations, nil
}

// SourceAgg represent a source aggregation definition
type SourceAgg struct {
	Aggregation
}

// TermAgg represent a term aggregation definition
type TermAgg struct {
	Type    string        `json:"type"`
	Name    string        `json:"name"`
	Field   string        `json:"field"`
	Size    int           `json:"size"`
	SubAggs []Aggregation `json:"aggs"`
}

// GetName returns the agg name
func (agg *TermAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *TermAgg) AggSource() elastic.Aggregation {
	esAgg := elastic.NewTermsAggregation().Field(agg.Field).Size(agg.Size)
	for _, subAggs := range agg.SubAggs {
		esAgg.SubAggregation(subAggs.GetName(), subAggs.AggSource())
	}
	return esAgg
}

// UnmarshalJSON unmarshal a json byte slice in a TermAgg
func (agg *TermAgg) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	var _type string
	err = json.Unmarshal(*objMap["type"], &_type)
	if err != nil {
		return err
	}
	agg.Type = _type

	var name string
	err = json.Unmarshal(*objMap["name"], &name)
	if err != nil {
		return err
	}
	agg.Name = name

	var field string
	err = json.Unmarshal(*objMap["field"], &field)
	if err != nil {
		return err
	}
	agg.Field = field

	if _, ok := objMap["size"]; ok {
		var size int
		err = json.Unmarshal(*objMap["size"], &size)
		if err != nil {
			return err
		}
		agg.Size = size
	}
	if _, ok := objMap["aggs"]; ok {
		var rawAggregationsMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["aggs"], &rawAggregationsMessage)
		if err != nil {
			return err
		}
		subAggs, err := unMarshallAggregations(rawAggregationsMessage)
		if err != nil {
			return err
		}
		agg.SubAggs = subAggs
	}
	return nil
}

// HistogramAgg represent an histogram aggregation definition
type HistogramAgg struct {
	Type     string        `json:"type"`
	Name     string        `json:"name"`
	Field    string        `json:"field"`
	Interval float64       `json:"interval"`
	SubAggs  []Aggregation `json:"aggs"`
}

// GetName returns the agg name
func (agg *HistogramAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *HistogramAgg) AggSource() elastic.Aggregation {
	esAgg := elastic.NewHistogramAggregation().Field(agg.Field).Interval(agg.Interval)
	for _, subAggs := range agg.SubAggs {
		esAgg.SubAggregation(subAggs.GetName(), subAggs.AggSource())
	}
	return esAgg
}

// UnmarshalJSON unmarshal a json byte slice in a HistogramAgg
func (agg *HistogramAgg) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	var _type string
	err = json.Unmarshal(*objMap["type"], &_type)
	if err != nil {
		return err
	}
	agg.Type = _type

	var name string
	err = json.Unmarshal(*objMap["name"], &name)
	if err != nil {
		return err
	}
	agg.Name = name

	var field string
	err = json.Unmarshal(*objMap["field"], &field)
	if err != nil {
		return err
	}
	agg.Field = field

	if _, ok := objMap["interval"]; ok {
		var interval float64
		err = json.Unmarshal(*objMap["interval"], &interval)
		if err != nil {
			return err
		}
		agg.Interval = interval
	}
	if _, ok := objMap["aggs"]; ok {
		var rawAggregationsMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["aggs"], &rawAggregationsMessage)
		if err != nil {
			return err
		}
		subAggs, err := unMarshallAggregations(rawAggregationsMessage)
		if err != nil {
			return err
		}
		agg.SubAggs = subAggs
	}
	return nil
}

// DateHistogramAgg represent a datehistogram aggregation definition
type DateHistogramAgg struct {
	Type     string        `json:"type"`
	Name     string        `json:"name"`
	Field    string        `json:"field"`
	Interval string        `json:"interval"`
	SubAggs  []Aggregation `json:"aggs"`
	TimeZone string        `json:"timezone,omitempty"`
}

// GetName returns the agg name
func (agg *DateHistogramAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *DateHistogramAgg) AggSource() elastic.Aggregation {
	esAgg := elastic.NewDateHistogramAggregation().Field(agg.Field).Interval(agg.Interval)
	for _, subAggs := range agg.SubAggs {
		esAgg.SubAggregation(subAggs.GetName(), subAggs.AggSource())
	}
	if agg.TimeZone != "" {
		esAgg.TimeZone(agg.TimeZone)
	}
	return esAgg
}

// UnmarshalJSON unmarshal a json byte slice in a DateHistogramAgg
func (agg *DateHistogramAgg) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	var _type string
	err = json.Unmarshal(*objMap["type"], &_type)
	if err != nil {
		return err
	}
	agg.Type = _type

	var name string
	err = json.Unmarshal(*objMap["name"], &name)
	if err != nil {
		return err
	}
	agg.Name = name

	var field string
	err = json.Unmarshal(*objMap["field"], &field)
	if err != nil {
		return err
	}
	agg.Field = field

	if _, ok := objMap["interval"]; ok {
		var interval string
		err = json.Unmarshal(*objMap["interval"], &interval)
		if err != nil {
			return err
		}
		agg.Interval = interval
	}
	if _, ok := objMap["aggs"]; ok {
		var rawAggregationsMessage []*json.RawMessage
		err = json.Unmarshal(*objMap["aggs"], &rawAggregationsMessage)
		if err != nil {
			return err
		}
		subAggs, err := unMarshallAggregations(rawAggregationsMessage)
		if err != nil {
			return err
		}
		agg.SubAggs = subAggs
	}
	return nil
}

// CardinalityAgg represent a cardinality aggregation definition
type CardinalityAgg struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Field  string `json:"field"`
	Script bool   `json:"script"`
}

// GetName returns the agg name
func (agg *CardinalityAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *CardinalityAgg) AggSource() elastic.Aggregation {
	if agg.Script {
		return elastic.NewCardinalityAggregation().Script(elastic.NewScriptInline(agg.Field))
	}
	return elastic.NewCardinalityAggregation().Field(agg.Field)
}

// AvgAgg represent an average aggregation definition
type AvgAgg struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Field  string `json:"field"`
	Script bool   `json:"script"`
}

// GetName returns the agg name
func (agg *AvgAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *AvgAgg) AggSource() elastic.Aggregation {
	if agg.Script {
		return elastic.NewAvgAggregation().Script(elastic.NewScriptInline(agg.Field))
	}
	return elastic.NewAvgAggregation().Field(agg.Field)
}

// SumAgg represent a sum aggregation definition
type SumAgg struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Field  string `json:"field"`
	Script bool   `json:"script"`
}

// GetName returns the agg name
func (agg *SumAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *SumAgg) AggSource() elastic.Aggregation {
	if agg.Script {
		return elastic.NewSumAggregation().Script(elastic.NewScriptInline(agg.Field))
	}
	return elastic.NewSumAggregation().Field(agg.Field)
}

// MinAgg represent a min aggregation definition
type MinAgg struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Field  string `json:"field"`
	Script bool   `json:"script"`
}

// GetName returns the agg name
func (agg *MinAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *MinAgg) AggSource() elastic.Aggregation {
	if agg.Script {
		return elastic.NewMinAggregation().Script(elastic.NewScriptInline(agg.Field))
	}
	return elastic.NewMinAggregation().Field(agg.Field)
}

// MaxAgg represent a max aggregation definition
type MaxAgg struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Field  string `json:"field"`
	Script bool   `json:"script"`
}

// GetName returns the agg name
func (agg *MaxAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *MaxAgg) AggSource() elastic.Aggregation {
	if agg.Script {
		return elastic.NewMaxAggregation().Script(elastic.NewScriptInline(agg.Field))
	}
	return elastic.NewMaxAggregation().Field(agg.Field)
}

// FilterAgg represent a filter aggregation definition
type FilterAgg struct {
	Name    string       `json:"name"`
	SubAggs *Aggregation `json:"aggs"`
	Query   *Query       `json:"filter"`
}

// GetName returns the agg name
func (agg *FilterAgg) GetName() string {
	return agg.Name
}

// AggSource returns a standard olivere elasticsearch aggregation
func (agg *FilterAgg) AggSource() elastic.Aggregation {
	return nil
}
