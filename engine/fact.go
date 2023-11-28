package engine

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v4/builder"
	"github.com/myrteametrics/myrtea-sdk/v4/expression"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
	"go.uber.org/zap"
)

// ConditionsAlias ...
type ConditionsAlias struct {
	Condition ConditionFragment `json:"condition,omitempty"`
	Comment   string            `json:"comment"`
}

// Restitution ...
type Restitution struct{}

// Fact is the main structure used to for the full fact definition
type Fact struct {
	ID               int64                `json:"id"`
	Name             string               `json:"name"`
	Description      string               `json:"description"`
	IsObject         bool                 `json:"isObject"`
	Model            string               `json:"model"`
	CalculationDepth int64                `json:"calculationDepth,omitempty"`
	Intent           *IntentFragment      `json:"intent,omitempty"`
	Dimensions       []*DimensionFragment `json:"dimensions,omitempty"`
	Condition        ConditionFragment    `json:"condition,omitempty"`
	Restitution      []Restitution        `json:"restitution,omitempty"`
	Comment          string               `json:"comment"`
	AdvancedSource   string               `json:"source,omitempty"`
	IsTemplate       bool                 `json:"isTemplate"`
	Variables        []string             `json:"variables,omitempty"`
}

// IsValid checks if a fact definition is valid and has no missing mandatory fields
// * Name must not be empty
// * CalculationDepth must not be less than 0
// * Intent must be valid
// * Dimensions must be valid
// * Condition must be valid
func (f *Fact) IsValid() (bool, error) {
	if f.Name == "" {
		return false, errors.New("Missing Name")
	}
	if f.CalculationDepth < 0 {
		return false, errors.New("Missing CalculationDepth")
	}

	if !f.IsObject && f.AdvancedSource == "" {
		if f.Model == "" {
			return false, errors.New("Missing Model")
		}
		if f.Intent == nil {
			return false, errors.New("Missing Intent")
		}
		if ok, err := f.Intent.IsValid(); !ok {
			return false, errors.New("Invalid Intent:" + err.Error())
		}
		if f.Dimensions != nil {
			for _, dimension := range f.Dimensions {
				if ok, err := dimension.IsValid(); !ok {
					return false, errors.New("Invalid Dimension:" + err.Error())
				}
			}
		}
		if f.Condition != nil {
			if ok, err := f.Condition.IsValid(); !ok {
				return false, errors.New("Invalid Condition:" + err.Error())
			}
		}
	}
	return true, nil
}

// UnmarshalJSON unmarshal a fact from a json string
func (f *Fact) UnmarshalJSON(b []byte) error {
	type Alias Fact
	aux := &struct {
		*Alias
		Condition *json.RawMessage `json:"condition,omitempty"`
	}{
		Alias: (*Alias)(f),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if aux.Condition != nil {
		condition, err := unmarshalConditionFragment(aux.Condition)
		if err != nil {
			return err
		}
		f.Condition = condition
	}
	return nil
}

// IsExecutable check if a fact is complete and executable
func (f *Fact) IsExecutable() bool {
	if f.AdvancedSource != "" {
		return true
	}

	if f.Model == "" {
		return false
	}
	if f.Intent == nil {
		return false
	}
	if f.Intent.Operator == 0 {
		return false
	}
	if f.Intent.Term == "" {
		return false
	}
	return true
}

// ContextualizeDimensions contextualize fact dimensions placeholders (standard or custom) and set the right timezone if needed
func (f *Fact) ContextualizeDimensions(t time.Time, placeholders map[string]string) {
	for _, dim := range f.Dimensions {
		if dim.Operator == DateHistogram {
			dim.TimeZone = utils.GetTimeZone(t)
		}
	}
}

// ContextualizeCondition contextualize fact condition tree placeholders (standard or custom) and set the right timezone if needed
func (f *Fact) ContextualizeCondition(t time.Time, placeholders map[string]string, processGval ...bool) error {
	return contextualizeCondition(f.Condition, t, placeholders, processGval...)
}

func contextualizeCondition(condition ConditionFragment, t time.Time, placeholders map[string]string, processGval ...bool) error {

	shouldProcess := shouldProcess(processGval...)

	variables := make(map[string]interface{}, 0)
	for k, v := range placeholders {
		variables[k] = v
	}
	for k, v := range expression.GetDateKeywords(t) {
		variables[k] = v
	}

	switch c := condition.(type) {
	case *BooleanFragment:
		for _, cond := range c.Fragments {
			err := contextualizeCondition(cond, t, placeholders, processGval...)
			if err != nil {
				return err
			}
		}
	case *LeafConditionFragment:
		if c.Value != nil && reflect.TypeOf(c.Value).Kind() == reflect.String {
			if shouldProcess {
				exp := c.Value.(string)
				result, err := expression.Process(expression.LangEval, exp, variables)
				if err != nil {
					if c.Operator == OptionalFor {
						c.Field = ""
						c.Value = ""
					} else {
						zap.L().Warn("Expression evaluation failed", zap.String("exp", c.Value.(string)), zap.Error(err))
						return err
					}
				}
				if result != nil {
					c.Value = result
				}
			}
		}
		if c.Value2 != nil && reflect.TypeOf(c.Value2).Kind() == reflect.String {
			if shouldProcess {
				exp := c.Value2.(string)
				result, err := expression.Process(expression.LangEval, exp, variables)
				if err != nil {
					zap.L().Warn("Expression evaluation failed", zap.String("exp", c.Value2.(string)), zap.Error(err))
					return err
				}
				if result != nil {
					c.Value2 = result
				}
			}
		}
		if c.TimeZone == "" && (c.Operator == From || c.Operator == To || c.Operator == Between) {
			c.TimeZone = utils.GetTimeZone(t)
		}
	}
	return nil
}

func (f *Fact) toElasticQueryAdvancedSource() (*builder.EsSearch, error) {
	var search builder.EsSearch
	var source map[string]interface{}
	err := json.Unmarshal([]byte(f.AdvancedSource), &source)
	if err != nil {
		return nil, err
	}
	delete(source, "index")
	delete(source, "order")
	b, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}

	search.Source = string(b)
	return &search, nil
}

// ToElasticQuery convert the fact in an elasticsearch search query
func (f *Fact) ToElasticQuery(t time.Time, placeholders map[string]string) (*builder.EsSearch, error) {

	if !f.IsExecutable() {
		return nil, errors.New("Incomplete fact")
	}

	if f.AdvancedSource != "" {
		return f.toElasticQueryAdvancedSource()
	}

	output := builder.EsSearch{}
	output.Indices = []string{}
	output.Size = 0
	output.Order = false

	if f.Intent != nil {
		if f.Intent.Operator != Select {
			var intentQuery builder.Aggregation
			if f.Model != f.Intent.Term || f.Intent.Operator != Count {
				var err error
				intentQuery, err = buildElasticAgg(f.Intent)
				if err != nil {
					return nil, err
				}
			}
			if f.Dimensions != nil {
				var err error
				intentQuery, err = buildElasticBucket(f.Dimensions, intentQuery)
				if err != nil {
					return nil, err
				}
			}
			if intentQuery != nil {
				output.Aggs = []builder.Aggregation{intentQuery}
			}
		} else {
			output.Order = true
		}
	} // else { // find an implicit intent ? }

	if f.Condition != nil {
		variables := make(map[string]interface{}, 0)
		for k, v := range placeholders {
			variables[k] = v
		}
		for k, v := range expression.GetDateKeywords(t) {
			variables[k] = v
		}

		filterQuery, err := buildElasticFilter(f.Condition, variables)
		if err != nil {
			return nil, err
		}
		if filterQuery != nil {
			var filter builder.Query
			query := builder.BoolQuery{
				Type:    "bool",
				Filter:  []builder.Query{filterQuery},
				Must:    nil,
				Should:  nil,
				MustNot: nil,
			}
			filter = &query
			output.Query = filter
		}
	}

	return &output, nil
}

func shouldProcess(processGval ...bool) bool {
	if len(processGval) > 0 {
		return processGval[0]
	}
	return true
}
