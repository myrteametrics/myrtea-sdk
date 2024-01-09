package elasticsearchv8

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v4/engine"
	"github.com/myrteametrics/myrtea-sdk/v4/expression"
	"go.uber.org/zap"
)

func ConvertFactToSearchRequestV8(f engine.Fact, ti time.Time, parameters map[string]string) (*search.Request, error) {
	variables := make(map[string]interface{}, 0)
	for k, v := range parameters {
		variables[k] = v
	}
	for k, v := range expression.GetDateKeywords(ti) {
		variables[k] = v
	}

	request := search.NewRequest()
	query, err := buildElasticFilter(f.Condition, variables)
	if err != nil {
		zap.L().Warn("buildElasticFilter", zap.Error(err))
		return nil, err
	}
	request.Query = query

	if f.Intent.Operator != engine.Select {
		mainAggName, mainAgg, err := buildElasticAgg(f.Intent)
		if err != nil {
			zap.L().Warn("buildElasticAgg", zap.Error(err))
			return nil, err
		}
		aggName, agg, err := buildElasticBucket(mainAggName, mainAgg, f.Dimensions)
		if err != nil {
			zap.L().Warn("buildElasticBucket", zap.Error(err))
			return nil, err
		}
		aggregations := map[string]types.Aggregations{aggName: agg}
		request.Aggregations = aggregations
	}

	return request, nil
}

// // buildElasticBucket
func buildElasticBucket(name string, intent types.Aggregations, dimensions []*engine.DimensionFragment) (string, types.Aggregations, error) {
	var output types.Aggregations

	output = intent
	for _, frag := range dimensions {

		agg := types.Aggregations{
			Aggregations: make(map[string]types.Aggregations, 0),
		}

		switch frag.Operator {
		case engine.By:
			size := frag.Size
			if size == 0 {
				size = 100 // default ?
			}
			agg.Terms = &types.TermsAggregation{
				Field: some.String(frag.Term),
				Size:  some.Int(size),
			}
			agg.Aggregations[name] = output

		case engine.Histogram:
			var interval types.Float64 = types.Float64(frag.Interval)
			if interval == 0 {
				interval = 100 // default ?
			}
			agg.Histogram = &types.HistogramAggregation{
				Field:    some.String(frag.Term),
				Interval: &interval,
			}
			agg.Aggregations[name] = output

		case engine.DateHistogram:
			var dateInterval types.Duration = frag.DateInterval
			if dateInterval == "" {
				dateInterval = "1d" // default ?
			}
			agg.DateHistogram = &types.DateHistogramAggregation{
				Field:    some.String(frag.Term),
				Interval: &dateInterval,
			}
			agg.Aggregations[name] = output
		}

		output = agg

		name = fmt.Sprintf("%s_%s", frag.Operator.String(), frag.Term)
		if frag.Name != "" {
			name = frag.Name
		}

	}
	return name, output, nil
}

func buildElasticAgg(frag *engine.IntentFragment) (string, types.Aggregations, error) {

	if frag == nil {
		return "", types.Aggregations{}, errors.New("No intent fragment")
	}

	name := fmt.Sprintf("%s_%s", frag.Operator.String(), frag.Term)
	if frag.Name != "" {
		name = frag.Name
	}

	agg := types.Aggregations{
		Aggregations: make(map[string]types.Aggregations, 0),
	}

	switch frag.Operator {
	case engine.Count:
		agg.Cardinality = &types.CardinalityAggregation{
			Field: some.String(frag.Term),
		}

	case engine.Avg:
		agg.Avg = &types.AverageAggregation{
			Field: some.String(frag.Term),
		}

	case engine.Sum:
		agg.Sum = &types.SumAggregation{
			Field: some.String(frag.Term),
		}

	case engine.Min:
		agg.Min = &types.MinAggregation{
			Field: some.String(frag.Term),
		}

	case engine.Max:
		agg.Max = &types.MaxAggregation{
			Field: some.String(frag.Term),
		}

	default:
		return "", types.Aggregations{}, errors.New("Invalid intent kind: " + frag.Operator.String())
	}
	return name, agg, nil
}

func buildElasticFilter(frag engine.ConditionFragment, variables map[string]interface{}) (*types.Query, error) {

	var query *types.Query = types.NewQuery()

	switch f := frag.(type) {
	case *engine.BooleanFragment:
		query.Bool = types.NewBoolQuery()

		if f.Operator == engine.If {
			val, err := expression.Process(expression.LangEval, f.Expression, variables)
			if err != nil {
				return nil, fmt.Errorf("expression evaluation failed : %s", err)
			}
			if valIf, ok := val.(bool); !ok || !valIf {
				break
			}
		}

		subAgg := make([]types.Query, 0)
		for _, subFrag := range f.Fragments {
			agg, err := buildElasticFilter(subFrag, variables)
			if err != nil {
				return nil, err
			}
			if agg != nil {
				subAgg = append(subAgg, *agg)
			}
		}

		if len(subAgg) > 0 {
			switch f.Operator {
			case engine.And:
				query.Bool.Must = subAgg
			case engine.Or:
				query.Bool.Should = subAgg
			case engine.Not:
				query.Bool.MustNot = subAgg
			case engine.If:
				query.Bool.Must = subAgg
			}
		}

	case *engine.LeafConditionFragment:
		switch f.Operator {
		case engine.Exists:
			query.Exists = &types.ExistsQuery{
				Field: f.Field,
			}

		case engine.For:
			if reflect.ValueOf(f.Value).Kind() == reflect.Slice {
				var termsQuery types.TermsQuery
					termsQuery.TermsQuery = map[string]types.TermsQueryField{f.Field: f.Value}
					query.Terms = &termsQuery			
			} else {
				query.Term = map[string]types.TermQuery{
					f.Field: {Value: f.Value},
				}
			}

		case engine.From:
			var rangeQuery types.RangeQuery
			if value, ok := f.Value.(float64); ok {
				var tvalue types.Float64 = types.Float64(value)
				rangeQuery = types.NumberRangeQuery{
					Gte: &tvalue,
				}
			}
			if value, ok := f.Value.(string); ok {
				rangeQuery = types.DateRangeQuery{
					Gte:      some.String(value),
					TimeZone: some.String(f.TimeZone),
				}
			}
			query.Range = map[string]types.RangeQuery{
				f.Field: rangeQuery,
			}

		case engine.To:
			var rangeQuery types.RangeQuery
			if value, ok := f.Value.(float64); ok {
				var tvalue types.Float64 = types.Float64(value)
				rangeQuery = types.NumberRangeQuery{
					Lt: &tvalue,
				}
			}
			if value, ok := f.Value.(string); ok {
				rangeQuery = types.DateRangeQuery{
					Lt:       some.String(value),
					TimeZone: some.String(f.TimeZone),
				}
			}
			query.Range = map[string]types.RangeQuery{
				f.Field: rangeQuery,
			}

		case engine.Between:
			var rangeQuery types.RangeQuery
			value, ok := f.Value.(float64)
			value2, ok2 := f.Value2.(float64)
			if ok && ok2 {
				var tvalue types.Float64 = types.Float64(value)
				var tvalue2 types.Float64 = types.Float64(value2)
				rangeQuery = types.NumberRangeQuery{
					Gte: &tvalue,
					Lt:  &tvalue2,
				}
			}

			valueStr, ok := f.Value.(string)
			valueStr2, ok2 := f.Value2.(string)
			if ok && ok2 {
				rangeQuery = types.DateRangeQuery{
					Gte: some.String(valueStr),
					Lt:  some.String(valueStr2),
				}
			}

			query.Range = map[string]types.RangeQuery{
				f.Field: rangeQuery,
			}

		case engine.OptionalFor:
			if f.Field == "" || f.Value == "" {
				return nil, nil
			}
			query.Term = map[string]types.TermQuery{
				f.Field: {Value: f.Value},
			}
		case engine.Regexp:
			if value, ok := f.Value.(string); ok {
				query.Regexp = map[string]types.RegexpQuery{
					f.Field: {Value: value},
				}
			}
		case engine.OptionalRegexp:
			if f.Field == "" || f.Value == "" {
				return nil, nil
			}
			if value, ok := f.Value.(string); ok {
				query.Regexp = map[string]types.RegexpQuery{
					f.Field: {Value: value},
				}
			}
		case engine.Wildcard:
			if value, ok := f.Value.(string); ok {
				query.Wildcard = map[string]types.WildcardQuery{
					f.Field: {Value: &value},
				}
			}
		case engine.OptionalWildcard:
			if f.Field == "" || f.Value == "" {
				return nil, nil
			}
			if value, ok := f.Value.(string); ok {
				query.Wildcard = map[string]types.WildcardQuery{
					f.Field: {Value: &value},
				}
			}
			
		default:
			return nil, errors.New("Invalid filter kind: " + f.Operator.String())
		}
	}
	return query, nil
}
