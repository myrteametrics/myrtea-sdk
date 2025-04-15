package elasticsearch

import (
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/calendarinterval"
	"reflect"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v5/engine"
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"go.uber.org/zap"
)

func ConvertFactToSearchRequestV8(f engine.Fact, ti time.Time, parameters map[string]string) (*search.Request, error) {
	variables := make(map[string]interface{})
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

	if f.Intent.Operator != engine.Select && f.Intent.Operator != engine.Delete {
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
			Aggregations: make(map[string]types.Aggregations),
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
			var interval = types.Float64(frag.Interval)
			if interval == 0 {
				interval = 100 // default ?
			}
			agg.Histogram = &types.HistogramAggregation{
				Field:    some.String(frag.Term),
				Interval: &interval,
			}
			agg.Aggregations[name] = output

		case engine.DateHistogram:
			histogramAgg := &types.DateHistogramAggregation{
				Field: some.String(frag.Term),
			}

			// Fixed interval
			if frag.CalendarFixed {
				if frag.DateInterval == "" {
					histogramAgg.FixedInterval = 24 * time.Hour
				} else {
					duration, err := time.ParseDuration(frag.DateInterval)
					if err != nil {
						return "", types.Aggregations{}, err
					}
					histogramAgg.FixedInterval = duration
				}
			} else { // Calendar interval
				var calendarInterval = frag.DateInterval
				if calendarInterval == "" {
					calendarInterval = "month" // default ?
				}

				histogramAgg.CalendarInterval = &calendarinterval.CalendarInterval{Name: calendarInterval}
			}

			agg.DateHistogram = histogramAgg
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
		return "", types.Aggregations{}, errors.New("no intent fragment")
	}

	name := fmt.Sprintf("%s_%s", frag.Operator.String(), frag.Term)
	if frag.Name != "" {
		name = frag.Name
	}

	agg := types.Aggregations{
		Aggregations: make(map[string]types.Aggregations),
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
	var query = types.NewQuery()

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
			query.Range = map[string]types.RangeQuery{
				f.Field: createRangeQuery(f.Field, f.Value, nil, f.TimeZone),
			}
		case engine.To:
			query.Range = map[string]types.RangeQuery{
				f.Field: createRangeQuery(f.Field, nil, f.Value, f.TimeZone),
			}
		case engine.Between:
			query.Range = map[string]types.RangeQuery{
				f.Field: createRangeQuery(f.Field, f.Value, f.Value2, f.TimeZone),
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

func createRangeQuery(field string, value interface{}, value2 interface{}, timeZone string) types.RangeQuery {
	var rangeQuery types.RangeQuery

	if value != nil {
		switch v := value.(type) {
		case int64, int32:
			tvalue := types.Float64(float64(val))
			if value2 != nil {
				tvalue2 := types.Float64(float64(val2))
				rangeQuery = types.NumberRangeQuery{
					Gte: &tvalue,
					Lt:  &tvalue2,
				}
			} else {
				rangeQuery = types.NumberRangeQuery{
					Gte: &tvalue,
				}
			}
		case float64:
			tvalue := types.Float64(v)
			if value2 != nil {
				if v2, ok := value2.(float64); ok {
					tvalue2 := types.Float64(v2)
					rangeQuery = types.NumberRangeQuery{
						Gte: &tvalue,
						Lt:  &tvalue2,
					}
				} else {
					rangeQuery = types.NumberRangeQuery{
						Gte: &tvalue,
					}
				}
			} else {
				rangeQuery = types.NumberRangeQuery{
					Gte: &tvalue,
				}
			}
		case string:
			dateRangeQuery := types.DateRangeQuery{
				Gte: some.String(v),
			}
			if value2 != nil {
				if v2, ok := value2.(string); ok {
					dateRangeQuery.Lt = some.String(v2)
				}
			}
			if timeZone != "" {
				dateRangeQuery.TimeZone = some.String(timeZone)
			}
			rangeQuery = dateRangeQuery
		}
	} else if value2 != nil {
		switch v2 := value2.(type) {
		case float64:
			tvalue2 := types.Float64(v2)
			rangeQuery = types.NumberRangeQuery{
				Lt: &tvalue2,
			}
		case string:
			dateRangeQuery := types.DateRangeQuery{
				Lt: some.String(v2),
			}
			if timeZone != "" {
				dateRangeQuery.TimeZone = some.String(timeZone)
			}
			rangeQuery = dateRangeQuery
		}
	}

	return rangeQuery
}
