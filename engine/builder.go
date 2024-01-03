package engine

import (
	"errors"
	"fmt"

	"github.com/myrteametrics/myrtea-sdk/v4/builder"
	"github.com/myrteametrics/myrtea-sdk/v4/expression"
)

// buildElasticBucket
func buildElasticBucket(frags []*DimensionFragment, intent builder.Aggregation) (builder.Aggregation, error) {
	var output builder.Aggregation

	output = intent
	for _, frag := range frags {
		name := fmt.Sprintf("%s_%s", frag.Operator.String(), frag.Term)
		if frag.Name != "" {
			name = frag.Name
		}

		switch frag.Operator {
		case By:
			size := frag.Size
			if size == 0 {
				size = 100 // default ?
			}
			agg := builder.TermAgg{
				Name:  name, // FIXME: Manage multiple bucket-term (with specific id increment)
				Field: frag.Term,
				Size:  size,
			}
			if output != nil {
				agg.SubAggs = make([]builder.Aggregation, 0)
				agg.SubAggs = append(agg.SubAggs, output)
			}
			var a builder.Aggregation = &agg
			output = a

		case Histogram:
			interval := frag.Interval
			if interval == 0 {
				interval = 100 // default ?
			}
			agg := builder.HistogramAgg{
				Type:     "histogram",
				Name:     name,
				Field:    frag.Term,
				Interval: interval,
			}
			if output != nil {
				agg.SubAggs = make([]builder.Aggregation, 0)
				agg.SubAggs = append(agg.SubAggs, output)
			}
			var a builder.Aggregation = &agg
			output = a

		case DateHistogram:
			dateInterval := frag.DateInterval
			if dateInterval == "" {
				dateInterval = "1d" // default ?
			}
			agg := builder.DateHistogramAgg{
				Type:     "datehistogram",
				Name:     name,
				Field:    frag.Term,
				Interval: dateInterval,
			}
			if frag.TimeZone != "" {
				agg.TimeZone = frag.TimeZone
			}
			if output != nil {
				agg.SubAggs = make([]builder.Aggregation, 0)
				agg.SubAggs = append(agg.SubAggs, output)
			}
			var a builder.Aggregation = &agg
			output = a
		}
	}
	return output, nil
}

func buildElasticAgg(frag *IntentFragment) (builder.Aggregation, error) {

	if frag == nil {
		return nil, errors.New("No intent fragment")
	}

	name := fmt.Sprintf("%s_%s", frag.Operator.String(), frag.Term)
	if frag.Name != "" {
		name = frag.Name
	}

	var output builder.Aggregation
	switch frag.Operator {
	case Count:
		agg := builder.CardinalityAgg{
			Name:   name, // FIXME: Manage multiple agg (with specific id increment)
			Field:  frag.Term,
			Script: frag.Script,
		}
		output = &agg

	case Avg:
		agg := builder.AvgAgg{
			Name:   name, // FIXME: Manage multiple agg (with specific id increment)
			Field:  frag.Term,
			Script: frag.Script,
		}
		output = &agg

	case Sum:
		agg := builder.SumAgg{
			Name:   name, // FIXME: Manage multiple agg (with specific id increment)
			Field:  frag.Term,
			Script: frag.Script,
		}
		output = &agg

	case Min:
		agg := builder.MinAgg{
			Name:   name, // FIXME: Manage multiple agg (with specific id increment)
			Field:  frag.Term,
			Script: frag.Script,
		}
		output = &agg

	case Max:
		agg := builder.MaxAgg{
			Name:   name, // FIXME: Manage multiple agg (with specific id increment)
			Field:  frag.Term,
			Script: frag.Script,
		}
		output = &agg

	default:
		return nil, errors.New("Invalid intent kind")
	}
	return output, nil
}

func buildElasticFilter(frag ConditionFragment, variables map[string]interface{}) (builder.Query, error) {
	var output builder.Query

	switch frag.(type) {
	case *BooleanFragment:
		f := frag.(*BooleanFragment)
		b := builder.BoolQuery{
			Type:    "bool",
			Filter:  nil,
			Must:    nil,
			Should:  nil,
			MustNot: nil,
		}

		if f.Operator == If {
			val, err := expression.Process(expression.LangEval, f.Expression, variables)
			if err != nil {
				return nil, fmt.Errorf("expression evaluation failed : %s", err)
			}
			if valIf, ok := val.(bool); !ok || !valIf {
				break
			}
		}

		subAgg := make([]builder.Query, 0)
		for _, subFrag := range f.Fragments {
			agg, err := buildElasticFilter(subFrag, variables)
			if err != nil {
				return nil, err
			}
			if agg != nil {
				subAgg = append(subAgg, agg)
			}
		}

		if len(subAgg) > 0 {
			switch f.Operator {
			case And:
				b.Must = subAgg
			case Or:
				b.Should = subAgg
			case Not:
				b.MustNot = subAgg
			case If:
				b.Must = subAgg
			}
			output = &b
		}

	case *LeafConditionFragment:
		f := frag.(*LeafConditionFragment)

		switch f.Operator {
		case Exists:
			q := builder.ExistsQuery{
				Type:  "exists",
				Field: f.Field,
			}
			output = &q

		case For:
			q := builder.TermQuery{
				Type:  "terms",
				Field: f.Field,
				Value: f.Value,
			}
			output = &q

		case From:
			q := builder.RangeQuery{
				Type:        "range",
				Field:       f.Field,
				From:        f.Value,
				IncludeFrom: true,
				To:          nil,
			}
			if f.TimeZone != "" {
				q.TimeZone = f.TimeZone
			}
			output = &q

		case To:
			q := builder.RangeQuery{
				Type:      "range",
				Field:     f.Field,
				From:      nil,
				To:        f.Value,
				IncludeTo: false,
			}
			if f.TimeZone != "" {
				q.TimeZone = f.TimeZone
			}
			output = &q

		case Between:
			q := builder.RangeQuery{
				Type:        "range",
				Field:       f.Field,
				From:        f.Value,
				IncludeFrom: true,
				To:          f.Value2,
				IncludeTo:   false,
			}
			if f.TimeZone != "" {
				q.TimeZone = f.TimeZone
			}
			output = &q

		case Script:
			q := builder.ScriptQuery{
				Type:   "script",
				Script: f.Field,
			}
			output = &q

		case OptionalFor:
			if f.Field == "" || f.Value == "" {
				return nil, nil
			}
			q := builder.TermQuery{
				Type:  "terms",
				Field: f.Field,
				Value: f.Value,
			}
			output = &q
		case Regexp:
			q := builder.RegexpQuery{
				Type:  "regexp",
				Field: f.Field,
				Value: f.Value,
			}
			output = &q
		case OptionalRegexp:
			if f.Field == "" || f.Value == "" {
				return nil, nil
			}
			q := builder.RegexpQuery{
				Type:  "regexp",
				Field: f.Field,
				Value: f.Value,
			}
			output = &q

		default:
			return nil, errors.New("Invalid filter kind")
		}
	}
	return output, nil
}
