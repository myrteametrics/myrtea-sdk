package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func TestIsExecutable(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
	}
	if !f.IsExecutable() {
		t.Error("Fact should be executable")
	}
}

// func TestAdvancedSource(t *testing.T) {
// 	f := Fact{
// ID: 1,
// Name: "1",
// 		AdvancedSource: `{"index":"ct-shipment-search","size":0,"order":true,"query":{"bool":{"must":[{"exists":{"field":"bu_origin"}},{"exists":{"field":"dpdfiledist_date"}},{"exists":{"field":"dpdex_done_date"}},{"term":{"cross_border":"true"}},{"range":{"dpdfiledist_date":{"from":"begin","time_zone":"Europe/Paris","to":"now"}}},{"script":{"script":"doc.dpdex_done_date.value.millis \\u003e doc.dpdfiledist_date.value.millis"}}]}},"aggs":{"global":{"avg":{"script":"(doc.dpdex_done_date.value.millis - doc.dpdfiledist_date.value.millis) / 1000"}},"group_by_bu":{"aggs":{"delay1":{"avg":{"script":"(doc.dpdex_done_date.value.millis - doc.dpdfiledist_date.value.millis) / 1000"}}},"terms":{"field":"bu_origin","size":25}}}}`,
// 	}
// 	if !f.IsExecutable() {
// 		t.Error("Fact should be executable")
// 	}

// 	executorCredentials := &elasticsearch.Credentials{
// 		URLs: []string{"http://localhost:9200"},
// 	}
// 	elasticsearch.ReplaceGlobals(executorCredentials)

// 	esQuery, _ := f.ToElasticQuery()
// 	search, err := builder.BuildEsSearch(elasticsearch.C(), esQuery, time.Now(), nil)
// 	res, err := elasticsearch.C().ExecuteSearch(search)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	spew.Dump(res.Aggregations)
// }

func TestIsExecutableInvalid(t *testing.T) {
	invalidFacts := []Fact{
		{ // Missing model
			ID:     1,
			Name:   "1",
			Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		},
		{ // Missing intent
			ID:    1,
			Name:  "1",
			Model: "model",
		},
		{ // Missing intent term
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Count},
		},
		{ // Missing intent operator
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Term: "myintent"},
		},
	}
	for _, f := range invalidFacts {
		if f.IsExecutable() {
			t.Error("Fact should not be executable")
		}
	}
}

func TestToElasticQueryValidSelect(t *testing.T) {

	t.SkipNow() // Development test

	f := Fact{
		ID:        1,
		Name:      "1",
		Model:     "model",
		Intent:    &IntentFragment{Operator: Select, Term: "model"},
		Condition: &LeafConditionFragment{Operator: For, Field: "myfield", Value: "myvalue"},
	}
	q, err := f.ToElasticQuery(time.Now(), map[string]string{})
	if err != nil {
		t.Error(err)
	}
	_ = q
}

func TestToElasticQueryValid(t *testing.T) {
	validFacts := []Fact{
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Count, Term: "myintent"},
			Condition: &BooleanFragment{
				Operator: And,
				Fragments: []ConditionFragment{
					&LeafConditionFragment{Operator: For, Field: "myfield", Value: "myvalue"},
				},
			},
		},
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Count, Term: "myintent"},
			Dimensions: []*DimensionFragment{
				{Operator: By, Term: "mydimension"},
				{Operator: Histogram, Term: "mydimension"},
				{Operator: DateHistogram, Term: "mydimension"},
			},
			Condition: &BooleanFragment{
				Operator: And,
				Fragments: []ConditionFragment{
					&LeafConditionFragment{Operator: For, Field: "myfield", Value: "myvalue"},
					&BooleanFragment{
						Operator: Not,
						Fragments: []ConditionFragment{
							&LeafConditionFragment{Operator: Exists, Field: "myfield"},
							&LeafConditionFragment{Operator: Script, Field: "${myscript}"},
						},
					},
					&BooleanFragment{
						Operator: Or,
						Fragments: []ConditionFragment{
							&LeafConditionFragment{Operator: Between, Field: "myfield", Value: "myvalue", Value2: "myvalue"},
							&LeafConditionFragment{Operator: From, Field: "myfield", Value: "myvalue"},
							&LeafConditionFragment{Operator: To, Field: "myfield", Value: "myvalue"},
						},
					},
				},
			},
		},
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Avg, Term: "myintent"},
		},
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Sum, Term: "myintent"},
		},
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Min, Term: "myintent"},
		},
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Max, Term: "myintent"},
		},
		{
			ID:     1,
			Name:   "1",
			Model:  "model",
			Intent: &IntentFragment{Operator: Select, Term: "model"},
		},
	}

	for _, f := range validFacts {
		b, err := json.Marshal(f)
		if err != nil {
			t.Error(err)
		}
		var f2 Fact
		err = json.Unmarshal(b, &f2)
		if err != nil {
			t.Error(err)
		}
		_, err = f.ToElasticQuery(time.Now(), map[string]string{})
		if err != nil {
			t.Error(err)
		}
	}

}

func TestToElasticQueryInvalid(t *testing.T) {
	invalidFacts := []Fact{
		{
			ID:   1,
			Name: "1",
		},
	}

	for _, f := range invalidFacts {
		_, err := f.ToElasticQuery(time.Now(), map[string]string{})
		if err == nil {
			t.Error("Fact should not be convertible to elastic query")
		}
	}
}

func TestContextualizeInvalid(t *testing.T) {
	facts := []Fact{
		{Condition: &LeafConditionFragment{Operator: From, Field: "myfield", Value: `test`}},
		{Condition: &LeafConditionFragment{Operator: From, Field: "myfield", Value: `"goodvalue" + test`}},
		{Condition: &LeafConditionFragment{Operator: From, Field: "myfield", Value: `calendar_add(test, "-24h")`}},
	}
	ts := time.Now()
	placeholders := map[string]string{}

	for _, f := range facts {
		err := f.ContextualizeCondition(ts, placeholders)
		if err == nil {
			t.Error("Expression should be invalid")
			t.FailNow()
		}
	}
}

func TestContextualize2(t *testing.T) {

	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Condition: &BooleanFragment{
			Operator: Or,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: `25`},
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: `"test"`},
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: `var1 + "test"`},
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: `begin`},
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: `begin + "test"`},
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: `calendar_add(begin, "-24h")`, TimeZone: "\"+02:00\""},
			},
		},
	}
	ts := time.Now()
	placeholders := map[string]string{"var1": "hello"}

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)
	if c1.Fragments[0].(*LeafConditionFragment).Value != float64(25) {
		t.Error("invalid fragment 0 value")
	}
	if c1.Fragments[1].(*LeafConditionFragment).Value != "test" {
		t.Error("invalid fragment 1 value")
	}
	if c1.Fragments[2].(*LeafConditionFragment).Value != "hellotest" {
		t.Error("invalid fragment 2 value")
	}
}

func TestContextualize(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Dimensions: []*DimensionFragment{
			{Operator: By, Term: "mydimension"},
			{Operator: Histogram, Term: "mydimension"},
			{Operator: DateHistogram, Term: "mydimension"},
		},
		Condition: &BooleanFragment{
			Operator: Or,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: Between, Field: "myfield", Value: "begin", Value2: "now"},
				&LeafConditionFragment{Operator: From, Field: "myfield", Value: "now"},
				&LeafConditionFragment{Operator: To, Field: "myfield", Value: "now"},
				&LeafConditionFragment{Operator: For, Field: "myfield", Value: "myvariable"},
			},
		},
	}

	ts, _ := time.Parse("2006-01-02T15:04:05.000Z07:00", "2019-09-15T12:30:00.000+02:00")
	placeholders := map[string]string{
		"myvariable": "myvalue",
	}

	f.ContextualizeDimensions(ts, placeholders)

	if f.Dimensions[2].TimeZone != "+02:00" {
		t.Error("Invalid datehistogram timezone")
	}

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)
	if c1.Fragments[0].(*LeafConditionFragment).Value != "2019-09-15T00:00:00.000" {
		t.Error("invalid __begin__replacement")
	}
	if c1.Fragments[0].(*LeafConditionFragment).Value2 != "2019-09-15T12:30:00.000" {
		t.Error("invalid __now__ replacement")
	}
	if c1.Fragments[0].(*LeafConditionFragment).TimeZone != "+02:00" {
		t.Error("invalid timezone")
	}
	if c1.Fragments[1].(*LeafConditionFragment).Value != "2019-09-15T12:30:00.000" {
		t.Error("invalid __now__ replacement")
	}
	if c1.Fragments[1].(*LeafConditionFragment).TimeZone != "+02:00" {
		t.Error("invalid timezone")
	}
	if c1.Fragments[2].(*LeafConditionFragment).Value != "2019-09-15T12:30:00.000" {
		t.Error("invalid __now__ replacement")
	}
	if c1.Fragments[2].(*LeafConditionFragment).TimeZone != "+02:00" {
		t.Error("invalid timezone")
	}
	if c1.Fragments[3].(*LeafConditionFragment).Value != "myvalue" {
		t.Error("invalid __myvariable__ replacement")
	}
}

func TestContextualizeOptionalFor(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Condition: &BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: Exists, Field: "myfield"},
				&LeafConditionFragment{Operator: OptionalFor, Field: "myfield", Value: "myvariable"},
			},
		},
	}

	placeholders := map[string]string{
		"myvariable": "myvalue",
	}

	ts := time.Now()

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)
	if c1.Fragments[1].(*LeafConditionFragment).Field == "" {
		t.Error("Fragment 2 Field should have not been removed (OptionalFor)")
	}
	if c1.Fragments[1].(*LeafConditionFragment).Value == "" {
		t.Error("Fragment 2 Value should have not been removed (OptionalFor)")
	}
}

func TestContextualizeOptionalForEmpty(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Condition: &BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: Exists, Field: "myfield"},
				&LeafConditionFragment{Operator: OptionalFor, Field: "myfield", Value: "myvariable"},
			},
		},
	}

	placeholders := map[string]string{
		// "myvariable": "myvalue",
	}

	ts := time.Now()

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)
	if c1.Fragments[1].(*LeafConditionFragment).Field != "" {
		t.Error("Fragment 2 Field should have been removed (OptionalFor)")
	}
	if c1.Fragments[1].(*LeafConditionFragment).Value != "" {
		t.Error("Fragment 2 Value should have been removed (OptionalFor)")
	}
}

func TestToElasticQueryOptionalForEmpty(t *testing.T) {

	f := Fact{
		ID:        1,
		Name:      "1",
		Model:     "model",
		Intent:    &IntentFragment{Operator: Sum, Term: "model"},
		Condition: &LeafConditionFragment{Operator: OptionalFor, Field: "myfield", Value: "myvalue"},
	}

	ti := time.Now()
	placeholders := map[string]string{}

	f.ContextualizeDimensions(ti, placeholders)
	err := f.ContextualizeCondition(ti, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	q, err := f.ToElasticQuery(ti, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_ = q
}

func TestContextualizeForSlice(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Condition: &BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: Exists, Field: "myfield"},
				&LeafConditionFragment{Operator: For, Field: "myfield", Value: `[myvariable1, "my_variable_2", "my-variable-3", myvariable4]`},
			},
		},
	}

	placeholders := map[string]string{
		"myvariable1": "test1",
		"myvariable4": "test4",
	}

	ts := time.Now()

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)
	if c1.Fragments[1].(*LeafConditionFragment).Field == "" {
		t.Error("Fragment 2 Field should have not been removed (OptionalFor)")
	}
	if c1.Fragments[1].(*LeafConditionFragment).Value == "" {
		t.Error("Fragment 2 Value should have not been removed (OptionalFor)")
	}

	// spew.Dump(f.ToElasticQuery(ts, placeholders))
	// q, _ := f.ToElasticQuery(ts, placeholders)
	// s, _ := builder.BuildEsSearchSource(q)
	// b, _ := json.Marshal(s)
	// t.Log(string(b))
	// t.Fail()

}

func TestContextualizeOptionalRegexp(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Condition: &BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: Exists, Field: "myfield"},
				&LeafConditionFragment{Operator: OptionalFor, Field: "myfield", Value: "myvariable"},
			},
		},
	}

	placeholders := map[string]string{
		"myvariable": "**a*a",
	}

	ts := time.Now()

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)
	if c1.Fragments[1].(*LeafConditionFragment).Field == "" {
		t.Error("Fragment 2 Field should have not been removed (OptionalRegexp)")
	}
	if c1.Fragments[1].(*LeafConditionFragment).Value == "" {
		t.Error("Fragment 2 Value should have not been removed (OptionalRegexp)")
	}
}

func TestContextualizeWildCard(t *testing.T) {
	f := Fact{
		ID:     1,
		Name:   "1",
		Model:  "model",
		Intent: &IntentFragment{Operator: Count, Term: "myintent"},
		Condition: &BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&LeafConditionFragment{Operator: Exists, Field: "myfield"},
				&LeafConditionFragment{Operator: OptionalWildcard, Field: "myfield1", Value: ""},
				&LeafConditionFragment{Operator: Wildcard, Field: "myfield", Value: "myvariable"},
			},
		},
	}

	placeholders := map[string]string{
		"myvariable": "**a*a",
	}

	ts := time.Now()

	err := f.ContextualizeCondition(ts, placeholders)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	c1 := f.Condition.(*BooleanFragment)

	if c1.Fragments[2].(*LeafConditionFragment).Field == "" {
		t.Error("Fragment 2 Field should have not been removed (Wildcard)")
	}
	if c1.Fragments[2].(*LeafConditionFragment).Value == "" {
		t.Error("Fragment 2 Value should have not been removed (Wildcard)")
	}
}
