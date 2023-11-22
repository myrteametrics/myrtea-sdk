package engine

import (
	"testing"
)

func TestBuildElasticFilterValidIf(t *testing.T) {
	frags := []ConditionFragment{
		&BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&BooleanFragment{
					Operator:   If,
					Expression: "2 > 1",
					Fragments: []ConditionFragment{
						&LeafConditionFragment{
							Operator: Exists,
							Field:    "test",
						},
					},
				},
			},
		},
		&BooleanFragment{
			Operator:   If,
			Expression: "2 > 1",
			Fragments: []ConditionFragment{
				&LeafConditionFragment{
					Operator: Exists,
					Field:    "test",
				},
			},
		},
		&BooleanFragment{
			Operator:   If,
			Expression: "2 > 1",
			Fragments: []ConditionFragment{
				&LeafConditionFragment{
					Operator: Regexp,
					Field:    "monChamp",
					Value:    "ma.*expression",
				},
				&LeafConditionFragment{
					Operator: Exists,
					Field:    "test",
				},
			},
		},
	}

	for _, frag := range frags {
		query, err := buildElasticFilter(frag, map[string]interface{}{})
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if query == nil {
			t.Error("query must not be nil")
			t.FailNow()
		}
	}
}

func TestBuildElasticFilterInvalidIf(t *testing.T) {
	frags := []ConditionFragment{
		&BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&BooleanFragment{
					Operator:   If,
					Expression: "2 < 1",
					Fragments: []ConditionFragment{
						&LeafConditionFragment{
							Operator: Exists,
							Field:    "test",
						},
					},
				},
			},
		},
		&BooleanFragment{
			Operator:   If,
			Expression: "2 < 1",
			Fragments: []ConditionFragment{
				&LeafConditionFragment{
					Operator: Exists,
					Field:    "test",
				},
			},
		},
	}

	for _, frag := range frags {
		query, err := buildElasticFilter(frag, map[string]interface{}{})
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if query != nil {
			t.Error("query must be nil")
			t.FailNow()
		}
	}
}

func TestBuildElasticFilterValidIfWithVariables(t *testing.T) {
	frags := []ConditionFragment{
		&BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&BooleanFragment{
					Operator:   If,
					Expression: "myvar > 1",
					Fragments: []ConditionFragment{
						&LeafConditionFragment{
							Operator: Exists,
							Field:    "test",
						},
					},
				},
			},
		},
		&BooleanFragment{
			Operator:   If,
			Expression: "myvar > 1",
			Fragments: []ConditionFragment{
				&LeafConditionFragment{
					Operator: Exists,
					Field:    "test",
				},
			},
		},
	}

	for _, frag := range frags {
		query, err := buildElasticFilter(frag, map[string]interface{}{"myvar": 2})
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if query == nil {
			t.Error("query must not be nil")
			t.FailNow()
		}
	}
}

func TestBuildElasticFilterInvalidIfWithVariables(t *testing.T) {
	frags := []ConditionFragment{
		&BooleanFragment{
			Operator: And,
			Fragments: []ConditionFragment{
				&BooleanFragment{
					Operator:   If,
					Expression: "myvar < 1",
					Fragments: []ConditionFragment{
						&LeafConditionFragment{
							Operator: Exists,
							Field:    "test",
						},
					},
				},
			},
		},
		&BooleanFragment{
			Operator:   If,
			Expression: "myvar < 1",
			Fragments: []ConditionFragment{
				&LeafConditionFragment{
					Operator: Exists,
					Field:    "test",
				},
			},
		},
	}

	for _, frag := range frags {
		query, err := buildElasticFilter(frag, map[string]interface{}{"myvar": 2})
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if query != nil {
			t.Error("query must be nil")
			t.FailNow()
		}
	}
}

func TestBuildElasticFilterOptionalFor(t *testing.T) {
	frag := &BooleanFragment{
		Operator: And,
		Fragments: []ConditionFragment{
			&LeafConditionFragment{
				Operator: Exists,
				Field:    "test",
			},
			&LeafConditionFragment{
				Operator: OptionalFor,
				Field:    "",
				Value:    "",
			},
		},
	}

	query, err := buildElasticFilter(frag, map[string]interface{}{"zz": 2})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if query == nil {
		t.Error("query must not be nil")
		t.FailNow()
	}
}
