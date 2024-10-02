package engine

import (
	"testing"
)

func TestGetDimensionFragment(t *testing.T) {
	for _, tok := range DimensionTokens {
		f, err := GetDimensionFragment(tok.String())
		if err != nil {
			t.Error(err)
		}
		if f.Operator != tok {
			t.Error("Invalid operator")
		}
	}
}

func TestGetDimensionFragmentInvalid(t *testing.T) {
	_, err := GetDimensionFragment("not_a_fragment")
	if err == nil {
		t.Error("Fragment not_a_fragment should not exists")
	}
}

func TestDimensionFragmentIsValid(t *testing.T) {
	cases := []struct {
		frag     DimensionFragment
		expected bool
		errMsg   string
	}{
		{DimensionFragment{Operator: By, Term: "term", Size: 1, Interval: 1}, true, ""},
		{DimensionFragment{Operator: 0, Term: "term", Size: 1, Interval: 1}, false, "missing Operator"},
		{DimensionFragment{Operator: By, Term: "", Size: 1, Interval: 1}, false, "missing Term"},
		{DimensionFragment{Operator: By, Term: "term", Size: -1, Interval: 1}, false, "size is lower than 0"},
		{DimensionFragment{Operator: By, Term: "term", Size: 1, Interval: -1}, false, "interval is lower than 0"},
		{DimensionFragment{Operator: DateHistogram, Term: "term", Size: 1, Interval: 1, DateInterval: "invalid", CalendarFixed: true}, false, "invalid date interval"},
		{DimensionFragment{Operator: DateHistogram, Term: "term", Size: 1, Interval: 1, DateInterval: "invalid", CalendarFixed: false}, false, "invalid date interval"},
		{DimensionFragment{Operator: DateHistogram, Term: "term", Size: 1, Interval: 1, DateInterval: "second", CalendarFixed: false}, true, ""},
	}

	for _, c := range cases {
		valid, err := c.frag.IsValid()
		if valid != c.expected {
			t.Errorf("expected %v, got %v", c.expected, valid)
		}
		if err != nil && err.Error() != c.errMsg {
			t.Errorf("expected error %v, got %v", c.errMsg, err)
		}
	}
}

func TestGetDimensionFragmentByName(t *testing.T) {
	cases := []struct {
		name     string
		expected DimensionToken
		errMsg   string
	}{
		{"by", By, ""},
		{"histogram", Histogram, ""},
		{"datehistogram", DateHistogram, ""},
		{"invalid", 0, "no token with name invalid"},
	}

	for _, c := range cases {
		frag, err := GetDimensionFragment(c.name)
		if c.errMsg == "" && err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if c.errMsg != "" && (err == nil || err.Error() != c.errMsg) {
			t.Errorf("expected error %v, got %v", c.errMsg, err)
		}
		if frag != nil && frag.Operator != c.expected {
			t.Errorf("expected operator %v, got %v", c.expected, frag.Operator)
		}
	}
}
