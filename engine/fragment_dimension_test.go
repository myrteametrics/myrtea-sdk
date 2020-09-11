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
