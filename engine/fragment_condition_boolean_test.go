package engine

import (
	"testing"
)

func TestGetBooleanFragment(t *testing.T) {
	for _, tok := range BooleanTokens {
		f, err := GetBooleanFragment(tok.String())
		if err != nil {
			t.Error(err)
		}
		if f.Operator != tok {
			t.Error("Invalid operator")
		}
	}
}

func TestGetBooleanFragmentInvalid(t *testing.T) {
	_, err := GetBooleanFragment("not_a_fragment")
	if err == nil {
		t.Error("Fragment not_a_fragment should not exists")
	}
}
