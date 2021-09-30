package engine

import (
	"testing"
)

func TestGetLeafConditionFragment(t *testing.T) {
	for _, tok := range ConditionTokens {
		f, err := GetLeafConditionFragment(tok.String())
		if err != nil {
			t.Error(err)
		}
		if f.Operator != tok {
			t.Error("Invalid operator", f.Operator, tok)
		}
	}
}

func TestGetLeafConditionFragmentInvalid(t *testing.T) {
	_, err := GetLeafConditionFragment("not_a_fragment")
	if err == nil {
		t.Error("Fragment not_a_fragment should not exists")
	}
}
