package engine

import (
	"testing"
)

func TestGetIntentFragment(t *testing.T) {
	for _, tok := range IntentTokens {
		f, err := GetIntentFragment(tok.String())
		if err != nil {
			t.Error(err)
		}
		if f.Operator != tok {
			t.Error("Invalid operator")
		}
	}
}

func TestGetIntentFragmentInvalid(t *testing.T) {
	_, err := GetIntentFragment("not_a_fragment")
	if err == nil {
		t.Error("Fragment not_a_fragment should not exists")
	}
}
