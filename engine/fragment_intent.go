package engine

import (
	"errors"
	"strings"
)

// IntentFragment is a fragment type which contains a single intent definition
type IntentFragment struct {
	Name     string      `json:"name,omitempty"`
	Operator IntentToken `json:"operator"`
	Term     string      `json:"term"`
	Script   bool        `json:"script,omitempty"`
}

// IsValid checks if an intent fragment is valid and has no missing mandatory fields
// * Operator must not be empty (or 0 value)
// * Term must not be empty
func (frag *IntentFragment) IsValid() (bool, error) {
	if frag.Operator == 0 {
		return false, errors.New("Missing Operator")
	}
	if frag.Term == "" {
		return false, errors.New("Missing Term")
	}
	return true, nil
}

var intentMap = map[IntentToken]func() *IntentFragment{
	Count: func() *IntentFragment {
		return &IntentFragment{"", Count, "", false}
	},
	Sum: func() *IntentFragment {
		return &IntentFragment{"", Sum, "", false}
	},
	Avg: func() *IntentFragment {
		return &IntentFragment{"", Avg, "", false}
	},
	Min: func() *IntentFragment {
		return &IntentFragment{"", Min, "", false}
	},
	Max: func() *IntentFragment {
		return &IntentFragment{"", Max, "", false}
	},
	Select: func() *IntentFragment {
		return &IntentFragment{"", Select, "", false}
	},
}

// GetIntentFragment search and return an intent fragment by it's name
func GetIntentFragment(name string) (*IntentFragment, error) {
	intent := GetIntentToken(strings.ToLower(name))
	if intent == nil {
		return nil, errors.New("no token with name " + name)
	}
	if frag, ok := intentMap[*intent]; ok {
		f := frag()
		return f, nil
	}
	return nil, errors.New("no fragment with name " + name)
}
