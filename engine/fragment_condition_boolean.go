package engine

import (
	"errors"
	"strings"
)

// BooleanFragment is a fragment type allowing to combine multiple condition fragment with a boolean operator
type BooleanFragment struct {
	Operator   BooleanToken        `json:"operator"`
	Expression string              `json:"expression,omitempty"`
	Fragments  []ConditionFragment `json:"fragments"`
}

// IsValid checks if an boolean fragment is valid and has no missing mandatory fields
// * Operator must not be empty (or 0 value)
// * Fragments must not be nil or empty
func (frag *BooleanFragment) IsValid() (bool, error) {
	if frag.Operator == 0 {
		return false, errors.New("Missing Operator")
	}
	if frag.Operator == If && frag.Expression == "" {
		return false, errors.New("Missing Expression with If operator")
	}
	if frag.Fragments == nil {
		return false, errors.New("Missing Fragments")
	}
	if len(frag.Fragments) <= 0 {
		return false, errors.New("Missing Fragments")
	}
	for _, subFrag := range frag.Fragments {
		if ok, err := subFrag.IsValid(); !ok {
			return false, errors.New("Invalid Fragment:" + err.Error())
		}
	}
	return true, nil
}

var booleanMap = map[BooleanToken]func() *BooleanFragment{
	And: func() *BooleanFragment {
		return &BooleanFragment{And, "", nil}
	},
	Or: func() *BooleanFragment {
		return &BooleanFragment{Or, "", nil}
	},
	Not: func() *BooleanFragment {
		return &BooleanFragment{Not, "", nil}
	},
	If: func() *BooleanFragment {
		return &BooleanFragment{If, "", nil}
	},
}

// GetBooleanFragment search and return a boolean fragment by it's name
func GetBooleanFragment(name string) (*BooleanFragment, error) {
	boolean := GetBooleanToken(strings.ToLower(name))
	if boolean == nil {
		return nil, errors.New("no token with name " + name)
	}
	if frag, ok := booleanMap[*boolean]; ok {
		f := frag()
		return f, nil
	}
	return nil, errors.New("no fragment with name " + name)
}
