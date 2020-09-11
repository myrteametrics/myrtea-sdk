package engine

import (
	"errors"
	"strings"
)

// LeafConditionFragment is a fragment containing a single terminal condition
type LeafConditionFragment struct {
	Operator ConditionToken `json:"operator"`
	Field    string         `json:"term"`
	Value    interface{}    `json:"value,omitempty"`
	Value2   interface{}    `json:"value2,omitempty"`
	TimeZone string         `json:"timezone,omitempty"`
}

// IsValid checks if a leaf condition fragment is valid and has no missing mandatory fields
// * Operator must not be empty (or 0 value)
// * Fragments must not be nil or empty
func (frag *LeafConditionFragment) IsValid() (bool, error) {
	if frag.Operator == 0 {
		return false, errors.New("Missing Operator")
	}
	if frag.Field == "" {
		return false, errors.New("Missing Field")
	}
	switch frag.Operator {
	case Exists:
	case Script:
	case For:
		if frag.Value == nil {
			return false, errors.New("Missing Value")
		}
	case From:
		if frag.Value == nil {
			return false, errors.New("Missing Value")
		}
	case To:
		if frag.Value == nil {
			return false, errors.New("Missing Value")
		}
	case Between:
		if frag.Value == nil {
			return false, errors.New("Missing Value")
		}
		if frag.Value2 == nil {
			return false, errors.New("Missing Value2")
		}
	}
	return true, nil
}

var leafConditionMap = map[ConditionToken]func() *LeafConditionFragment{
	For: func() *LeafConditionFragment {
		return &LeafConditionFragment{For, "", nil, nil, ""}
	},
	From: func() *LeafConditionFragment {
		return &LeafConditionFragment{From, "", nil, nil, ""}
	},
	To: func() *LeafConditionFragment {
		return &LeafConditionFragment{To, "", nil, nil, ""}
	},
	Between: func() *LeafConditionFragment {
		return &LeafConditionFragment{Between, "", nil, nil, ""}
	},
	Exists: func() *LeafConditionFragment {
		return &LeafConditionFragment{Exists, "", nil, nil, ""}
	},
	Script: func() *LeafConditionFragment {
		return &LeafConditionFragment{Script, "", nil, nil, ""}
	},
}

// GetLeafConditionFragment search and return a leaf condition fragment by it's name
func GetLeafConditionFragment(name string) (*LeafConditionFragment, error) {
	condition := GetConditionToken(strings.ToLower(name))
	if condition == nil {
		return nil, errors.New("no token with name " + name)
	}
	if frag, ok := leafConditionMap[*condition]; ok {
		f := frag()
		return f, nil
	}
	return nil, errors.New("no fragment with name " + name)
}
