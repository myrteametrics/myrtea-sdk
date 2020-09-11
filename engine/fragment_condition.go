package engine

import (
	"encoding/json"
)

// ConditionFragment is an interface for condition fragment (which can be boolean or leafcondition)
type ConditionFragment interface {
	IsValid() (bool, error)
}

func unmarshalConditionFragment(raw *json.RawMessage) (ConditionFragment, error) {
	if raw == nil {
		return nil, nil
	}
	var m map[string]*json.RawMessage
	err := json.Unmarshal(*raw, &m)
	if err != nil {
		return nil, err
	}

	var operator string
	err = json.Unmarshal(*m["operator"], &operator)
	if err != nil {
		return nil, err
	}

	switch operator {
	case And.String(), Or.String(), Not.String(), If.String():

		type Alias BooleanFragment
		aux := struct {
			*BooleanFragment
			Fragments []*json.RawMessage `json:"fragments"`
		}{
			BooleanFragment: (*BooleanFragment)(nil),
		}
		if err := json.Unmarshal(*raw, &aux); err != nil {
			return nil, err
		}

		subFrags := make([]ConditionFragment, 0)
		for _, subFragDef := range aux.Fragments {
			subFrag, err := unmarshalConditionFragment(subFragDef)
			if err != nil {
				return nil, err
			}
			subFrags = append(subFrags, subFrag)
		}
		aux.BooleanFragment.Fragments = subFrags
		return aux.BooleanFragment, nil

	default:
		var frag *LeafConditionFragment
		err := json.Unmarshal(*raw, &frag)
		if err != nil {
			return nil, err
		}
		return frag, nil
	}
}
