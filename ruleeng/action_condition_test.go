package ruleeng

import (
	"encoding/json"
	"testing"
)

func TestActionConditionUnmarshalLegacy(t *testing.T) {
	data := `{"t":{"enabled":true,"actionSetId":"a","conditions":[]},"t_minus_1":{"enabled":false,"actionSetId":"b","conditions":[]}}`
	var c ActionCondition
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Slots) != 2 {
		t.Fatalf("expected 2 slots, got %d", len(c.Slots))
	}
	if s := c.Slot(0); s == nil || !s.Enabled || s.ActionSetID != "a" {
		t.Errorf("invalid offset 0 slot: %+v", s)
	}
	if s := c.Slot(1); s == nil || s.Enabled || s.ActionSetID != "b" {
		t.Errorf("invalid offset 1 slot: %+v", s)
	}
}

func TestActionConditionUnmarshalLegacyPartial(t *testing.T) {
	var c ActionCondition
	if err := json.Unmarshal([]byte(`{"t_minus_1":{"enabled":true,"actionSetId":"b","conditions":[]}}`), &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Slots) != 1 || c.Slot(1) == nil || c.Slot(0) != nil {
		t.Errorf("unexpected slots: %+v", c.Slots)
	}
}

func TestActionConditionUnmarshalSlots(t *testing.T) {
	data := `{"slots":[{"offset":0,"enabled":true,"actionSetId":"a","conditions":[]},{"offset":4,"enabled":true,"actionSetId":"a","conditions":[]}]}`
	var c ActionCondition
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Slots) != 2 || c.Slot(4) == nil || c.MaxEnabledOffset() != 4 {
		t.Errorf("unexpected slots: %+v", c.Slots)
	}
}

func TestActionConditionRoundTripUsesSlots(t *testing.T) {
	var c ActionCondition
	if err := json.Unmarshal([]byte(`{"t":{"enabled":true,"actionSetId":"a","conditions":[]}}`), &c); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(out, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["slots"]; !ok {
		t.Errorf("expected the slots format, got %s", out)
	}
	if _, ok := raw["t"]; ok {
		t.Errorf("legacy field must not be written, got %s", out)
	}
}

func TestActionConditionMaxEnabledOffsetIgnoresDisabled(t *testing.T) {
	c := &ActionCondition{Slots: []ActionConditionSlot{
		{Offset: 1, Enabled: true, ActionSetID: "a"},
		{Offset: 9, Enabled: false, ActionSetID: "a"},
	}}
	if got := c.MaxEnabledOffset(); got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
	var nilCondition *ActionCondition
	if nilCondition.MaxEnabledOffset() != 0 || nilCondition.Slot(0) != nil {
		t.Errorf("nil condition must be safe")
	}
}

func TestActionConditionValidate(t *testing.T) {
	cases := []struct {
		name    string
		c       *ActionCondition
		wantErr bool
	}{
		{"nil", nil, false},
		{"valid", &ActionCondition{Slots: []ActionConditionSlot{{Offset: 0, Enabled: true, ActionSetID: "a"}, {Offset: 3, Enabled: true, ActionSetID: "a"}}}, false},
		{"negative", &ActionCondition{Slots: []ActionConditionSlot{{Offset: -1, Enabled: true, ActionSetID: "a"}}}, true},
		{"too deep", &ActionCondition{Slots: []ActionConditionSlot{{Offset: MaxActionConditionOffset + 1, Enabled: true, ActionSetID: "a"}}}, true},
		{"duplicate", &ActionCondition{Slots: []ActionConditionSlot{{Offset: 2, Enabled: true, ActionSetID: "a"}, {Offset: 2, Enabled: false, ActionSetID: "a"}}}, true},
		{"enabled without action set", &ActionCondition{Slots: []ActionConditionSlot{{Offset: 1, Enabled: true}}}, true},
		{"disabled without action set", &ActionCondition{Slots: []ActionConditionSlot{{Offset: 1, Enabled: false}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.c.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("wantErr=%v, got %v", tc.wantErr, err)
			}
		})
	}
}
