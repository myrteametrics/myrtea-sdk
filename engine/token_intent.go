package engine

import (
	"bytes"
	"encoding/json"
)

// IntentToken enumeration for intent tokens
type IntentToken int

const (
	// Count intent token
	Count IntentToken = iota + 1
	// Sum intent token
	Sum
	// Avg intent token
	Avg
	// Min intent token
	Min
	// Max intent token
	Max
	// Select intent token
	Select
)

func (s IntentToken) String() string {
	return intentToString[s]
}

// IntentTokens list every supported intent token
var IntentTokens = []IntentToken{Count, Sum, Avg, Min, Max, Select}

var intentToString = map[IntentToken]string{
	Count:  "count",
	Sum:    "sum",
	Avg:    "avg",
	Min:    "min",
	Max:    "max",
	Select: "select",
}

var intentToID = map[string]IntentToken{
	"count":  Count,
	"sum":    Sum,
	"avg":    Avg,
	"min":    Min,
	"max":    Max,
	"select": Select,
}

// GetIntentToken search and return an intent token from the standard supported operator list
func GetIntentToken(name string) *IntentToken {
	if value, exists := intentToID[name]; exists {
		return &value
	}
	return nil
}

// MarshalJSON marshals the enum as a quoted json string
func (s IntentToken) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(intentToString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *IntentToken) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value
	*s = intentToID[j]
	return nil
}
