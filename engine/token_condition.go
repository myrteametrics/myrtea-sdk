package engine

import (
	"bytes"
	"encoding/json"
)

// ConditionToken enumeration for condition tokens
type ConditionToken int

const (
	// For condition token
	For ConditionToken = iota + 1
	// From condition token
	From
	// To condition token
	To
	// Between condition token
	Between
	// Exists condition token
	Exists
	// Script condition token
	Script
	// OptionalFor condition token
	OptionalFor
	//
	Regexp
)

func (s ConditionToken) String() string {
	return conditionToString[s]
}

// ConditionTokens list every supported condition token
var ConditionTokens = []ConditionToken{For, From, To, Between, Exists, Script, OptionalFor, Regexp}

var conditionToString = map[ConditionToken]string{
	For:         "for",
	From:        "from",
	To:          "to",
	Between:     "between",
	Exists:      "exists",
	Script:      "script",
	OptionalFor: "optionalfor",
	Regexp:      "regexp",
}

var conditionToID = map[string]ConditionToken{
	"for":         For,
	"from":        From,
	"to":          To,
	"between":     Between,
	"exists":      Exists,
	"script":      Script,
	"optionalfor": OptionalFor,
	"regexp":      Regexp,
}

// GetConditionToken search and return a condition token from the standard supported operator list
func GetConditionToken(name string) *ConditionToken {
	if value, exists := conditionToID[name]; exists {
		return &value
	}
	return nil
}

// MarshalJSON marshals the enum as a quoted json string
func (s ConditionToken) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(conditionToString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *ConditionToken) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value
	*s = conditionToID[j]
	return nil
}
