package engine

import (
	"bytes"
	"encoding/json"
)

// BooleanToken enumeration for boolean tokens
type BooleanToken int

const (
	// And boolean token
	And BooleanToken = iota + 1
	// Or boolean token
	Or
	// Not boolean token
	Not
	// If boolean token
	If
)

func (s BooleanToken) String() string {
	return booleanToString[s]
}

// BooleanTokens list every supported boolean token
var BooleanTokens = []BooleanToken{And, Or, Not}

var booleanToString = map[BooleanToken]string{
	And: "and",
	Or:  "or",
	Not: "not",
	If:  "if",
}

var booleanToID = map[string]BooleanToken{
	"and": And,
	"or":  Or,
	"not": Not,
	"if":  If,
}

// GetBooleanToken search and return a boolean token from the standard supported operator list
func GetBooleanToken(name string) *BooleanToken {
	if value, exists := booleanToID[name]; exists {
		return &value
	}
	return nil
}

// MarshalJSON marshals the enum as a quoted json string
func (s BooleanToken) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(booleanToString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *BooleanToken) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value
	*s = booleanToID[j]
	return nil
}
