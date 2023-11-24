package connector

import (
	"bytes"
)

// Mode ...
type Mode int

const (
	// Self ...
	Self Mode = iota + 1
	// EnrichTo ...
	EnrichTo
	// EnrichFrom ...
	EnrichFrom
)

func (s Mode) String() string {
	return toString[s]
}

var toString = map[Mode]string{
	Self:       "self",
	EnrichTo:   "enrich_to",
	EnrichFrom: "enrich_from",
}

var toID = map[string]Mode{
	"self":        Self,
	"enrich_to":   EnrichTo,
	"enrich_from": EnrichFrom,
}

// MarshalJSON marshals the enum as a quoted json string
func (s Mode) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *Mode) UnmarshalJSON(b []byte) error {
	var j string
	err := jsoni.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value, 'Created' in this case.
	*s = toID[j]
	return nil
}
