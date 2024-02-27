package index

import (
	"bytes"
	"encoding/json"
)

// Depth ...
type Depth int

const (
	// Last is used for the current index (mainly for indexing)
	Last Depth = iota + 1
	// Patch is used for optimized update on old indices
	Patch
	// All is used to query all indices (mainly for search)
	All
)

func (s *Depth) String() string {
	return toString[*s]
}

var toString = map[Depth]string{
	Last:  "current",
	All:   "search",
	Patch: "Patch",
}

var toID = map[string]Depth{
	"current": Last,
	"search":  All,
	"Patch":   Patch,
}

// MarshalJSON marshals the enum as a quoted json string
func (s *Depth) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toString[*s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *Depth) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value, 'Created' in this case.
	*s = toID[j]
	return nil
}
