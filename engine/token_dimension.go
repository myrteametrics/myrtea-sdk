package engine

import (
	"bytes"
	"encoding/json"
)

// DimensionToken enumeration for dimension tokens
type DimensionToken int

const (
	// By dimension token
	By DimensionToken = iota + 1
	// Histogram dimension token
	Histogram
	// DateHistogram dimension token
	DateHistogram
)

func (s DimensionToken) String() string {
	return dimensionToString[s]
}

// DimensionTokens list every supported dimension token
var DimensionTokens = []DimensionToken{By, Histogram, DateHistogram}

var dimensionToString = map[DimensionToken]string{
	By:            "by",
	Histogram:     "histogram",
	DateHistogram: "datehistogram",
}

var dimensionToID = map[string]DimensionToken{
	"by":            By,
	"histogram":     Histogram,
	"datehistogram": DateHistogram,
}

// GetDimensionToken search and return a dimension token from the standard supported operator list
func GetDimensionToken(name string) *DimensionToken {
	if value, exists := dimensionToID[name]; exists {
		return &value
	}
	return nil
}

// MarshalJSON marshals the enum as a quoted json string
func (s DimensionToken) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(dimensionToString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *DimensionToken) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value
	*s = dimensionToID[j]
	return nil
}
