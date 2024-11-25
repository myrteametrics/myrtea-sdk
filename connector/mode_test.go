package connector

import (
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"testing"
)

func TestModeStringReturnsCorrectString(t *testing.T) {
	expression.AssertEqual(t, "self", Self.String())
	expression.AssertEqual(t, "enrich_to", EnrichTo.String())
	expression.AssertEqual(t, "enrich_from", EnrichFrom.String())
}

func TestModeUnmarshalError(t *testing.T) {
	var mode Mode
	err := mode.UnmarshalJSON([]byte(`{,"invalid}"`))
	expression.AssertNotEqual(t, nil, err)
}

func TestModeMarshalJSONReturnsCorrectJSON(t *testing.T) {
	expectedSelf := []byte(`"self"`)
	expectedEnrichTo := []byte(`"enrich_to"`)
	expectedEnrichFrom := []byte(`"enrich_from"`)

	selfJSON, err := Self.MarshalJSON()
	expression.AssertEqual(t, err, nil, "Expected no error")
	expression.AssertEqual(t, string(expectedSelf), string(selfJSON))

	enrichToJSON, err := EnrichTo.MarshalJSON()
	expression.AssertEqual(t, err, nil, "Expected no error")
	expression.AssertEqual(t, string(expectedEnrichTo), string(enrichToJSON))

	enrichFromJSON, err := EnrichFrom.MarshalJSON()
	expression.AssertEqual(t, err, nil, "Expected no error")
	expression.AssertEqual(t, string(expectedEnrichFrom), string(enrichFromJSON))
}

func TestModeUnmarshalJSONParsesCorrectly(t *testing.T) {
	var mode Mode

	err := mode.UnmarshalJSON([]byte(`"self"`))
	expression.AssertEqual(t, err, nil, "Expected no error")
	expression.AssertEqual(t, Self, mode)

	err = mode.UnmarshalJSON([]byte(`"enrich_to"`))
	expression.AssertEqual(t, err, nil, "Expected no error")
	expression.AssertEqual(t, EnrichTo, mode)

	err = mode.UnmarshalJSON([]byte(`"enrich_from"`))
	expression.AssertEqual(t, err, nil, "Expected no error")
	expression.AssertEqual(t, EnrichFrom, mode)
}

func TestModeUnmarshalJSONReturnsErrorForInvalidInput(t *testing.T) {
	var mode Mode

	err := mode.UnmarshalJSON([]byte(`"invalid"`))
	expression.AssertEqual(t, err, nil)
	expression.AssertEqual(t, Mode(0), mode)
}
