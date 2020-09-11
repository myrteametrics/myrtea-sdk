package engine

import "testing"

func TestTokenDimensionString(t *testing.T) {
	if DateHistogram.String() != "datehistogram" {
		t.Error("Invalid string")
	}
}

func TestGetTokenDimension(t *testing.T) {
	if By != *GetDimensionToken("by") {
		t.Error("Invalid get dimension token by")
	}
	if Histogram != *GetDimensionToken("histogram") {
		t.Error("Invalid get dimension token histogram")
	}
	if DateHistogram != *GetDimensionToken("datehistogram") {
		t.Error("Invalid get dimension token datehistogram")
	}
}

func TestGetTokenDimensionInvalid(t *testing.T) {
	if GetDimensionToken("not_a_token") != nil {
		t.Error("Token not_a_token should ne exists")
	}
}

func TestTokenDimensionMarshalJSON(t *testing.T) {
	b, _ := Histogram.MarshalJSON()
	if string(b) != "\"histogram\"" {
		t.Error("wrong marshal token")
		t.Log(string(b))
	}
}

func TestTokenDimensionUnMarshalJSON(t *testing.T) {
	var bt DimensionToken
	err := bt.UnmarshalJSON([]byte(`"histogram"`))
	if err != nil {
		t.Error(err)
	}
	if bt != Histogram {
		t.Error("wrong unmarshal token")
		t.Log(bt)
	}
}

func TestTokenDimensionUnMarshalJSONInvalid(t *testing.T) {
	var bt DimensionToken
	err := bt.UnmarshalJSON([]byte(`{'key':'value'}`))
	if err == nil {
		t.Error("Unmarshal should return an error")
	}
}
