package engine

import "testing"

func TestTokenIntentString(t *testing.T) {
	if Count.String() != "count" {
		t.Error("Invalid string")
	}
}

func TestGetTokenIntent(t *testing.T) {
	if Count != *GetIntentToken("count") {
		t.Error("Invalid get intent token count")
	}
	if Sum != *GetIntentToken("sum") {
		t.Error("Invalid get intent token sum")
	}
	if Avg != *GetIntentToken("avg") {
		t.Error("Invalid get intent token avg")
	}
	if Min != *GetIntentToken("min") {
		t.Error("Invalid get intent token min")
	}
	if Max != *GetIntentToken("max") {
		t.Error("Invalid get intent token max")
	}
}

func TestGetTokenIntentInvalid(t *testing.T) {
	if GetIntentToken("not_a_token") != nil {
		t.Error("Token not_a_token should ne exists")
	}
}

func TestTokenIntentMarshalJSON(t *testing.T) {
	b, _ := Count.MarshalJSON()
	if string(b) != "\"count\"" {
		t.Error("wrong marshal token")
		t.Log(string(b))
	}
}

func TestTokenIntentUnMarshalJSON(t *testing.T) {
	var bt IntentToken
	err := bt.UnmarshalJSON([]byte(`"count"`))
	if err != nil {
		t.Error(err)
	}
	if bt != Count {
		t.Error("wrong unmarshal token")
		t.Log(bt)
	}
}

func TestTokenIntentUnMarshalJSONInvalid(t *testing.T) {
	var bt IntentToken
	err := bt.UnmarshalJSON([]byte(`{'key':'value'}`))
	if err == nil {
		t.Error("Unmarshal should return an error")
	}
}
