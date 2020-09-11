package engine

import "testing"

func TestTokenConditionString(t *testing.T) {
	if Between.String() != "between" {
		t.Error("Invalid string")
	}
}

func TestGetTokenCondition(t *testing.T) {
	if For != *GetConditionToken("for") {
		t.Error("Invalid get condition token for")
	}
	if From != *GetConditionToken("from") {
		t.Error("Invalid get condition token from")
	}
	if To != *GetConditionToken("to") {
		t.Error("Invalid get condition token to")
	}
	if Between != *GetConditionToken("between") {
		t.Error("Invalid get condition token between")
	}
	if Exists != *GetConditionToken("exists") {
		t.Error("Invalid get condition token exists")
	}
	if Script != *GetConditionToken("script") {
		t.Error("Invalid get condition token script")
	}
}

func TestGetTokenConditionInvalid(t *testing.T) {
	if GetConditionToken("not_a_token") != nil {
		t.Error("Token not_a_token should ne exists")
	}
}

func TestTokenConditionMarshalJSON(t *testing.T) {
	b, _ := For.MarshalJSON()
	if string(b) != "\"for\"" {
		t.Error("wrong marshal token")
		t.Log(string(b))
	}
}

func TestTokenConditionUnMarshalJSON(t *testing.T) {
	var bt ConditionToken
	err := bt.UnmarshalJSON([]byte(`"for"`))
	if err != nil {
		t.Error(err)
	}
	if bt != For {
		t.Error("wrong unmarshal token")
		t.Log(bt)
	}
}

func TestTokenConditionUnMarshalJSONInvalid(t *testing.T) {
	var bt ConditionToken
	err := bt.UnmarshalJSON([]byte(`{'key':'value'}`))
	if err == nil {
		t.Error("Unmarshal should return an error")
	}
}
