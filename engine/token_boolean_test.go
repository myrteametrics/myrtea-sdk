package engine

import "testing"

func TestTokenBooleanString(t *testing.T) {
	if And.String() != "and" {
		t.Error("Invalid string")
	}
}

func TestGetTokenBoolean(t *testing.T) {
	if And != *GetBooleanToken("and") {
		t.Error("Invalid get boolean token and")
	}
	if Or != *GetBooleanToken("or") {
		t.Error("Invalid get boolean token or")
	}
	if Not != *GetBooleanToken("not") {
		t.Error("Invalid get boolean token not")
	}
	if If != *GetBooleanToken("if") {
		t.Error("Invalid get boolean token if")
	}
}

func TestGetTokenBooleanInvalid(t *testing.T) {
	if GetBooleanToken("not_a_token") != nil {
		t.Error("Token not_a_token should ne exists")
	}
}

func TestTokenBooleanMarshalJSON(t *testing.T) {
	b, _ := And.MarshalJSON()
	if string(b) != "\"and\"" {
		t.Error("wrong marshal token")
		t.Log(string(b))
	}
}

func TestTokenBooleanUnMarshalJSON(t *testing.T) {
	var bt BooleanToken
	err := bt.UnmarshalJSON([]byte(`"and"`))
	if err != nil {
		t.Error(err)
	}
	if bt != And {
		t.Error("wrong unmarshal token")
		t.Log(bt)
	}
}

func TestTokenBooleanUnMarshalJSONInvalid(t *testing.T) {
	var bt BooleanToken
	err := bt.UnmarshalJSON([]byte(`{'key':'value'}`))
	if err == nil {
		t.Error("Unmarshal should return an error")
	}
}
