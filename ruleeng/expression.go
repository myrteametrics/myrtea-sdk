package ruleeng

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/myrteametrics/myrtea-sdk/v4/expression"
)

//Expression struct to represent an expression
type Expression string

//EvaluateAsBool evaluates the expression and verifies that the result is the type boolean
//use to evaluate the conditions in the rule cases (conditions should return a boolean as result)
func (exp Expression) EvaluateAsBool(k KnowledgeBase) (bool, error) {
	value, err := expression.Process(expression.LangEval, string(exp), k.GetFacts())

	if err != nil {
		return false, err
	}

	switch v := value.(type) {
	case bool:
		return v, nil
	default:
		return false, errors.New("The result of the expression is not boolean: " + string(exp))
	}
}

//EvaluateAsString evaluates the expression and verifies that the result is the type string
func (exp Expression) EvaluateAsString(k KnowledgeBase) (string, error) {
	value, err := expression.Process(expression.LangEval, string(exp), k.GetFacts())

	if err != nil {
		return "", err
	}

	switch v := value.(type) {
	case string:
		return v, nil
	default:
		return "", errors.New("The result of the expression is not string: " + string(exp))
	}
}

//Evaluate evaluates the expression and return the result as interface{}
func (exp Expression) Evaluate(k KnowledgeBase) (interface{}, error) {
	return expression.Process(expression.LangEval, string(exp), k.GetFacts())
}

//MarshalJSON mashals a Expression to a a quoted json string
func (exp Expression) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(strings.Replace(string(exp), "\"", "\\\"", -1))
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to Expression
func (exp *Expression) UnmarshalJSON(data []byte) error {
	var expStr string
	err := json.Unmarshal(data, &expStr)
	if err != nil {
		return err
	}
	*exp = Expression(expStr)
	return err
}
