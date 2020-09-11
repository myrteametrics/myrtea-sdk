package expression

import (
	"errors"
	"fmt"
	"strconv"
)

func advancedAddition(a, b interface{}) (interface{}, error) {
	return advancedOperation(a, b, "+")
}

func advancedSubtraction(a, b interface{}) (interface{}, error) {
	return advancedOperation(a, b, "-")
}

func advancedMultiplication(a, b interface{}) (interface{}, error) {
	return advancedOperation(a, b, "*")
}

func advancedDivision(a, b interface{}) (interface{}, error) {
	return advancedOperation(a, b, "/")
}

func advancedOperation(a, b interface{}, operator string) (interface{}, error) {

	switch operand1 := a.(type) {
	case map[string]interface{}:
		switch operand2 := b.(type) {
		case map[string]interface{}:
			return mapMapOperation(operand1, operand2, operator)
		case int, int32, int64, float32, float64:
			return mapNumberOperation(operand1, operand2, operator)
		}
	}

	if a != nil && b != nil && operator == "+" {
		return fmt.Sprintf("%v%v", a, b), nil
	}
	return nil, fmt.Errorf("invalid operation (%T) %s (%T)", a, operator, b)

}

func mapMapOperation(operand1 map[string]interface{}, operand2 map[string]interface{}, operator string) (interface{}, error) {
	output := make(map[string]interface{}, 0)
	keys := make(map[string]bool, 0)
	for key, value := range operand1 {
		if v1, ok := extractFloat(value); ok {
			if val, ok := operand2[key]; ok {
				if v2, ok := extractFloat(val); ok {
					output[key] = applyMathOperator(operator, v1, v2)
					keys[key] = true
				} else {
					output[key] = nil
				}
			} else {
				output[key] = applyMathOperator(operator, v1, 0)
			}
		} else {
			output[key] = nil
		}
	}
	for key, value := range operand2 {
		if _, ok := keys[key]; ok {
			continue
		}
		if v2, ok := extractFloat(value); ok {
			output[key] = applyMathOperator(operator, 0, v2)
		} else {
			output[key] = nil
		}
	}
	return output, nil
}

func mapNumberOperation(operand1 map[string]interface{}, operand2 interface{}, operator string) (interface{}, error) {
	output := make(map[string]interface{}, 0)
	if v2, ok := extractFloat(operand2); ok {
		for key, value := range operand1 {
			if v1, ok := extractFloat(value); ok {
				output[key] = applyMathOperator(operator, v1, v2)
			} else {
				output[key] = nil
			}
		}
		return output, nil
	}
	return nil, errors.New("Unsupported types in map number operation")
}

func applyMathOperator(operator string, val1 float64, val2 float64) interface{} {
	var output float64
	switch operator {
	case "+":
		output = val1 + val2
	case "-":
		output = val1 - val2
	case "*":
		output = val1 * val2
	case "/":
		output = val1 / val2
	}

	if IsInvalidNumber(output) {
		return nil
	}

	return output
}

func extractFloat(input interface{}) (float64, bool) {
	switch val := input.(type) {
	case int, int32, int64, float32, float64:
		str := fmt.Sprint(val)
		v, err := strconv.ParseFloat(str, 64)
		return v, err == nil
	default:
		return 0, false
	}
}
