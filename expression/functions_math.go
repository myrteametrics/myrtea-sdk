package expression

import (
	"errors"
	"math"
)

// Usage: <value> [<value>...]
func length(input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case []string:
		return (float64)(len(v)), nil
	case []int:
		return (float64)(len(v)), nil
	case []float64:
		return (float64)(len(v)), nil
	case []interface{}:
		return (float64)(len(v)), nil
	case string, int, float64, interface{}:
		return (float64)(1), nil
	default:
		return nil, errors.New("Not suported input type in function length")
	}
}

// Usage: <value> [<value>...]
func max(input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case []int:
		return maxInts(v), nil
	case []string:
		return maxStrings(v), nil
	case []float64:
		return maxFloats(v), nil
	default:
		return nil, errors.New("Not suported input type in function length")
	}
}

// Usage: <value> [<value>...]
func min(input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case []int:
		return minInts(v), nil
	case []string:
		return minStrings(v), nil
	case []float64:
		return minFloats(v), nil
	default:
		return nil, errors.New("Not suported input type in function length")
	}
}

// Usage: <value> [<value>...]
func sum(input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case []int:
		return sumInts(v), nil
	case []float64:
		return sumFloats(v), nil
	default:
		return nil, errors.New("Not suported input type in function length")
	}
}

// Usage: <value> [<value>...]
func average(input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case []int:
		return averageInts(v), nil
	case []float64:
		return averageFloats(v), nil
	default:
		return nil, errors.New("Not suported input type in function length")
	}
}

func maxInts(input []int) float64 {
	if len(input) == 0 {
		return 0
	}
	max := input[0]
	for _, v := range input {
		if max < v {
			max = v
		}
	}
	return (float64)(max)
}

func maxFloats(input []float64) float64 {
	if len(input) == 0 {
		return 0
	}
	max := input[0]
	for _, v := range input {
		if max < v {
			max = v
		}
	}
	return max
}

func maxStrings(input []string) string {
	if len(input) == 0 {
		return ""
	}
	max := input[0]
	for _, v := range input {
		if max < v {
			max = v
		}
	}
	return max
}

func minInts(input []int) float64 {
	if len(input) == 0 {
		return 0
	}
	min := input[0]
	for _, v := range input {
		if min > v {
			min = v
		}
	}
	return (float64)(min)
}

func minFloats(input []float64) float64 {
	if len(input) == 0 {
		return 0
	}
	min := input[0]
	for _, v := range input {
		if min > v {
			min = v
		}
	}
	return min
}

func minStrings(input []string) string {
	if len(input) == 0 {
		return ""
	}
	min := input[0]
	for _, v := range input {
		if min > v {
			min = v
		}
	}
	return min
}

func sumInts(input []int) float64 {
	sum := 0
	for _, v := range input {
		sum = sum + v
	}
	return (float64)(sum)
}

func sumFloats(input []float64) float64 {
	sum := (float64)(0)
	for _, v := range input {
		sum = sum + v
	}
	return sum
}

func averageInts(input []int) float64 {
	if len(input) == 0 {
		return 0
	}
	return sumInts(input) / (float64)(len(input))
}

func averageFloats(input []float64) float64 {
	if len(input) == 0 {
		return 0
	}
	return sumFloats(input) / (float64)(len(input))
}

// roundToDecimal rounds a number to a specific number of decimal places
func roundToDecimal(input interface{}, decimalPlaces interface{}) (interface{}, error) {
	floatInput, ok := input.(float64)
	if !ok {
		return nil, errors.New("first argument must be a float64")
	}

	var intDecimalPlaces int
	switch dp := decimalPlaces.(type) {
	case float64:
		intDecimalPlaces = int(dp)
	case int:
		intDecimalPlaces = dp
	default:
		return nil, errors.New("second argument must be an int or a float64 representing an int")
	}

	if intDecimalPlaces < 0 {
		return nil, errors.New("decimal places must be non-negative")
	}

	shifted := floatInput * math.Pow(10, float64(intDecimalPlaces))
	rounded := math.Round(shifted)
	return rounded / math.Pow(10, float64(intDecimalPlaces)), nil
}

func convertAsFloat(value interface{}) float64 {
	switch v := value.(type) {
	case int, int32, int64:
		return float64(v.(int))
	case float32, float64:
		return v.(float64)
	default:
		return 0
	}
}

func safeDivide(divider interface{}, dividende interface{}) float64 {
	floatDivider := convertAsFloat(divider)
	floatDividende := convertAsFloat(dividende)

	if floatDividende == float64(0) {
		return 0
	}

	return floatDivider / floatDividende
}
