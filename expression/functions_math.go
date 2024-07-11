package expression

import (
	"errors"
	"fmt"
	"math"
	"strconv"
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
func mathMax(input interface{}) (interface{}, error) {
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
func mathMin(input interface{}) (interface{}, error) {
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
	maxVal := input[0]
	for _, v := range input {
		if maxVal < v {
			maxVal = v
		}
	}
	return maxVal
}

func maxStrings(input []string) string {
	if len(input) == 0 {
		return ""
	}
	maxVal := input[0]
	for _, v := range input {
		if maxVal < v {
			maxVal = v
		}
	}
	return maxVal
}

func minInts(input []int) float64 {
	if len(input) == 0 {
		return 0
	}
	minVal := input[0]
	for _, v := range input {
		if minVal > v {
			minVal = v
		}
	}
	return (float64)(minVal)
}

func minFloats(input []float64) float64 {
	if len(input) == 0 {
		return 0
	}
	minVal := input[0]
	for _, v := range input {
		if minVal > v {
			minVal = v
		}
	}
	return minVal
}

func minStrings(input []string) string {
	if len(input) == 0 {
		return ""
	}
	minVal := input[0]
	for _, v := range input {
		if minVal > v {
			minVal = v
		}
	}
	return minVal
}

func sumInts(input []int) float64 {
	sumVal := 0
	for _, v := range input {
		sumVal = sumVal + v
	}
	return (float64)(sumVal)
}

func sumFloats(input []float64) float64 {
	sumVal := (float64)(0)
	for _, v := range input {
		sumVal = sumVal + v
	}
	return sumVal
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

func safeDivide(dividend interface{}, divisor interface{}) float64 {
	floatDividend, _ := convertAsFloat(dividend)
	floatDivisor, err := convertAsFloat(divisor)

	if err != nil || floatDivisor == 0 {
		return 0
	}

	return floatDividend / floatDivisor
}

func numberWithoutExponent(value interface{}) (interface{}, error) {
	floatValue, err := convertAsFloat(value)

	if err != nil {
		return value, fmt.Errorf("Unable to parse this value as a float : %v", value)
	}

	return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
}
