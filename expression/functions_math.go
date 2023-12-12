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
func roundToDecimal(input interface{}, decimalPlaces int) (interface{}, error) {
	switch v := input.(type) {
	case float64:
		shifted := v * math.Pow(10, float64(decimalPlaces))
		rounded := math.Round(shifted)
		return rounded / math.Pow(10, float64(decimalPlaces)), nil
	default:
		return nil, errors.New("not supported input type in function roundToDecimal")
	}
}
