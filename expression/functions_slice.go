package expression

import (
	"fmt"
)

func contains(arguments ...interface{}) (bool, error) {
	if len(arguments) != 2 {
		return false, fmt.Errorf("contains() expects exactly two arguments")
	}

	arg2, ok2 := arguments[1].(string)
	if !ok2 {
		return false, fmt.Errorf("contains() expects exactly one []string argument and one string argument")
	}

	switch arg1 := arguments[0].(type) {
	case string:
		return arg1 == arg2, nil

	case []interface{}:
		for _, arg := range arg1 {
			if s, ok := arg.(string); ok {
				if s == arg2 {
					return true, nil
				}
			}
		}
	default:
		return false, fmt.Errorf("contains() expects exactly one []string argument and one string argument")
	}

	return false, nil
}

func appendSlice(arguments ...interface{}) ([]interface{}, error) {
	if len(arguments) == 0 {
		return []interface{}{}, fmt.Errorf("append() expects at least one argument")
	}

	result := make([]interface{}, 0)

	for _, arg := range arguments {
		switch val := arg.(type) {
		case []interface{}:
			result = append(result, val...)
		case interface{}:
			result = append(result, val)
		}
	}

	return result, nil
}

// filter returns a new slice containing only elements that match any of the provided values
// Usage: filter(array, value1, [value2, ...])
// Example: filter(["a", "b", "c"], "b") returns ["b"]
// Example: filter(["a", "b", "c"], "a", "c") returns ["a", "c"]
func filter(arguments ...interface{}) ([]interface{}, error) {
	if len(arguments) < 2 {
		return nil, fmt.Errorf("filter() expects at least two arguments: array and value(s) to include")
	}

	slice, ok := arguments[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("filter() expects first argument to be an array")
	}

	// Collect values to include
	includeValues := make([]interface{}, 0, len(arguments)-1)
	for i := 1; i < len(arguments); i++ {
		includeValues = append(includeValues, arguments[i])
	}

	result := make([]interface{}, 0)
	for _, item := range slice {
		for _, val := range includeValues {
			if compareValues(item, val) {
				result = append(result, item)
				break
			}
		}
	}

	return result, nil
}

// exclude returns a new slice excluding elements that match any of the provided values
// Usage: exclude(array, value1, [value2, ...])
// Example: exclude(["a", "b", "c"], "b") returns ["a", "c"]
// Example: exclude(["a", "b", "c"], "a", "c") returns ["b"]
func exclude(arguments ...interface{}) ([]interface{}, error) {
	if len(arguments) < 2 {
		return nil, fmt.Errorf("exclude() expects at least two arguments: array and value(s) to exclude")
	}

	slice, ok := arguments[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("exclude() expects first argument to be an array")
	}

	// Collect values to exclude
	excludeValues := make([]interface{}, 0, len(arguments)-1)
	for i := 1; i < len(arguments); i++ {
		excludeValues = append(excludeValues, arguments[i])
	}

	result := make([]interface{}, 0)
	for _, item := range slice {
		excluded := false
		for _, val := range excludeValues {
			if compareValues(item, val) {
				excluded = true
				break
			}
		}
		if !excluded {
			result = append(result, item)
		}
	}

	return result, nil
}

// compareValues compares two values, handling type conversions for numbers
func compareValues(a, b interface{}) bool {
	// Direct equality check first
	if a == b {
		return true
	}

	// Handle numeric comparisons
	aNum, aIsNum := toFloat64(a)
	bNum, bIsNum := toFloat64(b)

	if aIsNum && bIsNum {
		return aNum == bNum
	}

	return false
}

// toFloat64 converts various numeric types to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}
