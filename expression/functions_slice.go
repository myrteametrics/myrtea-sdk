package expression

import (
	"fmt"
	"sort"
	"strings"
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

	// Build maps for fast lookup
	numericMap := make(map[float64]bool)
	otherMap := make(map[interface{}]bool)
	for _, val := range includeValues {
		if num, ok := toFloat64(val); ok {
			numericMap[num] = true
		} else {
			otherMap[val] = true
		}
	}

	result := make([]interface{}, 0)
	for _, item := range slice {
		include := false
		if num, ok := toFloat64(item); ok {
			if numericMap[num] {
				include = true
			}
		} else {
			if otherMap[item] {
				include = true
			}
		}
		if include {
			result = append(result, item)
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

	// Build maps for fast lookup
	numericMap := make(map[float64]bool)
	otherMap := make(map[interface{}]bool)
	for _, val := range excludeValues {
		if num, ok := toFloat64(val); ok {
			numericMap[num] = true
		} else {
			otherMap[val] = true
		}
	}

	result := make([]interface{}, 0)
	for _, item := range slice {
		excluded := false
		if num, ok := toFloat64(item); ok {
			if numericMap[num] {
				excluded = true
			}
		} else {
			if otherMap[item] {
				excluded = true
			}
		}
		if !excluded {
			result = append(result, item)
		}
	}

	return result, nil
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

// sort sorts an array (supports strings and numbers)
// Usage: sort(array, order)
// order: "asc" or "desc" (defaults to "asc" if not provided or invalid)
// Example: sort([3, 1, 2], "asc") returns [1, 2, 3]
// Example: sort([3, 1, 2], "desc") returns [3, 2, 1]
// Example: sort(["c", "a", "b"], "asc") returns ["a", "b", "c"]
func sortSlice(arguments ...interface{}) ([]interface{}, error) {
	if len(arguments) < 1 || len(arguments) > 2 {
		return nil, fmt.Errorf("sort() expects one or two arguments (array, [order])")
	}

	slice, ok := arguments[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("sort() expects first argument to be an array")
	}

	// Default to ascending order
	order := "asc"
	if len(arguments) == 2 {
		orderArg, ok := arguments[1].(string)
		if !ok {
			return nil, fmt.Errorf("sort() expects second argument to be a string (\"asc\" or \"desc\")")
		}
		order = strings.ToLower(orderArg)
		if order != "asc" && order != "desc" {
			return nil, fmt.Errorf("sort() order must be \"asc\" or \"desc\"")
		}
	}

	// Create a copy to avoid modifying the original
	result := make([]interface{}, len(slice))
	copy(result, slice)

	// Check if all elements are numeric or all are strings
	allNumeric := true
	allStrings := true

	for _, item := range result {
		if _, ok := toFloat64(item); !ok {
			allNumeric = false
		}
		if _, ok := item.(string); !ok {
			allStrings = false
		}
	}

	if allNumeric {
		// Sort numerically
		sort.Slice(result, func(i, j int) bool {
			vi, _ := toFloat64(result[i])
			vj, _ := toFloat64(result[j])
			if order == "asc" {
				return vi < vj
			}
			return vi > vj
		})
	} else if allStrings {
		// Sort as strings
		sort.Slice(result, func(i, j int) bool {
			si := result[i].(string)
			sj := result[j].(string)
			if order == "asc" {
				return si < sj
			}
			return si > sj
		})
	} else {
		return nil, fmt.Errorf("sort() expects all elements to be either numbers or strings")
	}

	return result, nil
}

// join concatenates array elements into a string with a separator
// Usage: join(array, separator)
// Example: join(["a", "b", "c"], ", ") returns "a, b, c"
// Example: join([1, 2, 3], "-") returns "1-2-3"
func join(arguments ...interface{}) (string, error) {
	if len(arguments) != 2 {
		return "", fmt.Errorf("join() expects exactly two arguments: array and separator")
	}

	slice, ok := arguments[0].([]interface{})
	if !ok {
		return "", fmt.Errorf("join() expects first argument to be an array")
	}

	separator, ok := arguments[1].(string)
	if !ok {
		return "", fmt.Errorf("join() expects second argument to be a string")
	}

	// Convert all elements to strings
	strSlice := make([]string, len(slice))
	for i, item := range slice {
		strSlice[i] = fmt.Sprintf("%v", item)
	}

	return strings.Join(strSlice, separator), nil
}
