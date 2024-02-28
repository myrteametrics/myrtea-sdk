package expression

import (
	"fmt"
	"strconv"
	"strings"
)

// replace returns a new string with all matches of the pattern replaced by the replacement.
// Usage: <string> <pattern> <replacement>
func replace(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 3 {
		return nil, fmt.Errorf("replace() expects exactly 3 string argument")
	}
	str, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("replace() expects exactly 3 string argument")
	}
	pattern, ok := arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("replace() expects exactly 3 string argument")
	}
	replacement, ok := arguments[2].(string)
	if !ok {
		return nil, fmt.Errorf("replace() expects exactly 3 string argument")
	}
	return strings.ReplaceAll(str, pattern, replacement), nil
}

// Atoi converts a string to an integer.
// Usage: <string>
func atoi(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("Atoi() expects exactly 1 string argument")
	}
	str, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("Atoi() argument must be a string")
	}
	result, err := strconv.Atoi(str)
	if err != nil {
		return nil, fmt.Errorf("Atoi() error converting string to int: %s", err)
	}
	return result, nil
}
