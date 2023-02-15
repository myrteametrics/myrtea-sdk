package expression

import (
	"fmt"
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
		return nil, fmt.Errorf("day() expects exactly 3 string argument")
	}
	pattern, ok := arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("day() expects exactly 3 string argument")
	}
	replacement, ok := arguments[2].(string)
	if !ok {
		return nil, fmt.Errorf("day() expects exactly 3 string argument")
	}
	return strings.ReplaceAll(str, pattern, replacement), nil
}
