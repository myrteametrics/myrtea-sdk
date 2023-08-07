package expression

import (
	"fmt"
	"net/url"
)

// urlEncode returns a new string formatted with percent encoding
// Usage: <string>
func urlEncode(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("urlEncode() expects exactly 1 string argument")
	}
	str, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("urlEncode() expects exactly 1 string argument")
	}
	return url.QueryEscape(str), nil
}

// urlDecode returns a new string with percent encoding removed
// Usage: <string>
func urlDecode(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("urlDecode() expects exactly 1 string argument")
	}
	str, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("urlDecode() expects exactly 1 string argument")
	}
	return url.QueryUnescape(str)
}
