package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FilterHeaderOption describes a single header based filter. It is the franz-go
// counterpart of connector.FilterHeaderOption and keeps the same semantics.
type FilterHeaderOption struct {
	Key       string
	Value     string
	Values    []string
	Condition string
}

// GetHeader returns the value of the first header matching key.
func GetHeader(key string, headers []kgo.RecordHeader) (value string, found bool) {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value), true
		}
	}
	return "", false
}

// FilterHeaders evaluates every filter against the given record headers. It
// returns false and an explanatory message on the first failing filter, or true
// and an empty message when all filters pass.
//
// Supported conditions: exists, equals, equals_atleastone, notEquals,
// startWith, endWith, contains.
func FilterHeaders(filters []FilterHeaderOption, headers []kgo.RecordHeader) (bool, string) {
	for _, filter := range filters {
		value := "key_not_found"
		for _, header := range headers {
			if header.Key == filter.Key {
				value = string(header.Value)
			}
		}

		switch filter.Condition {
		case "exists":
			if value == "key_not_found" {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Value)
			}
		case "equals":
			if value != filter.Value {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Value)
			}
		case "equals_atleastone":
			isValid := false
			for _, val := range filter.Values {
				if val == value {
					isValid = true
					break
				}
			}
			if !isValid {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Values)
			}
		case "notEquals":
			if value == filter.Value {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Value)
			}
		case "startWith":
			if !strings.HasPrefix(value, filter.Value) {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Value)
			}
		case "endWith":
			if !strings.HasSuffix(value, filter.Value) {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Value)
			}
		case "contains":
			if !strings.Contains(value, filter.Value) {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", value, filter.Condition, filter.Value)
			}
		}
	}
	return true, ""
}
