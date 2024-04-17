package connector

import (
	"fmt"
	"strings"

	"github.com/IBM/sarama"
)

type FilterHeaderOption struct {
	Key       string
	Value     string
	Values    []string
	Condition string
}

func FilterHeaders(filters []FilterHeaderOption, headers []*sarama.RecordHeader) (bool, string) {
	for _, filter := range filters {
		value := "key_not_found"
		for _, header := range headers {
			key := string(header.Key)
			if key == filter.Key {
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
