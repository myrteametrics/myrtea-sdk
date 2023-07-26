package expression

import (
	"fmt"
	"reflect"
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

// fetchValueByMatchingAttribute checks each map within the provided array for an entry where the key equals fieldName and the corresponding value equals fieldValue.
// If such an entry is found, it attempts to return the value corresponding to the key named otherFieldName from the same map.
// It returns an error if the array or field names are of incorrect types or if the value corresponding to otherFieldName is not an integer.
// The function's arguments are (array, fieldName, fieldValue, otherFieldName).
func fetchValueByMatchingAttribute(arguments ...interface{}) (int, error) {
	if len(arguments) != 4 {
		return 0, fmt.Errorf("findByAttribute() expects exactly four arguments")
	}

	array, ok := arguments[0].([]map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("findByAttribute() expects first argument to be a slice of map[string]interface{}")
	}

	fieldName, ok := arguments[1].(string)
	if !ok {
		return 0, fmt.Errorf("findByAttribute() expects second argument to be a string")
	}

	fieldValue := arguments[2]

	otherFieldName, ok := arguments[3].(string)
	if !ok {
		return 0, fmt.Errorf("findByAttribute() expects fourth argument to be a string")
	}

	for _, item := range array {
		if reflect.DeepEqual(item[fieldName], fieldValue) {
			otherFieldValue, ok := item[otherFieldName].(int)
			if !ok {
				return 0, fmt.Errorf("value of %s is not int", otherFieldName)
			}
			return otherFieldValue, nil
		}
	}

	return 0, nil
}
