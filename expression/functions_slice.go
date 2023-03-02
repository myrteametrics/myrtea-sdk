package expression

import "fmt"

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
