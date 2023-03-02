package expression

import "fmt"

func contains(arguments ...interface{}) (bool, error) {
	if len(arguments) != 2 {
		return false, fmt.Errorf("contains() expects exactly two arguments")
	}
	arg1, ok1 := arguments[0].([]interface{})
	arg2, ok2 := arguments[1].(string)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("contains() expects exactly one []string argument and one string argument")
	}

	for _, arg := range arg1 {
		if s, ok := arg.(string); ok {
			if s == arg2 {
				return true, nil
			}
		}
	}

	return false, nil

}
