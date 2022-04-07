package expression

import (
	"fmt"

	"github.com/myrteametrics/myrtea-sdk/v4/utils"
)



func flatMap(arguments ...interface{})(map[string]interface{}, error){
	if len(arguments) != 3 {
		return nil, fmt.Errorf("flatMap() expects exactly three arguments")
	}
	arg1, ok1 := arguments[0].(string)
	arg2, ok2 := arguments[1].(string)
	arg3, ok3 := arguments[2].([]map[string]interface{})
	if !ok1 || !ok2 || !ok3 {
		return nil, fmt.Errorf("flatMap() expects exactly two string arguments and one []map[string]interface{}")
	}

	return utils.FlattenMap(arg1, arg2, arg3), nil

}