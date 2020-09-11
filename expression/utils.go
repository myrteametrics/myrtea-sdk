package expression

import "math"

//IsInvalidNumber return true if the input interface is a not valid number
func IsInvalidNumber(input interface{}) bool {
	switch r := input.(type) {
	case float64:
		if math.IsInf(r, 1) || math.IsNaN(r) || math.IsInf(r, -1) {
			return true
		}
	}
	return false
}
