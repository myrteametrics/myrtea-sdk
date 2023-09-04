package expression

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

// IsInvalidNumber return true if the input interface is a not valid number
func IsInvalidNumber(input interface{}) bool {
	switch r := input.(type) {
	case float64:
		if math.IsInf(r, 1) || math.IsNaN(r) || math.IsInf(r, -1) {
			return true
		}
	}
	return false
}

// AssertEqual checks if values are equal
func AssertEqual(t *testing.T, a interface{}, b interface{}, message ...string) {
	if a == b {
		return
	}

	var errorMessage string
	if len(message) != 0 {
		errorMessage = strings.Join(message, " ") + "\n"
	}

	t.Helper()
	t.Errorf("%sReceived %v (type %v), expected %v (type %v)", errorMessage, a, reflect.TypeOf(a), b, reflect.TypeOf(b))
	t.FailNow()
}

// AssertNotEqual checks if values are not equal
func AssertNotEqual(t *testing.T, a interface{}, b interface{}, message ...string) {
	if a != b {
		return
	}

	var errorMessage string
	if len(message) != 0 {
		errorMessage = strings.Join(message, " ") + "\n"
	}

	t.Helper()
	t.Errorf("%sReceived %v (type %v), expected != %v (type %v)", errorMessage, a, reflect.TypeOf(a), b, reflect.TypeOf(b))
	t.FailNow()
}
