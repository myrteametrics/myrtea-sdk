package expression

import (
	"errors"
	"math"
	"reflect"
	"strconv"
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

func convertAsFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return float64(v), nil
	case string:
		value, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	default:
		return 0, errors.New("Unable to convert this type as a float64")
	}
}

func convertAsBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return bool(v), nil
	case string:
		value, err := strconv.ParseBool(v)
		if err != nil {
			return false, err
		}
		return value, nil
	default:
		return false, errors.New("Unable to convert this type as a bool")
	}
}
