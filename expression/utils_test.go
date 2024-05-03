package expression

import (
	"testing"
)

func TestConvertAsFloat(t *testing.T) {

	testCases := []struct {
		name  string
		value interface{}
		want  float64
	}{
		{"convert float as a float64", 1234.1, 1234.1},
		{"convert string float as a float64", "1234.1", 1234.1},
		{"convert int as float64", 1234, 1234},
		{"convert string int as float64", "1234", 1234},

		{"convert explicit int as a float64", int(1), 1},
		{"convert explicit int32 as a float64", int32(1), 1},
		{"convert explicit int64 as a float64", int64(1), 1},
		{"convert explicit float32 as a float64", float32(1), 1},
		{"convert explicit float64 as a float64", float64(1), 1},
		{"invalid string", "test", 0},
		{"invalid string", false, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got_value, got_err := convertAsFloat(tc.value)
			if got_value != tc.want {
				t.Errorf("convertAsFloat() test with name \"%v\" returned \"%v\", want \"%v\", error returned is %v", tc.name, got_value, tc.want, got_err)
			}
		})
	}
}
func TestConvertAsBool(t *testing.T) {

	testCases := []struct {
		name  string
		value interface{}
		want  bool
	}{
		{"convert true as bool", true, true},
		{"convert string true as bool", "true", true},
		{"convert false as bool", false, false},
		{"convert string false as bool", "false", false},
		{"cannot convert string", "test", false},
		{"cannot convert another type", 1, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got_value, got_err := convertAsBool(tc.value)
			if got_value != tc.want {
				t.Errorf("convertAsBool() test with name \"%v\" returned \"%v\", want \"%v\", error returned is %v", tc.name, got_value, tc.want, got_err)
			}
		})
	}
}
