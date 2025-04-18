package expression

import (
	"fmt"
	"testing"
)

// Usage: <value> [<value>...]
func TestLength(t *testing.T) {
	val, err := length([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid length")
	}
	val, err = length([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid length")
		t.Log(val)
	}

	val, err = length([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid length")
	}
	val, err = length([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid length")
		t.Log(val)
	}

	val, err = length([]string{"a", "4", "c"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid length")
	}
	val, err = length([]string{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid length")
		t.Log(val)
	}

	val, err = length([]interface{}{true, true, false})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid length")
		t.Log(val)
	}
}

// Usage: <value> [<value>...]
func TestMax(t *testing.T) {
	val, err := mathMax([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid max")
	}
	val, err = mathMax([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid max")
		t.Log(val)
	}

	val, err = mathMax([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid max")
	}
	val, err = mathMax([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid max")
		t.Log(val)
	}

	val, err = mathMax([]string{"a", "4", "c"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "c" {
		t.Error("invalid max")
	}
	val, err = mathMax([]string{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "" {
		t.Error("invalid max")
		t.Log(val)
	}

	val, err = mathMax([]interface{}{true, true, false})
	if err == nil {
		t.Error("invalid type should return an error")
		t.FailNow()
	}
}

// Usage: <value> [<value>...]
func TestMin(t *testing.T) {
	val, err := mathMin([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 1.0 {
		t.Error("invalid min")
	}
	val, err = mathMin([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid min")
		t.Log(val)
	}

	val, err = mathMin([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 1.0 {
		t.Error("invalid min")
	}
	val, err = mathMin([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid min")
		t.Log(val)
	}

	val, err = mathMin([]string{"a", "4", "c"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "4" {
		t.Error("invalid min")
	}
	val, err = mathMin([]string{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "" {
		t.Error("invalid min")
		t.Log(val)
	}

	val, err = mathMin([]interface{}{true, true, false})
	if err == nil {
		t.Error("invalid type should return an error")
		t.FailNow()
	}
}

// Usage: <value> [<value>...]
func TestSum(t *testing.T) {
	val, err := sum([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 6.0 {
		t.Error("invalid sum")
	}
	val, err = sum([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid sum")
		t.Log(val)
	}

	val, err = sum([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 6.0 {
		t.Error("invalid sum")
	}
	val, err = sum([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid sum")
		t.Log(val)
	}

	val, err = sum([]string{"a", "4", "c"})
	if err == nil {
		t.Error("sum string is not supported")
		t.FailNow()
	}
	val, err = sum([]string{})
	if err == nil {
		t.Error("sum string is not supported")
		t.FailNow()
	}

	val, err = sum([]interface{}{true, true, false})
	if err == nil {
		t.Error("invalid type should return an error")
		t.FailNow()
	}
}

// Usage: <value> [<value>...]
func TestAverage(t *testing.T) {
	val, err := average([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 2.0 {
		t.Error("invalid average")
	}
	val, err = average([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid average")
		t.Log(val)
	}

	val, err = average([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 2.0 {
		t.Error("invalid average")
	}
	val, err = average([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid average")
		t.Log(val)
	}

	val, err = average([]string{"a", "4", "c"})
	if err == nil {
		t.Error("average string is not supported")
		t.FailNow()
	}
	val, err = average([]string{})
	if err == nil {
		t.Error("average string is not supported")
		t.FailNow()
	}

	val, err = average([]interface{}{true, true, false})
	if err == nil {
		t.Error("invalid type should return an error")
		t.FailNow()
	}
}

func TestRoundToDecimal(t *testing.T) {
	testCases := []struct {
		name     string
		input    float64
		decimals int
		want     float64
	}{
		{"Round to 2 decimals", 3.14159, 2, 3.14},
		{"Round to 0 decimals", 2.5, 0, 3},
		{"Round to 3 decimals", 3.14159, 3, 3.142},
		{"Round to 1 decimal", 1.25, 1, 1.3},
		{"Round negative number to 2 decimals", -3.14159, 2, -3.14},
		{"No rounding needed", 2.00, 2, 2.00},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := roundToDecimal(tc.input, tc.decimals)
			if err != nil {
				t.Fatalf("roundToDecimal(%v, %d) returned an unexpected error: %v", tc.input, tc.decimals, err)
			}
			if got != tc.want {
				t.Errorf("roundToDecimal(%v, %d) = %v, want %v", tc.input, tc.decimals, got, tc.want)
			}
		})
	}
}

func TestSafeDivide(t *testing.T) {
	testCases := []struct {
		name      string
		divider   interface{}
		dividende interface{}
		want      float64
	}{
		{"with int : 10 / 2", 10, 2, 5.0},
		{"with int : 0 / 0", 0, 0, 0},
		{"with float : 10 / 2", 10.0, 2.0, 5.0},
		{"with big values : 1e50 / 2", 1e50, 2.0, 1e50 / 2},
		{"missing values", nil, nil, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := safeDivide(tc.divider, tc.dividende)
			if got != tc.want {
				t.Errorf("safeDivide(%v, %d) = %v, want %v", tc.divider, tc.dividende, got, tc.want)
			}
		})
	}
}

func TestNumberWithoutExponent(t *testing.T) {
	testCases := []struct {
		name  string
		value interface{}
		want  string
		err   error
	}{
		{"integer value", 100000, "100000", nil},
		{"integer value with exponent", 1e7, "10000000", nil},
		{"float value", 18767.4868, "18767.4868", nil},
		{"float value with exponent", 4e-05, "0.00004", nil},
		{"float value with exponent as string", "4e-05", "0.00004", nil},
		{"not a number", "this is not a number", "this is not a number", fmt.Errorf("Unable to parse this value as a float : this is not a number")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := numberWithoutExponent(tc.value)
			if got != tc.want {
				t.Errorf("NumberWithoutExponent(%v) = %v, want %v", tc.value, got, tc.want)
			}
			if err != nil && tc.err != nil && err.Error() != tc.err.Error() {
				t.Errorf("NumberWithoutExponent(%v) returned err = %v wanted error = %v", tc.value, err, tc.err)
			}

		})
	}
}

func TestAbsoluteValue(t *testing.T) {
	testCases := []struct {
		name  string
		value interface{}
		want  float64
		err   error
	}{
		{"integer value", 100000, 100000, nil},
		{"negative integer value", -100000, 100000, nil},
		{"float value as a string", "18767.4868", 18767.4868, nil},
		{"negative float value as a string", "-18767.4868", 18767.4868, nil},
		{"-zero", "-0", 0, nil},
		{"nil value", nil, 0, fmt.Errorf("Unable to parse this value as a float : <nil>")},
		{"not a number", "this is not a number", 0, fmt.Errorf("Unable to parse this value as a float : this is not a number")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := absoluteValue(tc.value)
			if got != tc.want {
				t.Errorf("absoluteValue(%v) = %v, want %v", tc.value, got, tc.want)
			}
			if err != nil && tc.err != nil && err.Error() != tc.err.Error() {
				t.Errorf("absoluteValue(%v) returned err = %v wanted error = %v", tc.value, err, tc.err)
			}

		})
	}
}
