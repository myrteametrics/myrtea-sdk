package expression

import (
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
	if err == nil {
		t.Error("invalid type should return an error")
		t.FailNow()
	}
}

// Usage: <value> [<value>...]
func TestMax(t *testing.T) {
	val, err := max([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid max")
	}
	val, err = max([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid max")
		t.Log(val)
	}

	val, err = max([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 3.0 {
		t.Error("invalid max")
	}
	val, err = max([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid max")
		t.Log(val)
	}

	val, err = max([]string{"a", "4", "c"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "c" {
		t.Error("invalid max")
	}
	val, err = max([]string{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "" {
		t.Error("invalid max")
		t.Log(val)
	}

	val, err = max([]interface{}{true, true, false})
	if err == nil {
		t.Error("invalid type should return an error")
		t.FailNow()
	}
}

// Usage: <value> [<value>...]
func TestMin(t *testing.T) {
	val, err := min([]int{2, 3, 1})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 1.0 {
		t.Error("invalid min")
	}
	val, err = min([]int{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid min")
		t.Log(val)
	}

	val, err = min([]float64{2.0, 3.0, 1.0})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 1.0 {
		t.Error("invalid min")
	}
	val, err = min([]float64{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 0.0 {
		t.Error("invalid min")
		t.Log(val)
	}

	val, err = min([]string{"a", "4", "c"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "4" {
		t.Error("invalid min")
	}
	val, err = min([]string{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != "" {
		t.Error("invalid min")
		t.Log(val)
	}

	val, err = min([]interface{}{true, true, false})
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
