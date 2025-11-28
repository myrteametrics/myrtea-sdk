package expression

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestContainsSlice(t *testing.T) {
	variables := map[string]interface{}{"a": "C"}

	result, err := Process(LangEval, `contains(["A", "B", "C", "D"], a)`, variables)
	if err != nil {
		t.Error(err)
	}
	_ = result
}

func TestGvalAppendSlice(t *testing.T) {
	variables := map[string]interface{}{"a": []interface{}{"a", "b", "c"}}

	result, err := Process(LangEval, `append("test", "test2")`, variables)
	if err != nil {
		t.Fatal(err)
	}

	if slice, ok := result.([]interface{}); !ok {
		t.Error("result is not an array")
	} else {
		if len(slice) != 2 {
			t.Fatal("result slice should have length 2")
		}
		if slice[0].(string) != "test" {
			t.Error("result slice should have the right element")
		}
		if slice[1].(string) != "test2" {
			t.Error("result slice should have the right element")
		}
	}

	result, err = Process(LangEval, `append("test", a)`, variables)
	if err != nil {
		t.Fatal(err)
	}

	if slice, ok := result.([]interface{}); !ok {
		t.Error("result is not an array")
	} else {
		if len(slice) != 4 {
			t.Fatal("result slice should have length 4")
		}
		if slice[0].(string) != "test" {
			t.Error("slice[0] should be 'test'")
		}
		if slice[1].(string) != "a" {
			t.Error("slice[1] should be 'a'")
		}
		if slice[2].(string) != "b" {
			t.Error("slice[2] should be 'b'")
		}
		if slice[3].(string) != "c" {
			t.Error("slice[3] should be 'c'")
		}
	}

}

func TestProcessArray(t *testing.T) {
	variables := map[string]interface{}{"a": []string{"a", "b", "c"}}

	result, err := Process(LangEval, "a[0]", variables)
	if err != nil {
		t.Error(err)
	}
	_ = result
}

func TestProcess(t *testing.T) {
	variables := map[string]interface{}{"a": 10}

	result, err := Process(LangEval, "a,3", variables)
	if err == nil {
		t.Error("expression should not be evaluable")
	}
	_ = result
}

func TestGetEvaluable(t *testing.T) {
	eval, err := getEvaluable(LangEval, "a,3")
	if err == nil {
		t.Error("expression should not be evaluable")
		t.FailNow()
	}

	eval, err = getEvaluable(LangEval, "a > 1")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	eval, err = getEvaluable(LangEval, "a > 1") // cache
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_ = eval
}

func TestComplexDate(t *testing.T) {
	eval, err := Process(LangEval, "startOf(calendar_add(startOf(now, \"month\"), \"-24h\"), \"month\")", GetDateKeywords(time.Date(2020, 05, 17, 12, 30, 00, 0, time.UTC)))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if eval != "2020-04-01T00:00:00.000" {
		t.Error("invalid result")
		t.FailNow()
	}
}

func TestAdvancedAddition(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 1,
			"b": 2,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 5,
	}

	eval, err := Process(LangEval, "map1 + map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 1 || result["b"].(float64) != 5 || result["c"].(float64) != 4 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 + a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 6 || result["b"].(float64) != 7 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 + 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 4 || result["b"].(float64) != 5 {
		t.Error("Result value is not as expected")
	}

}

func TestAdvancedSubtraction(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 5,
			"b": 6,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 5,
	}

	eval, err := Process(LangEval, "map1 - map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 5 || result["b"].(float64) != 3 || result["c"].(float64) != -4 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 - a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 0 || result["b"].(float64) != 1 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 - 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 2 || result["b"].(float64) != 3 {
		t.Error("Result value is not as expected")
	}

}

func TestAdvancedMultiplication(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 1,
			"b": 2,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 5,
	}

	eval, err := Process(LangEval, "map1 * map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 0 || result["b"].(float64) != 6 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 * a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 5 || result["b"].(float64) != 10 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 * 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 3 || result["b"].(float64) != 6 {
		t.Error("Result value is not as expected")
	}

}

func TestAdvancedDivision(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 6,
			"b": 9,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 3,
	}

	eval, err := Process(LangEval, "map1 / map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"] != nil || result["b"].(float64) != 3 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

	variables["map1"] = result
	eval, err = Process(LangEval, "map1 / a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})

	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"] != nil || result["b"].(float64) != 1 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 / 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"] != nil || result["b"].(float64) != 1 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

}

func TestFlatten(t *testing.T) {
	variables := map[string]interface{}{
		"fact": []interface{}{map[string]interface{}{
			"aggs": map[string]interface{}{"doc_count": map[string]interface{}{"value": 12}},
			"key":  "2022-04-11T11:00:00.000"}},
		"key":  "key",
		"path": "aggs.doc_count.value",
		"a":    2,
		"test": "4",
		"str":  "abcd",
	}

	eval, err := Process(LangEval, "flatten_fact(fact,key,path)", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	resultat, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type not as expected")
	}
	if resultat["2022-04-11T11:00:00.000"].(int) != 12 {
		t.Error("result is not as expected")
	}

	eval, err = Process(LangEval, "flatten_fact(fact,key,path)/test", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	resultat, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type not as expected")
	}
	if resultat["2022-04-11T11:00:00.000"].(float64) != 3 {
		t.Error("result is not as expected")
	}

	_, err = Process(LangEval, "flatten_fact(fact,key,path)/str", variables)
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

}

func TestDivision(t *testing.T) {
	variables := map[string]interface{}{
		"a":    "2",
		"test": 2,
	}

	eval, err := Process(LangEval, "test / a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	resultat, ok := eval.(float64)
	if !ok {
		t.Error("Result type not as expected")
	}

	if resultat != 1 {
		t.Error("result is not as expected")
	}
}

func TestEvalReplace(t *testing.T) {
	// testing replace without variables
	eval, err := Process(LangEvalString, "replace(\"Hello World!\", \"World\", \"Myrtea\")", map[string]interface{}{})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(string)
	if !ok {
		t.Error("Result type not as expected")
	}

	AssertEqual(t, result, "Hello Myrtea!")

	// testing replace with variables
	variables := map[string]interface{}{
		"a": "test",
		"b": "es",
	}
	eval, err = Process(LangEvalString, "replace(a, b, \"se\")", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, eval, "tset")

}

func TestEvalFormatDate(t *testing.T) {
	eval, err := Process(LangEvalDate, "format_date(calendar_add(now, \"-3h\"), \"2006-01-02+15:04:05\")", GetDateKeywords(time.Now()))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(string)
	if !ok {
		t.Error("Result type not as expected")
	}

	regex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}\+\d{2}:\d{2}:\d{2}$`)
	if !regex.MatchString(result) {
		t.FailNow()
	}

	_, err = time.Parse("2006-01-02+15:04:05", result)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

}

func TestEvalGetFormattedDuration(t *testing.T) {
	testCases := []struct {
		name           string
		expression     string
		variables      map[string]interface{}
		expectedResult string
	}{
		{
			name:           "convert milliseconds",
			expression:     "get_formatted_duration(43100030, \"ms\", \"{h} Hours {m} Minutes {s} Seconds\", \"\", false, false)",
			variables:      map[string]interface{}{},
			expectedResult: "11 Hours 58 Minutes 20 Seconds",
		},
		{
			name:           "invalid unit with print 0 values",
			expression:     "get_formatted_duration(a, \"test\", \"{ms} ms\", \"\", false, true)",
			variables:      map[string]interface{}{"a": 1000},
			expectedResult: "0 ms",
		},
		{
			name:           "valid duration as a string",
			expression:     "get_formatted_duration(a, \"ms\", \"{ms} ms\", \"\", false, true)",
			variables:      map[string]interface{}{"a": "1000"},
			expectedResult: "1000 ms",
		},
		{
			name:           "invalid type",
			expression:     "get_formatted_duration(a, \"test\", 100, 0, 1, 1)",
			variables:      map[string]interface{}{"a": "test"},
			expectedResult: "error parsing duration, value given is test, of type string",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Process(LangEval, tc.expression, tc.variables)
			if err != nil {
				t.Error(err)
			}

			if result != tc.expectedResult {
				t.Errorf("Result value is not as expected: got \"%v\", want \"%v\"", result, tc.expectedResult)
			}
		})
	}

}

func TestEvalGetValueForCurrentDay(t *testing.T) {
	// testing replace without variables
	eval, err := Process(LangEval, "get_value_current_day([1,2,3,4,5,6,7], "+
		"[\"monday\", \"tuesday\", \"wednesday\", \"thursday\", \"friday\", \"saturday\", \"sunday\"], -1)",
		GetDateKeywords(time.Now()))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	AssertNotEqual(t, eval, -1)

	currentDay := strings.ToLower(time.Now().Weekday().String())
	expected := []float64{1, 2, 3, 4, 5, 6, 7}

	for i, day := range GetValidDayNames() {
		if currentDay == day {
			AssertEqual(t, eval, expected[i])
			break
		}
	}

}

func TestEvalRoundToDecimal(t *testing.T) {
	testCases := []struct {
		name           string
		expression     string
		variables      map[string]interface{}
		expectedResult float64
	}{
		{
			name:           "Round float to 2 decimal places",
			expression:     "roundToDecimal(123.4567, 2)",
			variables:      map[string]interface{}{},
			expectedResult: 123.46,
		},
		{
			name:           "Round float to 0 decimal places",
			expression:     "roundToDecimal(123.4567, 0)",
			variables:      map[string]interface{}{},
			expectedResult: 123,
		},
		{
			name:           "Round variable to 1 decimal place",
			expression:     "roundToDecimal(a, 1)",
			variables:      map[string]interface{}{"a": 123.4567},
			expectedResult: 123.5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Process(LangEval, tc.expression, tc.variables)
			if err != nil {
				t.Error(err)
			}

			roundedResult, ok := result.(float64)
			if !ok {
				t.Errorf("Result type is not as expected: got %T, want float64", result)
			}
			if roundedResult != tc.expectedResult {
				t.Errorf("Result value is not as expected: got %v, want %v", roundedResult, tc.expectedResult)
			}
		})
	}

}

func TestEvalSafeDivide(t *testing.T) {
	testCases := []struct {
		name           string
		expression     string
		variables      map[string]interface{}
		expectedResult float64
	}{
		{
			name:           "10 / 2 with integers",
			expression:     "safeDivide(10, 2)",
			variables:      map[string]interface{}{},
			expectedResult: 5.0,
		},
		{
			name:           "nil",
			expression:     "safeDivide(a, b)",
			variables:      map[string]interface{}{},
			expectedResult: 0,
		},
		{
			name:           "Round variable to 1 decimal place",
			expression:     "safeDivide(a, 1)",
			variables:      map[string]interface{}{"a": 123.4567},
			expectedResult: 123.4567,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Process(LangEval, tc.expression, tc.variables)
			if err != nil {
				t.Error(err)
			}

			if result != tc.expectedResult {
				t.Errorf("Result value is not as expected: got %v, want %v", result, tc.expectedResult)
			}
		})
	}

}

func TestEvalNumberWithoutExponent(t *testing.T) {
	testCases := []struct {
		name           string
		expression     string
		variables      map[string]interface{}
		expectedResult interface{}
		expectedError  error
	}{
		{
			name:           "remove exponent in 4e-05",
			expression:     "numberWithoutExponent(4e-05)",
			variables:      map[string]interface{}{},
			expectedResult: "0.00004",
			expectedError:  nil,
		},
		{
			name:           "test on integer with exponent",
			expression:     "numberWithoutExponent(1e6)",
			variables:      map[string]interface{}{},
			expectedResult: "1000000",
			expectedError:  nil,
		},
		{
			name:           "test on string number with exponent",
			expression:     "numberWithoutExponent(\"1e6\")",
			variables:      map[string]interface{}{},
			expectedResult: "1000000",
			expectedError:  nil,
		},
		{
			name:           "test on not a number",
			expression:     "numberWithoutExponent(\"this is a test\")",
			variables:      map[string]interface{}{},
			expectedResult: nil,
			expectedError:  fmt.Errorf("Unable to parse this value as a float : this is a test"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Process(LangEval, tc.expression, tc.variables)
			if err != nil && errors.Is(err, tc.expectedError) {
				t.Errorf("Result error is not as expected: got %v, want %v", err, tc.expectedError)
			}

			if result != tc.expectedResult {
				t.Errorf("Result value is not as expected: got %v, want %v", result, tc.expectedResult)
			}
		})
	}

}

func TestEvalAbsoluteValue(t *testing.T) {
	testCases := []struct {
		name           string
		expression     string
		variables      map[string]interface{}
		expectedResult interface{}
		expectedError  error
	}{
		{
			name:           "integer",
			expression:     "abs(100)",
			variables:      map[string]interface{}{},
			expectedResult: 100.0,
			expectedError:  nil,
		},
		{
			name:           "negative integer",
			expression:     "abs(-100)",
			variables:      map[string]interface{}{},
			expectedResult: 100.0,
			expectedError:  nil,
		},
		{
			name:           "string negative float",
			expression:     "abs(\"-100.12\")",
			variables:      map[string]interface{}{},
			expectedResult: 100.12,
			expectedError:  nil,
		},
		{
			name:           "test on not a number",
			expression:     "abs(\"this is a test\")",
			variables:      map[string]interface{}{},
			expectedResult: nil,
			expectedError:  fmt.Errorf("Unable to parse this value as a float : this is a test"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Process(LangEval, tc.expression, tc.variables)
			if err != nil && errors.Is(err, tc.expectedError) {
				t.Errorf("Result error is not as expected: got %v, want %v", err, tc.expectedError)
			}

			if result != tc.expectedResult {
				t.Errorf("Result value is not as expected: got %v, want %v", result, tc.expectedResult)
			}
		})
	}

}

// helper for running an expression and asserting bool result
func runBoolExpr(t *testing.T, name, expr string, want bool) {
	t.Helper()
	got, err := Process(LangEval, expr, map[string]interface{}{})
	if err != nil {
		t.Fatalf("%s: eval error: %v (expr=%s)", name, err, expr)
	}
	b, ok := got.(bool)
	if !ok {
		t.Fatalf("%s: result is not bool (got %T) (expr=%s)", name, got, expr)
	}
	if b != want {
		t.Errorf("%s: got %v, want %v (expr=%s)", name, b, want, expr)
	}
}

func TestOnceTodayAtHour_DynamicPrecision_WithExplicitTZ_CET(t *testing.T) {
	// tz = "1h"  ⇒ local = UTC + 1h (CET, winter)
	// "23h" local ⇒ 22:xx UTC
	// "23h30m" local ⇒ 22:30:xx UTC
	// "23h30m30s" local ⇒ 22:30:30 UTC

	// Hour precision: "23h" -> HH must match
	runBoolExpr(t,
		"Hour match (CET)",
		`once_today_at_hour("2025-01-15T22:15:05.000", "23h", "1h")`,
		true,
	)
	runBoolExpr(t,
		"Hour mismatch (CET)",
		`once_today_at_hour("2025-01-15T21:59:59.000", "23h", "1h")`,
		false,
	)

	// Minute precision: "23h30m" -> HH:MM must match (seconds ignored)
	runBoolExpr(t,
		"Minute match (CET)",
		`once_today_at_hour("2025-01-15T22:30:59.000", "23h30m", "1h")`,
		true,
	)
	runBoolExpr(t,
		"Minute mismatch (CET)",
		`once_today_at_hour("2025-01-15T22:31:00.000", "23h30m", "1h")`,
		false,
	)

	// Second precision: "23h30m30s" -> HH:MM:SS must match
	runBoolExpr(t,
		"Second match (CET)",
		`once_today_at_hour("2025-01-15T22:30:30.000", "23h30m30s", "1h")`,
		true,
	)
	runBoolExpr(t,
		"Second mismatch (CET)",
		`once_today_at_hour("2025-01-15T22:30:31.000", "23h30m30s", "1h")`,
		false,
	)
}

func TestBuildIndexNames_CurrentMonth(t *testing.T) {
	result, err := Process(LangEval, `generate_time_range_indexes("myrtea-ncu-YYYY.MM")`, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	now := time.Now().Format("2006.01")
	expected := "myrtea-ncu-" + now

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestBuildIndexNames_MonthRangeForward(t *testing.T) {
	result, err := Process(LangEval, `generate_time_range_indexes("myrtea-ncu-YYYY.MM", 2)`, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	now := time.Now()
	expected := []string{
		"myrtea-ncu-" + now.Format("2006.01"),
		"myrtea-ncu-" + now.AddDate(0, 1, 0).Format("2006.01"),
		"myrtea-ncu-" + now.AddDate(0, 2, 0).Format("2006.01"),
	}

	expectedStr := strings.Join(expected, ",")
	if result != expectedStr {
		t.Errorf("Expected %s, got %s", expectedStr, result)
	}
}

func TestBuildIndexNames_MonthRangeBackward(t *testing.T) {
	result, err := Process(LangEval, `generate_time_range_indexes("myrtea-ncu-YYYY-MM",-2)`, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	now := time.Now()

	expected := []string{
		"myrtea-ncu-" + now.AddDate(0, -2, 0).Format("2006-01"),
		"myrtea-ncu-" + now.AddDate(0, -1, 0).Format("2006-01"),
		"myrtea-ncu-" + now.Format("2006-01"),
	}

	expectedStr := strings.Join(expected, ",")
	if result != expectedStr {
		t.Errorf("Expected %s, got %s", expectedStr, result)
	}
}

func TestBuildIndexNames_DailyRange(t *testing.T) {
	result, err := Process(LangEval, `generate_time_range_indexes("myrtea-YYYY.MM.DD", -2)`, map[string]interface{}{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	now := time.Now()
	expected := []string{
		"myrtea-" + now.AddDate(0, 0, -2).Format("2006.01.02"),
		"myrtea-" + now.AddDate(0, 0, -1).Format("2006.01.02"),
		"myrtea-" + now.Format("2006.01.02"),
	}

	expectedStr := strings.Join(expected, ",")
	if result != expectedStr {
		t.Errorf("Expected %s, got %s", expectedStr, result)
	}
}

func TestBuildIndexNames_InvalidTemplate(t *testing.T) {
	_, err := Process(LangEval, `generate_time_range_indexes("myrtea-ncu")`, map[string]interface{}{})
	if err == nil {
		t.Fatalf("Expected error for invalid template, got nil")
	}
}

// TestGvalFilter tests the filter function integration with gval
func TestGvalFilter(t *testing.T) {
	// Test: filter with length - excluding "b"
	variables := map[string]interface{}{
		"list": []interface{}{"a", "b", "c"},
	}

	result, err := Process(LangEval, `length(exclude(list, "b"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 2 {
		t.Errorf("Expected length 2, got %v", result)
	}

	// Test: filter with length - including only "b"
	result, err = Process(LangEval, `length(filter(list, "b"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 1 {
		t.Errorf("Expected length 1, got %v", result)
	}

	// Test: filter multiple values
	result, err = Process(LangEval, `length(filter(list, "a", "c"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 2 {
		t.Errorf("Expected length 2, got %v", result)
	}

	// Test: exclude multiple values
	result, err = Process(LangEval, `length(exclude(list, "a", "c"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 1 {
		t.Errorf("Expected length 1, got %v", result)
	}
}

// TestGvalFilterWithNumbers tests filter/exclude with numeric arrays
func TestGvalFilterWithNumbers(t *testing.T) {
	variables := map[string]interface{}{
		"numbers": []interface{}{1, 2, 3, 4, 5},
	}

	// Filter only even numbers (2, 4)
	result, err := Process(LangEval, `length(filter(numbers, 2, 4))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 2 {
		t.Errorf("Expected length 2, got %v", result)
	}

	// Exclude specific numbers
	result, err = Process(LangEval, `length(exclude(numbers, 1, 5))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 3 {
		t.Errorf("Expected length 3, got %v", result)
	}
}

// TestGvalFilterChaining tests chaining filter operations
func TestGvalFilterChaining(t *testing.T) {
	variables := map[string]interface{}{
		"list": []interface{}{"a", "b", "c", "d", "e"},
	}

	// Chain: exclude "b", then exclude "d"
	result, err := Process(LangEval, `length(exclude(exclude(list, "b"), "d"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 3 {
		t.Errorf("Expected length 3 (a, c, e), got %v", result)
	}

	// Chain: filter for a,b,c then exclude b
	result, err = Process(LangEval, `length(exclude(filter(list, "a", "b", "c"), "b"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 2 {
		t.Errorf("Expected length 2 (a, c), got %v", result)
	}
}

// TestGvalFilterWithVariables tests using variables in filter expressions
func TestGvalFilterWithVariables(t *testing.T) {
	variables := map[string]interface{}{
		"list":         []interface{}{"apple", "banana", "cherry"},
		"excludeValue": "banana",
	}

	// Exclude using variable
	result, err := Process(LangEval, `length(exclude(list, excludeValue))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 2 {
		t.Errorf("Expected length 2, got %v", result)
	}

	// Filter using variable
	result, err = Process(LangEval, `length(filter(list, "apple"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 1 {
		t.Errorf("Expected length 1, got %v", result)
	}
}

// TestGvalFilterEmptyResults tests filter/exclude with no matches
func TestGvalFilterEmptyResults(t *testing.T) {
	variables := map[string]interface{}{
		"list": []interface{}{"a", "b", "c"},
	}

	// Filter with no matches
	result, err := Process(LangEval, `length(filter(list, "z"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 0 {
		t.Errorf("Expected length 0, got %v", result)
	}

	// Exclude all elements
	result, err = Process(LangEval, `length(exclude(list, "a", "b", "c"))`, variables)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.(float64) != 0 {
		t.Errorf("Expected length 0, got %v", result)
	}
}

// TestGvalSliceFunctions tests sort and join through the gval evaluator
func TestGvalSliceFunctions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		variables  map[string]interface{}
		expected   interface{}
		wantErr    bool
	}{
		{
			name:       "sort ascending with numbers",
			expression: `sort(numbers, "asc")`,
			variables: map[string]interface{}{
				"numbers": []interface{}{3, 1, 4, 1, 5, 9, 2, 6},
			},
			expected: []interface{}{1, 1, 2, 3, 4, 5, 6, 9},
			wantErr:  false,
		},
		{
			name:       "sort descending with strings",
			expression: `sort(words, "desc")`,
			variables: map[string]interface{}{
				"words": []interface{}{"zebra", "apple", "mango", "banana"},
			},
			expected: []interface{}{"zebra", "mango", "banana", "apple"},
			wantErr:  false,
		},
		{
			name:       "sort default order (ascending)",
			expression: `sort(numbers)`,
			variables: map[string]interface{}{
				"numbers": []interface{}{5, 2, 8, 1, 9},
			},
			expected: []interface{}{1, 2, 5, 8, 9},
			wantErr:  false,
		},
		{
			name:       "join strings",
			expression: `join(items, ", ")`,
			variables: map[string]interface{}{
				"items": []interface{}{"apple", "banana", "cherry"},
			},
			expected: "apple, banana, cherry",
			wantErr:  false,
		},
		{
			name:       "join numbers with dash",
			expression: `join(numbers, "-")`,
			variables: map[string]interface{}{
				"numbers": []interface{}{1, 2, 3, 4, 5},
			},
			expected: "1-2-3-4-5",
			wantErr:  false,
		},
		{
			name:       "combined: sort ascending then join",
			expression: `join(sort(values, "asc"), " | ")`,
			variables: map[string]interface{}{
				"values": []interface{}{5, 2, 8, 1, 9},
			},
			expected: "1 | 2 | 5 | 8 | 9",
			wantErr:  false,
		},
		{
			name:       "combined: filter, sort desc, and join",
			expression: `join(sort(filter(data, 1, 3, 5, 7, 9), "desc"), ", ")`,
			variables: map[string]interface{}{
				"data": []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
			expected: "9, 7, 5, 3, 1",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Process(LangEval, tt.expression, tt.variables)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Process() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Process() unexpected error: %v", err)
				return
			}

			// Compare results based on type
			switch exp := tt.expected.(type) {
			case string:
				if result != exp {
					t.Errorf("Process() result = %v, want %v", result, exp)
				}
			case []interface{}:
				resSlice, ok := result.([]interface{})
				if !ok {
					t.Errorf("Process() result is not a slice: %T", result)
					return
				}
				if len(resSlice) != len(exp) {
					t.Errorf("Process() result length = %v, want %v", len(resSlice), len(exp))
					return
				}
				for i := range resSlice {
					// Handle numeric comparison
					resNum, resIsNum := toFloat64(resSlice[i])
					expNum, expIsNum := toFloat64(exp[i])
					if resIsNum && expIsNum {
						if resNum != expNum {
							t.Errorf("Process() result[%d] = %v, want %v", i, resSlice[i], exp[i])
						}
					} else if resSlice[i] != exp[i] {
						t.Errorf("Process() result[%d] = %v, want %v", i, resSlice[i], exp[i])
					}
				}
			default:
				if result != exp {
					t.Errorf("Process() result = %v, want %v", result, exp)
				}
			}
		})
	}
}
