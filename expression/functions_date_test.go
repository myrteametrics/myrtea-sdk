package expression

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestDayOfWeek(t *testing.T) {
	val, err := dayOfWeek("2020-02-08 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 6 {
		t.Error("invalid value for saturday (6)")
		t.Error(val)
	}

	val, err = dayOfWeek("2020-02-09 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 7 {
		t.Error("invalid value for sunday (7)")
		t.Error(val)
	}

	val, err = dayOfWeek("2020-02-10 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 1 {
		t.Error("invalid value for monday (1)")
		t.Error(val)
	}
}

func TestDay(t *testing.T) {
	val, err := day("2020-02-10 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 10 {
		t.Error("invalid value for day (10)")
		t.Error(val)
	}
}

func TestMonth(t *testing.T) {
	val, err := month("2020-02-10 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 2 {
		t.Error("invalid value for month february (2)")
		t.Error(val)
	}
}

func TestYear(t *testing.T) {
	val, err := year("2020-02-10 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != 2020 {
		t.Error("invalid value for year (2020)")
		t.Error(val)
	}
}

func TestStartOf(t *testing.T) {
	val, err := startOf("2020-02-10 12:30:00+00:00", "day")
	if err != nil {
		t.Error(err)
	}
	if val != "2020-02-10T00:00:00.000" {
		t.Error(val)
	}

	val, err = startOf("2020-02-10 12:30:00+00:00", "month")
	if err != nil {
		t.Error(err)
	}
	if val != "2020-02-01T00:00:00.000" {
		t.Error(val)
	}

	val, err = startOf("2020-02-10 12:30:00+00:00", "year")
	if err != nil {
		t.Error(err)
	}
	if val != "2020-01-01T00:00:00.000" {
		t.Error(val)
	}
}

func TestEndOf(t *testing.T) {
	val, err := endOf("2020-02-10 12:30:00+00:00", "day")
	if err != nil {
		t.Error(err)
	}
	if val != "2020-02-11T00:00:00.000" {
		t.Error(val)
	}

	val, err = endOf("2020-02-10 12:30:00+00:00", "month")
	if err != nil {
		t.Error(err)
	}
	if val != "2020-03-01T00:00:00.000" {
		t.Error(val)
	}

	val, err = endOf("2020-02-10 12:30:00+00:00", "year")
	if err != nil {
		t.Error(err)
	}
	if val != "2021-01-01T00:00:00.000" {
		t.Error(val)
	}
}

func TestDateToMillis(t *testing.T) {
	d := time.Date(2020, 02, 10, 12, 30, 0, 0, time.UTC).Unix() * 1000
	val, err := dateToMillis("2020-02-10 12:30:00+00:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if val != d {
		t.Error("invalid value")
		t.Log(d)
		t.Log(val)
		t.FailNow()
	}

	val, err = dateToMillis()
	if err == nil {
		t.Error("datemillis should return an error without parameters")
		t.FailNow()
	}

	val, err = dateToMillis(12)
	if err == nil {
		t.Error("datemillis should return an error with invalid parameter type")
		t.FailNow()
	}

	val, err = dateToMillis("2020-2020-2020")
	if err == nil {
		t.Error("datemillis should return an error with invalid date format")
		t.FailNow()
	}
}

func TestDelayInDays(t *testing.T) {
	res, err := delayInDays("2020-02-08T12:30:00.000", "2020-02-10T12:30:00.000")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	d, _ := time.ParseDuration(fmt.Sprintf("%ds", res.(int64)/1000))
	if expected, _ := time.ParseDuration("48h"); d != expected {
		t.Error("invalid result")
		t.FailNow()
	}
}

func TestDelayInDaysInvalid(t *testing.T) {
	_, err := delayInDays("2020-02-08T12:30:00.000")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = delayInDays("2020-02-08T12:30:00.000", "2020-02-10T12:30:00.000", "other")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = delayInDays("2020-2020-2020", "2020-2020-2020")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = delayInDays(3, true)
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}
}

func TestAddDurationDays(t *testing.T) {
	res, err := addDurationDays("2020-02-08T12:30:00.000", "24h")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != "2020-02-09T12:30:00.000" {
		t.Error("invalid result")
		t.Log(res)
		t.FailNow()
	}
}

func TestAddDurationDaysInvalid(t *testing.T) {
	_, err := addDurationDays("2020-02-08T12:30:00.000")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = addDurationDays("2020-02-08T12:30:00.000", "3h", "other")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = addDurationDays("2020-2020-2020", "3h")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = addDurationDays("2020-02-08T12:30:00.000", "not_a_duration")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = addDurationDays(3, true)
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}
}

func TestTruncateDate(t *testing.T) {
	res, err := truncateDate("2020-02-08T12:31:00.000", "30m")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != "2020-02-08T12:30:00.000" {
		t.Error("invalid result")
		t.Logf("Result: %s, Expected: %s", res, "2020-02-08T12:30:00.000")
		t.FailNow()
	}

	res, err = truncateDate("2020-02-08T12:59:00.000", "30m")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != "2020-02-08T12:30:00.000" {
		t.Error("invalid result")
		t.Logf("Result: %s, Expected: %s", res, "2020-02-08T12:30:00.000")
		t.FailNow()
	}

	res, err = truncateDate("2020-02-08T12:30:00.000", "30m")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != "2020-02-08T12:30:00.000" {
		t.Error("invalid result")
		t.Logf("Result: %s, Expected: %s", res, "2020-02-08T12:30:00.000")
		t.FailNow()
	}

	res, err = truncateDate("2020-02-08T12:59:00.000", "15m")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != "2020-02-08T12:45:00.000" {
		t.Error("invalid result")
		t.Logf("Result: %s, Expected: %s", res, "2020-02-08T12:30:00.000")
		t.FailNow()
	}
}

func TestTruncateDateInvalid(t *testing.T) {
	_, err := truncateDate("2020-02-08T12:30:00.000")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = truncateDate("2020-02-08T12:30:00.000", "3h", "other")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = truncateDate("2020-2020-2020", "3h")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = truncateDate("2020-02-08T12:30:00.000", "not_a_duration")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = truncateDate(3, true)
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}
}

func TestExtractFromDate(t *testing.T) {
	res, err := extractFromDate("2020-02-08T12:31:18.000", "year")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 2020 {
		t.Errorf("Result: %d, Expected: %d", res, 2020)
		t.FailNow()
	}

	res, err = extractFromDate("2020-02-08T12:31:18.000", "month")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 2 {
		t.Errorf("Result: %d, Expected: %d", res, 2)
		t.FailNow()
	}

	res, err = extractFromDate("2020-02-08T12:31:18.000", "day")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 8 {
		t.Errorf("Result: %d, Expected: %d", res, 8)
		t.FailNow()
	}

	res, err = extractFromDate("2020-02-08T12:31:18.000", "dayOfMonth")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 6 {
		t.Errorf("Result: %d, Expected: %d", res, 6)
		t.FailNow()
	}

	res, err = extractFromDate("2020-02-08T12:31:18.000", "hour")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 12 {
		t.Errorf("Result: %d, Expected: %d", res, 12)
		t.FailNow()
	}

	res, err = extractFromDate("2020-02-08T12:31:18.000", "minute")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 31 {
		t.Errorf("Result: %d, Expected: %d", res, 31)
		t.FailNow()
	}

	res, err = extractFromDate("2020-02-08T12:31:18.000", "second")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != 18 {
		t.Errorf("Result: %d, Expected: %d", res, 18)
		t.FailNow()
	}
}

func TestExtractFromDateInvalid(t *testing.T) {
	_, err := extractFromDate("2020-02-08T12:31:18.000", "invalid")
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}

	_, err = extractFromDate("2020-02-08T12:31:18.000")
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}

	_, err = extractFromDate("2020-02-08T12:31", "month")
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}

	_, err = extractFromDate("month")
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}
}

func TestFormatDate(t *testing.T) {
	_, err := formatDate("blabla")
	if err == nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = formatDate("blabla", "blabla")
	if err == nil {
		t.Error("Given date should not be parsed (bad format)")
		t.FailNow()
	}
	result, err := formatDate("2023-08-04T14:57:07.923", "abcd")
	if result != "abcd" {
		t.Error("formatDate should return abcd")
		t.FailNow()
	}
	result, err = formatDate("2023-08-04T14:57:07.923", "2006-01-02")
	if result != "2023-08-04" {
		t.Error("formatDate should return 2023-08-04")
		t.FailNow()
	}
}

func TestGetValueForCurrentDay(t *testing.T) {
	_, err := getValueForCurrentDay()
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	_, err = getValueForCurrentDay([]float64{}, []interface{}{})
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	value, err := getValueForCurrentDay([]interface{}{}, []interface{}{}, -1)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_, err = getValueForCurrentDay([]interface{}{}, []interface{}{"test"}, -1)
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	value, err = getValueForCurrentDay([]interface{}{1, 2, 3, 4, 5, 6, 7}, []interface{}{"monday",
		"tuesday",
		"wednesday",
		"thursday",
		"friday",
		"saturday",
		"sunday",
	}, -1)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	AssertNotEqual(t, value, -1)
}

func TestGetFormattedDuration(t *testing.T) {
	testCases := []struct {
		name            string
		duration        interface{}
		inputUnit       interface{}
		format          interface{}
		separator       interface{}
		keepSeparator   interface{}
		printZeroValues interface{}
		want            string
	}{
		{"convert milliseconds", 43100030, "ms", "{h} Hours {m} Minutes {s} Seconds", "", false, false, "11 Hours 58 Minutes 20 Seconds"},
		{"with separator kept", 43100030, "ms", "{h} Hours, {m} Minutes, {s} Seconds", ",", true, false, "11 Hours, 58 Minutes, 20 Seconds"},
		{"with separator not kept", 43100030, "ms", "{h} Hours, {m} Minutes, {s} Seconds", ",", false, false, "11 Hours 58 Minutes 20 Seconds"},
		{"hours only", 43100030, "ms", "{h} Hours", "", false, false, "11 Hours"},
		{"convert day to minutes", 3, "d", "{m} minutes", "", false, false, "4320 minutes"},
		{"value kept in milliseconds", 1234567, "ms", "{ms} ms", "", false, false, "1234567 ms"},
		{"invalid unit without print 0 values", 1000, "test", "{ms} ms", "", false, false, ""},
		{"invalid unit with print 0 values", 1000, "test", "{ms} ms", "", false, true, "0 ms"},
		{"convert day in string to minutes", "3", "d", "{m} minutes", "", false, false, "4320 minutes"},
		{"convert day to minutes with boolean in string", "3", "d", "{m} minutes", "", "false", "false", "4320 minutes"},
		{"invalid type for duration", "1000aaa", 1, 100, 0, 1, 1, "error parsing duration, value given is 1000aaa, of type string"},
		{"invalid type for inputUnit", 1000, 1, 100, 0, 1, 1, "error parsing inputUnit, type is int"},
		{"invalid type for format", 1000, "test", 100, 0, 1, 1, "error parsing format, type is int"},
		{"invalid type for separator", 1000, "test", "", 0, 1, 1, "error parsing separator, type is int"},
		{"invalid type for keepSeparator", 1000, "test", "", "", 1, 1, "error parsing keepSeparator, type is int"},
		{"invalid type for printZeroValues", 1000, "test", "", "", true, 1, "error parsing printZeroValues, type is int"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getFormattedDuration(tc.duration, tc.inputUnit, tc.format, tc.separator, tc.keepSeparator, tc.printZeroValues)
			if got != tc.want {
				t.Errorf("getFormattedDuration() test with name \"%v\" returned \"%v\", want \"%v\"", tc.name, got, tc.want)
			}
		})
	}
}

func TestSplitFormat(t *testing.T) {
	testCases := []struct {
		name      string
		format    string
		separator string
		want      []string
	}{
		{"full format", "{h} Hours {m} Minutes {s} Seconds", "", []string{"{h} Hours ", "{m} Minutes ", "{s} Seconds"}},
		{"full format with separators", "{h} Hours, {m} Minutes, {s} Seconds", ",", []string{"{h} Hours", " {m} Minutes", " {s} Seconds"}},
		{"hours only", "{h} Hours", "", []string{"{h} Hours"}},
		{"time format", "{h}:{m}:{s}", ":", []string{"{h}", "{m}", "{s}"}},
		{"empty format", "", "", []string{""}},
		{"format with specified separator and without variables to replace", "a,a,a,a,a", ",", []string{"a", "a", "a", "a", "a"}},
		{"format with separator and without variables to replace", "a,a,a,a,a", "", []string{"a,a,a,a,a"}},
		{"invalid type", "{d} jours {g} ??", "", []string{"{d} jours ", "{g} ??"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitFormat(tc.format, tc.separator)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitFormat() test with name \"%v\" returned \"%v\", want \"%v\"", tc.name, got, tc.want)
			}
		})
	}
}

func TestInsertCalculatedUnit(t *testing.T) {
	testCases := []struct {
		name                   string
		durationMs             float64
		nextIndex, convertUnit int
		durationFormatSplited  []string
		format, regex          string
		printZeroValues        bool
		want1                  float64
		want2                  int
		want3                  []string
	}{
		{"test insert", 1000, 0, 1000, []string{"test replace"}, "test replace", "replace", false, 0, 1, []string{"test 1"}},
		{"test insert zero value not kept", 0, 0, 1, []string{"test replace"}, "test replace", "replace", false, 0, 0, []string{}},
		{"test insert zero value kept", 0, 0, 1, []string{"test replace"}, "test replace", "replace", true, 0, 1, []string{"test 0"}},
		{"test insert random number", 43100030, 0, 1000 * 60 * 60, []string{"test replace"}, "test replace", "replace", true, 43100030 - 11*1000*60*60, 1, []string{"test 11"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got1, got2, got3 :=
				insertCalculatedUnit(tc.durationMs, tc.nextIndex, tc.convertUnit,
					tc.durationFormatSplited, tc.format, tc.regex, tc.printZeroValues,
				)
			if got1 != tc.want1 || got2 != tc.want2 || !reflect.DeepEqual(got3, tc.want3) {
				t.Errorf("insertCalculatedUnit() test with name \"%v\" returned {%v, %v, %v}, want {%v, %v, %v}",
					tc.name, got1, got2, got3, tc.want1, tc.want2, tc.want3,
				)
			}
		})
	}
}

func TestAsMilliseconds(t *testing.T) {
	testCases := []struct {
		name      string
		duration  float64
		inputUnit string
		want      float64
	}{
		{"convert milliseconds as milliseconds", 1234, "ms", 1234},
		{"convert seconds as milliseconds", 1, "s", 1000},
		{"convert minutes as milliseconds", 1, "m", 60000},
		{"convert hours as milliseconds", 1, "h", 3600000},
		{"convert days as milliseconds", 1, "d", 86400000},
		{"convert milliseconds as milliseconds", 1234, "test", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := asMilliseconds(tc.duration, tc.inputUnit)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("asMilliseconds() test with name \"%v\" returned \"%v\", want \"%v\"", tc.name, got, tc.want)
			}
		})
	}
}

func runCase(t *testing.T, name, nowUTC, sendTime, tz string, want bool) {
	t.Helper()
	gotIface, err := onceTodayAtHour(nowUTC, sendTime, tz)
	if err != nil {
		t.Fatalf("%s: error: %v", name, err)
	}
	got, ok := gotIface.(bool)
	if !ok {
		t.Fatalf("%s: return type is not bool", name)
	}
	if got != want {
		t.Errorf("%s: got %v, want %v", name, got, want)
	}
}

func TestDynamicPrecision(t *testing.T) {
	// Hour precision: "23h" → HH must match (minutes/seconds ignored)
	runCase(t, "Hour match (CET)", "2025-01-15T22:15:05.000", "23h", "1h", true) // 23:xx local
	runCase(t, "Hour mismatch", "2025-01-15T21:59:59.000", "23h", "1h", false)

	// Minute precision: "23h30m" → HH:MM must match (seconds ignored)
	runCase(t, "Minute match", "2025-01-15T22:30:59.000", "23h30m", "1h", true) // 23:30 local
	runCase(t, "Minute mismatch", "2025-01-15T22:31:00.000", "23h30m", "1h", false)

	// Second precision: "23h30m30s" → HH:MM:SS must match
	runCase(t, "Second match", "2025-01-15T22:30:30.000", "23h30m30s", "1h", true) // 23:30:30 local
	runCase(t, "Second mismatch", "2025-01-15T22:30:31.000", "23h30m30s", "1h", false)
}
