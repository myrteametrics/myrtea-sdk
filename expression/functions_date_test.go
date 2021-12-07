package expression

import (
	"fmt"
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
