package calendar

import (
	"testing"
	"time"
)

func testDateArithmeticWithWorkingDays(t *testing.T, calendar Calendar, d time.Time, dur time.Duration, expected time.Time) {
	outputDate := calendar.Add(d, dur)
	if !outputDate.Equal(expected) {
		t.Error(calendar.GetName())
		t.Error(outputDate)
		t.Error(expected)
	}
}

func testDelay(t *testing.T, calendar Calendar, d1 time.Time, d2 time.Time, expected time.Duration) {
	outputDuration := calendar.Sub(d1, d2)
	if outputDuration != expected {
		t.Error(calendar.GetName())
		t.Error(outputDuration)
		t.Error(expected)
	}
}
