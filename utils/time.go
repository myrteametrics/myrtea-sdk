package utils

import (
	"time"
)

// TimeLayout is the myrtea default time layout
const TimeLayout = "2006-01-02T15:04:05.000"

// GetTime return now time formated to elasticsearch standard format
func GetTime(t time.Time) string {
	return t.Format(TimeLayout)
}

// GetTimeZone return timezone of the input time
func GetTimeZone(t time.Time) string {
	return t.Format("-07:00")
}

// GetBeginningOfDay return input time with time 00:00:00 formated to elasticsearch standard format
func GetBeginningOfDay(t time.Time) string {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(TimeLayout)
}

// GetBeginningOfMonth beginning of month
func GetBeginningOfMonth(t time.Time) string {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(TimeLayout)
}

// GetBeginningOfYear beginning of year
func GetBeginningOfYear(t time.Time) string {
	return time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, t.Location()).Format(TimeLayout)
}

// GetEndOfDay return input time with time 00:00:00 formated to elasticsearch standard format
func GetEndOfDay(t time.Time) string {
	return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location()).Format(TimeLayout)
}

// GetEndOfMonth beginning of month
func GetEndOfMonth(t time.Time) string {
	return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location()).Format(TimeLayout)
}

// GetEndOfYear beginning of year
func GetEndOfYear(t time.Time) string {
	return time.Date(t.Year()+1, time.January, 1, 0, 0, 0, 0, t.Location()).Format(TimeLayout)
}

// GetDailyRange returns a range of time for the current day (from 00:00:00 to now)
// with 1 value per hour
func GetDailyRange(t time.Time) []string {
	timeRange := []string{t.Format(TimeLayout)}
	for i := t.Hour(); i > 0; i-- {
		t = t.Add(-1 * time.Hour)
		timeRange = append(timeRange, t.Format(TimeLayout))
	}
	return timeRange
}
