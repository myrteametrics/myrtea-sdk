package utils

import (
	"time"
)

// TimeLayout is the myrtea default time layout
const TimeLayout = "2006-01-02T15:04:05.000"

// TimeLayoutWithZone is TimeLayout followed by the UTC offset of the date:
// "Z" for UTC, "+hh:mm" / "-hh:mm" otherwise
const TimeLayoutWithZone = TimeLayout + "Z07:00"

// GetTime return now time formated to elasticsearch standard format
func GetTime(t time.Time) string {
	return t.Format(TimeLayout)
}

// GetTimeZone return timezone of the input time
func GetTimeZone(t time.Time) string {
	_, offset := t.Zone()
	if offset == 0 {
		return ""
	}
	return t.Format("-07:00")
}

// GetBeginningOfDay returns the midnight starting the day of t, in the location of t,
// formatted with TimeLayoutWithZone
func GetBeginningOfDay(t time.Time) string {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(TimeLayoutWithZone)
}

// GetBeginningOfMonth returns the midnight starting the month of t, in the location of t,
// formatted with TimeLayoutWithZone
func GetBeginningOfMonth(t time.Time) string {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(TimeLayoutWithZone)
}

// GetBeginningOfYear returns the midnight starting the year of t, in the location of t,
// formatted with TimeLayoutWithZone
func GetBeginningOfYear(t time.Time) string {
	return time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, t.Location()).Format(TimeLayoutWithZone)
}

// GetEndOfDay returns the midnight ending the day of t, in the location of t,
// formatted with TimeLayoutWithZone
func GetEndOfDay(t time.Time) string {
	return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location()).Format(TimeLayoutWithZone)
}

// GetEndOfMonth returns the midnight ending the month of t, in the location of t,
// formatted with TimeLayoutWithZone
func GetEndOfMonth(t time.Time) string {
	return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location()).Format(TimeLayoutWithZone)
}

// GetEndOfYear returns the midnight ending the year of t, in the location of t,
// formatted with TimeLayoutWithZone
func GetEndOfYear(t time.Time) string {
	return time.Date(t.Year()+1, time.January, 1, 0, 0, 0, 0, t.Location()).Format(TimeLayoutWithZone)
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
