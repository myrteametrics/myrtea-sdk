package expression

import (
	"time"
)

// GetDateKeywords return a list of standard date time placeholders.
// Dates keep the wall clock of t and state its UTC offset ("Z" when t is in UTC).
func GetDateKeywords(t time.Time) map[string]interface{} {
	values := map[string]interface{}{
		"now":            formatDateWithZone(t),
		"begin":          formatDateWithZone(beginningOfDay(t)), // @Deprecated - keep for compatibility
		"startofday":     formatDateWithZone(beginningOfDay(t)),
		"startofnextday": formatDateWithZone(beginningOfDay(t.Add(24 * time.Hour))),
		"startofmonth":   formatDateWithZone(beginningOfMonth(t)),
	}
	return values
}

func beginningOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func beginningOfMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
}

func beginningOfYear(t time.Time) time.Time {
	return time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, t.Location())
}

func GetValidDayNames() []string {
	return []string{"monday",
		"tuesday",
		"wednesday",
		"thursday",
		"friday",
		"saturday",
		"sunday",
	}
}
