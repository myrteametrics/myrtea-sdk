package expression

import (
	"fmt"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v5/utils"
)

var (
	dateLayouts = [...]string{
		utils.TimeLayout,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.Kitchen,
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02",                         // RFC 3339
		"2006-01-02 15:04",                   // RFC 3339 with minutes
		"2006-01-02 15:04:05",                // RFC 3339 with seconds
		"2006-01-02 15:04:05-07:00",          // RFC 3339 with seconds and timezone
		"2006-01-02T15Z0700",                 // ISO8601 with hour
		"2006-01-02T15:04Z0700",              // ISO8601 with minutes
		"2006-01-02T15:04:05Z0700",           // ISO8601 with seconds
		"2006-01-02T15:04:05.999999999Z0700", // ISO8601 with nanoseconds
		"2006-01-02T15:04:05.999999999",      // ISO8601 with nanoseconds
	}
)

func parseDateAllFormat(s string) (time.Time, string, error) {
	for _, format := range dateLayouts {
		t, err := time.ParseInLocation(format, s, time.UTC)
		if err == nil {
			return t, format, nil
		}
	}
	return time.Time{}, "", fmt.Errorf("could not parse %s", s)
}
