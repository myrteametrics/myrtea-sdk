package expression

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// IsInvalidNumber return true if the input interface is a not valid number
func IsInvalidNumber(input interface{}) bool {
	switch r := input.(type) {
	case float64:
		if math.IsInf(r, 1) || math.IsNaN(r) || math.IsInf(r, -1) {
			return true
		}
	}
	return false
}

// AssertEqual checks if values are equal
func AssertEqual(t *testing.T, a interface{}, b interface{}, message ...string) {
	if a == b {
		return
	}

	var errorMessage string
	if len(message) != 0 {
		errorMessage = strings.Join(message, " ") + "\n"
	}

	t.Helper()
	t.Errorf("%sReceived %v (type %v), expected %v (type %v)", errorMessage, a, reflect.TypeOf(a), b, reflect.TypeOf(b))
	t.FailNow()
}

// AssertNotEqual checks if values are not equal
func AssertNotEqual(t *testing.T, a interface{}, b interface{}, message ...string) {
	if a != b {
		return
	}

	var errorMessage string
	if len(message) != 0 {
		errorMessage = strings.Join(message, " ") + "\n"
	}

	t.Helper()
	t.Errorf("%sReceived %v (type %v), expected != %v (type %v)", errorMessage, a, reflect.TypeOf(a), b, reflect.TypeOf(b))
	t.FailNow()
}

func convertAsFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return float64(v), nil
	case string:
		value, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	default:
		return 0, errors.New("Unable to convert this type as a float64")
	}
}

func convertAsBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return bool(v), nil
	case string:
		value, err := strconv.ParseBool(v)
		if err != nil {
			return false, err
		}
		return value, nil
	default:
		return false, errors.New("Unable to convert this type as a bool")
	}
}

type timePrecision int

const (
	precisionHour timePrecision = iota
	precisionMinute
	precisionSecond
)

// parseHMSStrict parses only "HHh", "HHhMMm", "HHhMMmSSs".
func parseHMSStrict(s string) (hh, mm, ss int, prec timePrecision, err error) {
	raw := strings.TrimSpace(strings.ToLower(s))
	// Strict HMS patterns:
	reH := regexp.MustCompile(`^(\d{1,2})h$`)
	reHM := regexp.MustCompile(`^(\d{1,2})h(\d{1,2})m$`)
	reHMS := regexp.MustCompile(`^(\d{1,2})h(\d{1,2})m(\d{1,2})s$`)

	if m := reHMS.FindStringSubmatch(raw); m != nil {
		hh, err = atoiBound(m[1], 0, 23)
		if err != nil {
			return
		}
		mm, err = atoiBound(m[2], 0, 59)
		if err != nil {
			return
		}
		ss, err = atoiBound(m[3], 0, 59)
		if err != nil {
			return
		}
		return hh, mm, ss, precisionSecond, nil
	}
	if m := reHM.FindStringSubmatch(raw); m != nil {
		hh, err = atoiBound(m[1], 0, 23)
		if err != nil {
			return
		}
		mm, err = atoiBound(m[2], 0, 59)
		if err != nil {
			return
		}
		ss = 0
		return hh, mm, ss, precisionMinute, nil
	}
	if m := reH.FindStringSubmatch(raw); m != nil {
		hh, err = atoiBound(m[1], 0, 23)
		if err != nil {
			return
		}
		mm, ss = 0, 0
		return hh, mm, ss, precisionHour, nil
	}
	return 0, 0, 0, precisionHour, fmt.Errorf("invalid send_time %q: expected HHh, HHhMMm, or HHhMMmSSs", s)
}

func atoiBound(s string, min, max int) (int, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", s)
	}
	if v < min || v > max {
		return 0, fmt.Errorf("value %d out of range [%d..%d]", v, min, max)
	}
	return v, nil
}

// computeTargetUTC_UTCplus:
// - tzSpec == "auto": Europe/Paris if available; else fallback to +1h/+2h by date.
// - tzSpec explicit like "2h", "+2h", "1h", "+1h", "-3h": local = UTC + tz => UTC = local - tz.
func computeTargetUTC_UTCplus(nowUTC, startUTC time.Time, hh, mm, ss int, tzSpec string) (time.Time, error) {
	if strings.EqualFold(strings.TrimSpace(tzSpec), "auto") {
		// Prefer real tz database (Europe/Paris)
		if loc, err := time.LoadLocation("Europe/Paris"); err == nil && loc != nil {
			nowLocal := nowUTC.In(loc)
			targetLocal := time.Date(nowLocal.Year(), nowLocal.Month(), nowLocal.Day(), hh, mm, ss, 0, loc)
			return targetLocal.UTC(), nil
		}
		// Fallback: EU DST rule → UTC+1 (winter) or UTC+2 (summer)
		utcPlus := utcPlusParis(nowUTC) // +1h or +2h
		// UTC target = (00:00Z + HH:MM:SS) - utcPlus
		return startUTC.Add(time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second - utcPlus), nil
	}

	// Explicit "UTC+X" semantics
	utcPlus, err := parseUTCPlus(tzSpec)
	if err != nil {
		return time.Time{}, fmt.Errorf("tz parse error: %v", err)
	}
	return startUTC.Add(time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second - utcPlus), nil
}

// parseUTCPlus: "2h", "+2h", "1h", "+1h", "-3h", "+90m", etc.
// If no sign, '+' is assumed.
func parseUTCPlus(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty tz")
	}
	if !(strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-")) {
		s = "+" + s
	}
	return time.ParseDuration(s)
}

// utcPlusParis: returns +2h in summer (CEST), +1h in winter (CET).
func utcPlusParis(dUTC time.Time) time.Duration {
	year := dUTC.Year()
	lastSunMar := lastSundayUTC(year, time.March)
	lastSunOct := lastSundayUTC(year, time.October)
	startDST := time.Date(year, time.March, lastSunMar, 1, 0, 0, 0, time.UTC) // 01:00 UTC
	endDST := time.Date(year, time.October, lastSunOct, 1, 0, 0, 0, time.UTC)

	if !dUTC.Before(startDST) && dUTC.Before(endDST) {
		return 2 * time.Hour // summer
	}
	return time.Hour // winter
}

func lastSundayUTC(year int, month time.Month) int {
	firstNext := time.Date(year, month+1, 1, 12, 0, 0, 0, time.UTC)
	lastOfMonth := firstNext.Add(-24 * time.Hour)
	offset := int(lastOfMonth.Weekday()) // 0=Sunday
	return lastOfMonth.AddDate(0, 0, -offset).Day()
}
