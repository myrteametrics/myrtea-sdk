package calendar

import (
	"time"

	"github.com/rickar/cal"
)

// CountryCode is a standard country code (for supported calendar)
type CountryCode int

// List of supported country codes
const (
	FR CountryCode = iota + 1
)

// StandardCalendar is a wrapper for a standard calendar implementation (with pre-configured holidays)
// It only support calendar for France (FR) but can easily implements some more country code
type StandardCalendar struct {
	name string
	c    *cal.Calendar
}

// NewStandardCalendar returns a new instance of a standard calendar
func NewStandardCalendar(name string, country CountryCode) *StandardCalendar {

	c := cal.NewCalendar()
	c.Observed = cal.ObservedExact

	switch country {
	case FR:
		c.SetWorkday(time.Saturday, true)
		cal.AddFranceHolidays(c)
	}

	return &StandardCalendar{
		name: name,
		c:    c,
	}
}

// GetName returns calendar name
func (calendar *StandardCalendar) GetName() string {
	return calendar.name
}

// Add returns the time t+d, taking into account working days
func (calendar *StandardCalendar) Add(t time.Time, d time.Duration) time.Time {
	//return calendar.c.SubSkipNonWorkdays(t, -1*d)
	if d == 0 {
		return t
	}
	factor := 1
	if d < 0 {
		factor = -1
		d = -d
	}

	if !calendar.c.IsWorkday(t) {
		if d > 0 {
			t = t.Add(Day)
		}
		t = dayBegin(t)
	}

	for i := time.Duration(0); i < d; {
		if i < d {
			t = t.AddDate(0, 0, factor*1)
		}
		if calendar.c.IsWorkday(t) {
			i += Day
		}
	}

	for ; !calendar.c.IsWorkday(t); t = t.AddDate(0, 0, factor*1) {
	}

	return t
}

// Sub returns the duration t-u, taking into account working days
func (calendar *StandardCalendar) Sub(t time.Time, u time.Time) time.Duration {
	// return time.Duration(calendar.c.CountWorkdays(t, u)) * 24 * time.Hour

	t = t.UTC()
	u = u.UTC()

	factor := 1
	if t.After(u) {
		tmp := t
		t = u
		u = tmp
		factor = -1
	}

	if dayBegin(t).Equal(dayBegin(u)) {
		return u.Sub(t)
	}

	d := time.Duration(0)
	firstDayTime := dayBegin(t.Add(Day)).Sub(t)
	if calendar.c.IsWorkday(t) {
		d += firstDayTime
	}
	t = t.Add(firstDayTime)

	lastDayTime := u.Sub(dayBegin(u))
	if calendar.c.IsWorkday(u) {
		d += lastDayTime
	}
	u = u.Add(-1 * lastDayTime)

	for ; t.Before(u); t = t.AddDate(0, 0, 1) {
		if calendar.c.IsWorkday(t) {
			d += Day
		}
	}

	return time.Duration(factor) * d
}

func dayBegin(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
