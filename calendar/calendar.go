package calendar

import "time"

// Calendar is an interface for every implementation of calendars
// It must allow :
// * Add a duration (which can be negative) to a time.Time (taking working days into account)
// * Calculate the duration between two time.Time (taking working days into account)
type Calendar interface {
	GetName() string

	Add(time.Time, time.Duration) time.Time
	Sub(time.Time, time.Time) time.Duration
}
