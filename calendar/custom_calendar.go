package calendar

import (
	"sort"
	"time"
)

var (
	// Day is the equivalent of 24 hours
	Day = time.Duration(24) * time.Hour
)

// Entry is a single day entry in a calendar
type Entry struct {
	ID         time.Time `json:"id"`
	WorkingDay bool      `json:"workingDay"` // Jour ouvrable
	WorkedDay  bool      `json:"workedDay"`  // Jour ouvré
}

// NewEntry returns a new calendar entry
func NewEntry(t time.Time, workingDay bool, workedDay bool) Entry {
	return Entry{ID: t, WorkingDay: workingDay, WorkedDay: workedDay}
}

// CustomCalendar is a standard calendar containing date entries with specific behavior
type CustomCalendar struct {
	name       string
	entryIndex map[time.Time]Entry
	entries    []Entry
}

// NewCustomCalendar returns a new calendar instance
func NewCustomCalendar(name string) *CustomCalendar {
	return &CustomCalendar{
		name:       name,
		entryIndex: make(map[time.Time]Entry),
		entries:    make([]Entry, 0),
	}
}

// GetName returns the calendar name
func (calendar *CustomCalendar) GetName() string {
	return calendar.name
}

// AddEntries add a new entry in the calendar
func (calendar *CustomCalendar) AddEntries(entries ...Entry) {
	for _, entry := range entries {
		calendar.entryIndex[entry.ID] = entry
	}
	entriesSlice := make([]Entry, 0)
	for _, v := range calendar.entryIndex {
		entriesSlice = append(entriesSlice, v)
	}
	sort.SliceStable(entriesSlice, func(i, j int) bool {
		return entriesSlice[i].ID.After(entriesSlice[j].ID)
	})
	calendar.entries = entriesSlice
}

// Add returns the time t+d, taking into account working days
func (calendar *CustomCalendar) Add(t time.Time, d time.Duration) time.Time {
	durationGap := time.Duration(0)
	for _, entry := range calendar.entries {
		if entry.WorkingDay {
			continue // skip working day entries
		}
		if entry.ID.Equal(t.Truncate(Day)) {
			durationGap += t.Sub(t.Truncate(Day))
		} else {
			checkDate := t.Add(d).Add(-1 * durationGap).Truncate(Day)
			if entry.ID.Before(t) && (entry.ID.Equal(checkDate) || entry.ID.After(checkDate)) {
				durationGap += 1 * Day
			}
		}
	}

	return t.Add(d).Add(-1 * time.Duration(durationGap))
}

// Sub returns the duration t-u, taking into account working days
func (calendar *CustomCalendar) Sub(t time.Time, u time.Time) time.Duration {
	durationGap := time.Duration(0)
	for _, entry := range calendar.entries {
		if entry.WorkingDay {
			continue // skip working day entries
		}
		if entry.ID.Equal(u.Truncate(Day)) {
			durationGap += u.Sub(u.Truncate(Day)) // Remove starting day time (because the last day is a working day)
		} else if entry.ID.Before(u) {
			if entry.ID.Equal(t) || entry.ID.After(t) {
				durationGap += 1 * Day
			} else if entry.ID.Equal(t.Truncate(Day)) {
				durationGap += t.Add(Day).Truncate(Day).Sub(t) // Remove remaning day time (because the first day is a working day)
			}
		}
	}

	return u.Sub(t.Add(durationGap))
}
