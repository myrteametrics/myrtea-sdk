package calendar

import (
	"fmt"
	"sync"
)

var (
	// Default is the default standard calendar
	Default = "default"

	_globalMu              sync.RWMutex
	_globalCalendars       map[string]Calendar
	_globalDefaultCalendar Calendar
)

func init() {
	_globalCalendars = make(map[string]Calendar, 0)
	_globalCalendars["default-fr"] = NewStandardCalendar("default-fr", FR)
	SetDefaultCalendar("default-fr")
}

// SetDefaultCalendar replace the current default calendar by another existing calendar
func SetDefaultCalendar(name string) error {
	_globalMu.Lock()
	defer _globalMu.Unlock()
	if c, found := _globalCalendars[name]; found {
		_globalDefaultCalendar = c
		return nil
	}
	return fmt.Errorf("Calendar %s doesn't exists", name)
}

// UpdateCalendar replace the current default calendar by another existing calendar
func UpdateCalendar(name string, calendar Calendar) {
	_globalMu.Lock()
	defer _globalMu.Unlock()
	_globalCalendars[name] = calendar
}

// GetDefaultCalendar returns the set default calendar
func GetDefaultCalendar() Calendar {
	_globalMu.RLock()
	defer _globalMu.RUnlock()
	return _globalDefaultCalendar
}

// GetCalendar returns a calendar by it's name
func GetCalendar(name string) (Calendar, bool) {
	_globalMu.RLock()
	defer _globalMu.RUnlock()
	if c, found := _globalCalendars[name]; found {
		return c, true
	}
	return nil, false
}
