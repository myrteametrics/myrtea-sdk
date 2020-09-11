package expression

import (
	"fmt"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v4/calendar"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
)

// delayInOpenDays returns the duration between two date in open days/time
// Usage: <date1> <date2> [calendar_name]
func delayInOpenDays(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 && len(arguments) != 3 {
		return nil, fmt.Errorf("delayInOpenDays() expects exactly 2 or 3 string argument")
	}

	var c calendar.Calendar
	if len(arguments) == 3 {
		var found bool
		c, found = calendar.GetCalendar(arguments[2].(string))
		if !found {
			return nil, fmt.Errorf("Calend %s not found", arguments[2].(string))
		}
	} else {
		c = calendar.GetDefaultCalendar()
	}

	s1, ok1 := arguments[0].(string)
	s2, ok2 := arguments[1].(string)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("delayInOpenDays() expects exactly 2 string argument")
	}

	t1, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("delayInOpenDays() %s", err.Error())
	}
	t2, _, err := parseDateAllFormat(s2)
	if err != nil {
		return nil, fmt.Errorf("delayInOpenDays() %s", err.Error())
	}
	if t1.IsZero() || t2.IsZero() {
		return nil, fmt.Errorf("delayInOpenDays() at least one date is empty")
	}
	return c.Sub(t1, t2).Nanoseconds() / 1e6, nil
}

// addDurationOpenDays adds a duration in "open days/time" to a specific date
// Usage: <date> <duration> [calendar_name]
func addDurationOpenDays(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 && len(arguments) != 3 {
		return nil, fmt.Errorf("addDurationOpenDays() expects exactly 2 or 3 string argument")
	}

	var c calendar.Calendar
	if len(arguments) == 3 {
		var found bool
		c, found = calendar.GetCalendar(arguments[2].(string))
		if !found {
			return nil, fmt.Errorf("Calend %s not found", arguments[2].(string))
		}
	} else {
		c = calendar.GetDefaultCalendar()
	}

	s1, ok1 := arguments[0].(string)
	s2, ok2 := arguments[1].(string)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("addDurationOpenDays() expects exactly 2 string argument")
	}

	d, err := time.ParseDuration(s2)
	if err != nil {
		return nil, fmt.Errorf("addDurationOpenDays() %s", err.Error())
	}
	t, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("addDurationOpenDays() %s", err.Error())
	}
	return c.Add(t, d).Format(utils.TimeLayout), nil
}
