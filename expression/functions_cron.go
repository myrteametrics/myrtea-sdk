package expression

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// cronThreshold returns the threshold of the highest-priority entry whose active window contains now,
// or defaultThreshold if none match.
//
// An entry is active when: lastFire <= now < lastFire + duration,
// where lastFire is the most recent cron fire time at or before now.
//
// When multiple entries match, the one with the highest priority wins.
// Entries with equal priority are resolved by order (first one wins).
//
// Usage: cron_threshold(now, defaultThreshold, entry1, entry2, ...)
//
// Important: this function evaluates cron schedules against `now` in UTC.
// Cron expressions must therefore be defined in UTC (not local time).
//
// Each entry is a map with:
//   - "cron"      (required) : cron expression string, e.g. "0 5 * * *"
//   - "duration"  (required) : duration string, e.g. "1h", "30m", "17h"
//   - "threshold" (required) : number
//   - "priority"  (optional) : number, default 0 — higher value wins on conflict
//
// Example:
//
//	cron_threshold(now, 0,
//	    {"cron": "0 5 * * *",   "duration": "1h",  "threshold": 1000},
//	    {"cron": "0 0 * * *",   "duration": "1h",  "threshold": 3000, "priority": 10},
//	    {"cron": "0 7 * * 1-6", "duration": "17h", "threshold": 1}
//	)
func cronThreshold(arguments ...interface{}) (interface{}, error) {
	if len(arguments) < 2 {
		return nil, fmt.Errorf("cron_threshold() expects at least 2 arguments: now and defaultThreshold")
	}

	// Parse now
	nowStr, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("cron_threshold() expects first argument (now) to be a string date")
	}
	now, _, err := parseDateAllFormat(nowStr)
	if err != nil {
		return nil, fmt.Errorf("cron_threshold() cannot parse now: %s", err.Error())
	}

	// Parse defaultThreshold
	defaultThreshold, ok := toFloat64(arguments[1])
	if !ok {
		return nil, fmt.Errorf("cron_threshold() expects second argument (defaultThreshold) to be a number")
	}

	var (
		bestThreshold = defaultThreshold
		bestPriority  = -1.0 // sentinel: no match yet
		matched       = false
	)

	for i, arg := range arguments[2:] {
		entry, ok := arg.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cron_threshold() entry %d must be a map", i+1)
		}

		cronExpr, err := requireString(entry, "cron", i+1)
		if err != nil {
			return nil, err
		}
		durationStr, err := requireString(entry, "duration", i+1)
		if err != nil {
			return nil, err
		}
		threshold, err := requireNumber(entry, "threshold", i+1)
		if err != nil {
			return nil, err
		}
		priority := 0.0
		if p, exists := entry["priority"]; exists {
			priority, ok = toFloat64(p)
			if !ok {
				return nil, fmt.Errorf("cron_threshold() entry %d: \"priority\" must be a number", i+1)
			}
		}

		duration, err := time.ParseDuration(durationStr)
		if err != nil {
			return nil, fmt.Errorf("cron_threshold() entry %d: cannot parse duration %q: %s", i+1, durationStr, err.Error())
		}
		if duration <= 0 {
			return nil, fmt.Errorf("cron_threshold() entry %d: duration must be positive, got %q", i+1, durationStr)
		}

		schedule, err := cronParser.Parse(cronExpr)
		if err != nil {
			return nil, fmt.Errorf("cron_threshold() entry %d: cannot parse cron %q: %s", i+1, cronExpr, err.Error())
		}

		// Find the last cron fire time at or before now by iterating forward
		// from (now - duration), then check if now falls within [lastFire, lastFire + duration).
		var lastFire time.Time
		t := schedule.Next(now.Add(-duration).Add(-time.Nanosecond))
		for !t.After(now) {
			lastFire = t
			t = schedule.Next(t)
		}
		if lastFire.IsZero() || !now.Before(lastFire.Add(duration)) {
			continue
		}

		// This entry matches — keep it if it has a higher priority than the current best,
		// or if it's the first match (matched == false).
		if !matched || priority > bestPriority {
			bestThreshold = threshold
			bestPriority = priority
			matched = true
		}
	}

	return bestThreshold, nil
}

func requireString(entry map[string]interface{}, key string, entryIndex int) (string, error) {
	v, exists := entry[key]
	if !exists {
		return "", fmt.Errorf("cron_threshold() entry %d: missing required field %q", entryIndex, key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("cron_threshold() entry %d: field %q must be a string", entryIndex, key)
	}
	return s, nil
}

func requireNumber(entry map[string]interface{}, key string, entryIndex int) (float64, error) {
	v, exists := entry[key]
	if !exists {
		return 0, fmt.Errorf("cron_threshold() entry %d: missing required field %q", entryIndex, key)
	}
	n, ok := toFloat64(v)
	if !ok {
		return 0, fmt.Errorf("cron_threshold() entry %d: field %q must be a number", entryIndex, key)
	}
	return n, nil
}
