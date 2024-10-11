package engine

import (
	"errors"
	"strings"
	"time"
)

// DimensionFragment is a fragment type which contains a single dimension definition
type DimensionFragment struct {
	Name          string         `json:"name,omitempty"`
	Operator      DimensionToken `json:"operator"`
	Term          string         `json:"term"`
	Size          int            `json:"size,omitempty"`
	Interval      float64        `json:"interval,omitempty"`
	DateInterval  string         `json:"dateinterval,omitempty"`
	CalendarFixed bool           `json:"calendarfixed,omitempty"`
	TimeZone      string         `json:"timezone,omitempty"`
}

var calendarIntervals = map[string]bool{
	"second":  true,
	"minute":  true,
	"hour":    true,
	"day":     true,
	"week":    true,
	"month":   true,
	"quarter": true,
	"year":    true,
}

// IsValid checks if an intent fragment is valid and has no missing mandatory fields
// * Operator must not be empty (or 0 value)
// * Term must not be empty
// * Size must not be lesser than 0
// * Interval must not be lesser than 0
func (frag *DimensionFragment) IsValid() (bool, error) {
	if frag.Operator == 0 {
		return false, errors.New("missing Operator")
	}
	if frag.Term == "" {
		return false, errors.New("missing Term")
	}
	if frag.Size < 0 {
		return false, errors.New("size is lower than 0")
	}
	if frag.Interval < 0 {
		return false, errors.New("interval is lower than 0")
	}

	// If the operator is a date histogram, check if the date interval is valid
	if frag.Operator == DateHistogram && frag.DateInterval != "" { // DateInterval can be empty, since we have a default value
		if frag.CalendarFixed {
			// parse duration, and check if it is valid
			_, err := time.ParseDuration(frag.DateInterval)
			if err != nil {
				return false, errors.New("invalid date interval")
			}
		} else {
			if _, ok := calendarIntervals[frag.DateInterval]; !ok {
				return false, errors.New("invalid date interval")
			}
		}
	}

	return true, nil
}

var dimensionMap = map[DimensionToken]func() *DimensionFragment{
	By: func() *DimensionFragment {
		return &DimensionFragment{"", By, "", 0, 0, "", false, ""}
	},
	Histogram: func() *DimensionFragment {
		return &DimensionFragment{"", Histogram, "", 0, 0, "", false, ""}
	},
	DateHistogram: func() *DimensionFragment {
		return &DimensionFragment{"", DateHistogram, "", 0, 0, "", false, ""}
	},
}

// GetDimensionFragment search and return a dimension fragment by its name
func GetDimensionFragment(name string) (*DimensionFragment, error) {
	dimension := GetDimensionToken(strings.ToLower(name))
	if dimension == nil {
		return nil, errors.New("no token with name " + name)
	}
	if frag, ok := dimensionMap[*dimension]; ok {
		f := frag()
		return f, nil
	}
	return nil, errors.New("no fragment with name " + name)
}
