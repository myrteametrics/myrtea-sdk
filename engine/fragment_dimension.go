package engine

import (
	"errors"
	"strings"
)

// DimensionFragment is a fragment type which contains a single dimension definition
type DimensionFragment struct {
	Name         string         `json:"name,omitempty"`
	Operator     DimensionToken `json:"operator"`
	Term         string         `json:"term"`
	Size         int            `json:"size,omitempty"`
	Interval     float64        `json:"interval,omitempty"`
	DateInterval string         `json:"dateinterval,omitempty"`
	TimeZone     string         `json:"timezone,omitempty"`
}

// IsValid checks if an intent fragment is valid and has no missing mandatory fields
// * Operator must not be empty (or 0 value)
// * Term must not be empty
// * Size must not be lesser than 0
// * Interval must not be lesser than 0
func (frag *DimensionFragment) IsValid() (bool, error) {
	if frag.Operator == 0 {
		return false, errors.New("Missing Operator")
	}
	if frag.Term == "" {
		return false, errors.New("Missing Term")
	}
	if frag.Size < 0 {
		return false, errors.New("Size is lower than 0")
	}
	if frag.Interval < 0 {
		return false, errors.New("Interval is lower than 0")
	}
	return true, nil
}

var dimensionMap = map[DimensionToken]func() *DimensionFragment{
	By: func() *DimensionFragment {
		return &DimensionFragment{"", By, "", 0, 0, "", ""}
	},
	Histogram: func() *DimensionFragment {
		return &DimensionFragment{"", Histogram, "", 0, 0, "", ""}
	},
	DateHistogram: func() *DimensionFragment {
		return &DimensionFragment{"", DateHistogram, "", 0, 0, "", ""}
	},
}

// GetDimensionFragment search and return a dimension fragment by it's name
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
