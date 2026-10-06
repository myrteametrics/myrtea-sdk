package utils

import (
	"testing"
	"time"
)

func TestGetDailyRange(t *testing.T) {
	ti := time.Date(2020, 1, 10, 12, 35, 0, 0, time.UTC)
	r := GetDailyRange(ti)
	if len(r) != 13 {
		t.Error("invalid number of time")
		t.Log(len(r))
		t.FailNow()
	}
	if r[0] != time.Date(2020, 1, 10, 12, 35, 0, 0, time.UTC).Format(TimeLayout) {
		t.Error("invalid r[0]")
		t.Log(r[0])
		t.FailNow()
	}
	if r[12] != time.Date(2020, 1, 10, 0, 35, 0, 0, time.UTC).Format(TimeLayout) {
		t.Error("invalid r[12]")
		t.Log(r[12])
		t.FailNow()
	}
}

func TestPeriodBoundaries(t *testing.T) {
	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Fatal(err)
	}
	ti := time.Date(2026, 10, 6, 22, 30, 0, 0, time.UTC)
	tiParis := ti.In(paris) // 2026-10-07 00:30 CEST

	tests := []struct {
		name     string
		result   time.Time
		expected string
	}{
		{"BeginningOfDay", BeginningOfDay(ti), "2026-10-06T00:00:00.000Z"},
		{"BeginningOfMonth", BeginningOfMonth(ti), "2026-10-01T00:00:00.000Z"},
		{"BeginningOfYear", BeginningOfYear(ti), "2026-01-01T00:00:00.000Z"},
		{"EndOfDay", EndOfDay(ti), "2026-10-07T00:00:00.000Z"},
		{"EndOfMonth", EndOfMonth(ti), "2026-11-01T00:00:00.000Z"},
		{"EndOfYear", EndOfYear(ti), "2027-01-01T00:00:00.000Z"},
		// Boundaries are those of the location of t, with its DST rules
		{"BeginningOfDay Paris", BeginningOfDay(tiParis), "2026-10-07T00:00:00.000+02:00"},
		{"EndOfMonth Paris", EndOfMonth(tiParis), "2026-11-01T00:00:00.000+01:00"},
		{"BeginningOfYear Paris", BeginningOfYear(tiParis), "2026-01-01T00:00:00.000+01:00"},
	}
	for _, test := range tests {
		if result := test.result.Format(TimeLayoutWithZone); result != test.expected {
			t.Errorf("%s = %s, expected %s", test.name, result, test.expected)
		}
	}
}

func TestDeprecatedPeriodBoundaries(t *testing.T) {
	ti := time.Date(2026, 10, 6, 22, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		result   string
		expected string
	}{
		{"GetBeginningOfDay", GetBeginningOfDay(ti), "2026-10-06T00:00:00.000"},
		{"GetBeginningOfMonth", GetBeginningOfMonth(ti), "2026-10-01T00:00:00.000"},
		{"GetBeginningOfYear", GetBeginningOfYear(ti), "2026-01-01T00:00:00.000"},
		{"GetEndOfDay", GetEndOfDay(ti), "2026-10-07T00:00:00.000"},
		{"GetEndOfMonth", GetEndOfMonth(ti), "2026-11-01T00:00:00.000"},
		{"GetEndOfYear", GetEndOfYear(ti), "2027-01-01T00:00:00.000"},
	}
	for _, test := range tests {
		if test.result != test.expected {
			t.Errorf("%s = %s, expected %s", test.name, test.result, test.expected)
		}
	}
}
