package calendar

import (
	"testing"
	"time"
)

func TestCalendarsAdd(t *testing.T) {

	defaultFrCalendar := NewStandardCalendar("default-fr", FR)
	customCalendar := NewCustomCalendar("test-calendar")
	customCalendar.AddEntries(
		NewEntry(time.Date(2019, time.April, 28, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 1, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 5, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 8, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 12, 0, 0, 0, 0, time.UTC), false, false),
	)

	for _, calendar := range [...]Calendar{defaultFrCalendar, customCalendar} {

		d := time.Date(2019, time.May, 11, 14, 0, 0, 0, time.UTC)
		testDateArithmeticWithWorkingDays(t, calendar, d, -1*Day, d.Add(-1*Day))
		testDateArithmeticWithWorkingDays(t, calendar, d, -2*Day, d.Add(-2*Day))
		testDateArithmeticWithWorkingDays(t, calendar, d, -3*Day, d.Add(-3*Day).Add(-1*Day))   // May 8th
		testDateArithmeticWithWorkingDays(t, calendar, d, -4*Day, d.Add(-4*Day).Add(-1*Day))   // May 8th
		testDateArithmeticWithWorkingDays(t, calendar, d, -5*Day, d.Add(-5*Day).Add(-2*Day))   // May 8th + Sunday
		testDateArithmeticWithWorkingDays(t, calendar, d, -6*Day, d.Add(-6*Day).Add(-2*Day))   // May 8th + Sunday
		testDateArithmeticWithWorkingDays(t, calendar, d, -7*Day, d.Add(-7*Day).Add(-2*Day))   // May 8th + Sunday
		testDateArithmeticWithWorkingDays(t, calendar, d, -8*Day, d.Add(-8*Day).Add(-3*Day))   // May 8th + Sunday + May 1th
		testDateArithmeticWithWorkingDays(t, calendar, d, -9*Day, d.Add(-9*Day).Add(-3*Day))   // May 8th + Sunday + May 1th
		testDateArithmeticWithWorkingDays(t, calendar, d, -10*Day, d.Add(-10*Day).Add(-4*Day)) // May 8th + Sunday + May 1th + Sunday

		testDateArithmeticWithWorkingDays(t, calendar, time.Date(2019, time.May, 8, 3, 0, 0, 0, time.UTC), -1*Day, time.Date(2019, time.May, 7, 0, 0, 0, 0, time.UTC))
		testDateArithmeticWithWorkingDays(t, calendar, time.Date(2019, time.May, 8, 23, 0, 0, 0, time.UTC), -1*Day, time.Date(2019, time.May, 7, 0, 0, 0, 0, time.UTC))
		testDateArithmeticWithWorkingDays(t, calendar, time.Date(2019, time.May, 8, 23, 0, 0, 0, time.UTC), -3*Day, time.Date(2019, time.May, 4, 0, 0, 0, 0, time.UTC))
	}

	// FIXME: Not working properly on custom calendar !
	calendar := defaultFrCalendar
	d := time.Date(2019, time.May, 6, 14, 0, 0, 0, time.UTC)
	testDateArithmeticWithWorkingDays(t, calendar, d, 1*Day, d.Add(1*Day))
	testDateArithmeticWithWorkingDays(t, calendar, d, 2*Day, d.Add(2*Day).Add(Day))   // May 8th
	testDateArithmeticWithWorkingDays(t, calendar, d, 3*Day, d.Add(3*Day).Add(Day))   // May 8th
	testDateArithmeticWithWorkingDays(t, calendar, d, 4*Day, d.Add(4*Day).Add(Day))   // May 8th
	testDateArithmeticWithWorkingDays(t, calendar, d, 5*Day, d.Add(5*Day).Add(2*Day)) // May 8th + Sunday

	testDateArithmeticWithWorkingDays(t, calendar, time.Date(2019, time.May, 8, 3, 0, 0, 0, time.UTC), 1*Day, time.Date(2019, time.May, 10, 0, 0, 0, 0, time.UTC))  // May 8th
	testDateArithmeticWithWorkingDays(t, calendar, time.Date(2019, time.May, 8, 23, 0, 0, 0, time.UTC), 1*Day, time.Date(2019, time.May, 10, 0, 0, 0, 0, time.UTC)) // May 8th
	testDateArithmeticWithWorkingDays(t, calendar, time.Date(2019, time.May, 8, 23, 0, 0, 0, time.UTC), 3*Day, time.Date(2019, time.May, 13, 0, 0, 0, 0, time.UTC)) // May 8th + Sunday

}

func TestStandardCalendarSub(t *testing.T) {

	defaultFrCalendar := NewStandardCalendar("default-fr", FR)
	customCalendar := NewCustomCalendar("test-calendar")
	customCalendar.AddEntries(
		NewEntry(time.Date(2019, time.April, 28, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 1, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 5, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 8, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 12, 0, 0, 0, 0, time.UTC), false, false),
	)

	for _, calendar := range [...]Calendar{defaultFrCalendar, customCalendar} {

		testDelay(t, calendar, time.Date(2019, time.May, 11, 12, 0, 0, 0, time.UTC), time.Date(2019, time.May, 15, 12, 0, 0, 0, time.UTC), (-1+4)*Day)                          // May 12th = Sunday
		testDelay(t, calendar, time.Date(2019, time.May, 11, 16, 0, 0, 0, time.UTC), time.Date(2019, time.May, 15, 12, 0, 0, 0, time.UTC), 8*time.Hour+(-1+3)*Day+12*time.Hour) // May 12th = Sunday
		testDelay(t, calendar, time.Date(2019, time.May, 7, 12, 0, 0, 0, time.UTC), time.Date(2019, time.May, 9, 12, 0, 0, 0, time.UTC), (-1+2)*Day)                            // May 8th
		testDelay(t, calendar, time.Date(2019, time.May, 7, 12, 0, 0, 0, time.UTC), time.Date(2019, time.May, 15, 12, 0, 0, 0, time.UTC), (-2+8)*Day)                           // May 8th + May 12 (Sunday)

		testDelay(t, calendar, time.Date(2019, time.May, 8, 3, 0, 0, 0, time.UTC), time.Date(2019, time.May, 10, 12, 0, 0, 0, time.UTC), (-1+2)*Day+12*time.Hour)
		testDelay(t, calendar, time.Date(2019, time.May, 8, 23, 0, 0, 0, time.UTC), time.Date(2019, time.May, 10, 12, 0, 0, 0, time.UTC), (-1+2)*Day+12*time.Hour)
		testDelay(t, calendar, time.Date(2019, time.May, 7, 3, 0, 0, 0, time.UTC), time.Date(2019, time.May, 10, 12, 0, 0, 0, time.UTC), 21*time.Hour+(-1+2)*Day+12*time.Hour)

		testDelay(t, calendar, time.Date(2019, time.May, 7, 8, 0, 0, 0, time.UTC), time.Date(2019, time.May, 8, 23, 0, 0, 0, time.UTC), 16*time.Hour)            // May 8th
		testDelay(t, calendar, time.Date(2019, time.May, 7, 23, 0, 0, 0, time.UTC), time.Date(2019, time.May, 8, 3, 0, 0, 0, time.UTC), 1*time.Hour)             // May 8th
		testDelay(t, calendar, time.Date(2019, time.May, 7, 23, 0, 0, 0, time.UTC), time.Date(2019, time.May, 9, 3, 0, 0, 0, time.UTC), 1*time.Hour+3*time.Hour) // May 8th
		testDelay(t, calendar, time.Date(2019, time.May, 7, 23, 0, 0, 0, time.UTC), time.Date(2019, time.May, 10, 23, 0, 0, 0, time.UTC), 1*time.Hour+1*Day+23*time.Hour)

		testDelay(t, defaultFrCalendar, time.Date(2019, time.December, 17, 10, 0, 0, 0, time.UTC), time.Date(2019, time.December, 17, 11, 00, 0, 0, time.UTC), 1*time.Hour)
		testDelay(t, defaultFrCalendar, time.Date(2019, time.December, 17, 23, 59, 0, 0, time.UTC), time.Date(2019, time.December, 18, 00, 01, 0, 0, time.UTC), 2*time.Minute)

		testDelay(t, defaultFrCalendar, time.Date(2019, time.December, 18, 02, 01, 0, 0, time.FixedZone("", 1*60*60)), time.Date(2019, time.December, 18, 02, 01, 0, 0, time.UTC), 1*time.Hour)

		testDelay(t, defaultFrCalendar, time.Date(2019, time.December, 18, 00, 01, 0, 0, time.FixedZone("", 1*60*60)), time.Date(2019, time.December, 18, 02, 01, 0, 0, time.UTC), 3*time.Hour)

	}

	calendar := defaultFrCalendar
	testDelay(t, calendar, time.Date(2019, time.May, 15, 23, 0, 0, 0, time.UTC), time.Date(2019, time.May, 14, 22, 0, 0, 0, time.UTC), -1*Day+-1*time.Hour)
}
