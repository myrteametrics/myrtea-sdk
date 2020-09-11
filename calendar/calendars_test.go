package calendar

import (
	"testing"
	"time"
)

func TestCalendars(t *testing.T) {
	c := GetDefaultCalendar()
	if c == nil {
		t.Error("default calendar is nil")
		t.FailNow()
	}
	if c.GetName() != "default-fr" {
		t.Error("invalid default calendar name")
		t.FailNow()
	}

	cc := NewCustomCalendar("new-fr")
	cc.AddEntries(
		NewEntry(time.Date(2019, time.April, 28, 0, 0, 0, 0, time.UTC), false, false),
		NewEntry(time.Date(2019, time.May, 1, 0, 0, 0, 0, time.UTC), false, false),
	)
	UpdateCalendar("new-fr", cc)

	var found bool
	c, found = GetCalendar("new-fr")
	if !found {
		t.Error("calendar new-fr not found")
		t.FailNow()
	}
	if c == nil {
		t.Error("calendar new-fr found but nil")
		t.FailNow()
	}
	if c.GetName() != "new-fr" {
		t.Log(c.GetName())
		t.Error("Invalid calendar name")
		t.FailNow()
	}
}
