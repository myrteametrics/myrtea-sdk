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
