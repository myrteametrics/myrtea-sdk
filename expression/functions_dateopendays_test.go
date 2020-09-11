package expression

import (
	"fmt"
	"testing"
	"time"
)

func TestDelayInOpenDays(t *testing.T) {
	res, err := delayInOpenDays("2020-02-08T12:30:00.000", "2020-02-10T12:30:00.000")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	d, _ := time.ParseDuration(fmt.Sprintf("%ds", res.(int64)/1000))
	if expected, _ := time.ParseDuration("24h"); d != expected {
		t.Error("invalid result")
		t.FailNow()
	}

	res, err = delayInOpenDays("2020-02-08T12:30:00.000", "2020-02-10T12:30:00.000", "default-fr")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	d, _ = time.ParseDuration(fmt.Sprintf("%ds", res.(int64)/1000))
	if expected, _ := time.ParseDuration("24h"); d != expected {
		t.Error("invalid result")
		t.FailNow()
	}
}

func TestDelayInOpenDaysInvalid(t *testing.T) {
	_, err := delayInOpenDays("2020-02-08T12:30:00.000")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = delayInOpenDays("2020-2020-2020", "2020-2020-2020")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = delayInOpenDays(3, true)
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}
	_, err = delayInOpenDays("2020-02-08T12:30:00.000", "2020-02-10T12:30:00.000", "not_a_calendar")
	if err == nil {
		t.Error("invalid calendar should return an error")
		t.FailNow()
	}
}

func TestAddDurationOpenDays(t *testing.T) {
	res, err := addDurationOpenDays("2020-02-08T12:30:00.000", "24h")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if res != "2020-02-10T12:30:00.000" {
		t.Error("invalid result")
		t.Log(res)
		t.FailNow()
	}
}

func TestAddDurationOpenDaysInvalid(t *testing.T) {
	_, err := addDurationOpenDays("2020-02-08T12:30:00.000")
	if err == nil {
		t.Error("invalid parameters number should return an error")
		t.FailNow()
	}
	_, err = addDurationOpenDays("2020-2020-2020", "3h")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = addDurationOpenDays("2020-02-08T12:30:00.000", "not_a_duration")
	if err == nil {
		t.Error("invalid parameters date format should return an error")
		t.FailNow()
	}
	_, err = addDurationOpenDays(3, true)
	if err == nil {
		t.Error("invalid parameters types should return an error")
		t.FailNow()
	}
	_, err = addDurationOpenDays("2020-02-08T12:30:00.000", "3h", "not_a_calendar")
	if err == nil {
		t.Error("invalid calendar should return an error")
		t.FailNow()
	}
}
