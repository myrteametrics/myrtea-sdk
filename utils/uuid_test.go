package utils

import (
	"testing"
)

func TestUUIDConvert(t *testing.T) {
	var lsb int64 = -7503457230663964891
	var msb int64 = -4106796803823093624
	u := NewUUIDFromBits(msb, lsb)
	if u != "c701ba10-cf6a-3888-97de-5d675af02725" {
		t.FailNow()
	}
}

func TestUUIDConvert2(t *testing.T) {
	var lsb int64 = -5584550200884554822
	var msb int64 = 8945390088549383280
	u := NewUUIDFromBits(msb, lsb)
	if u != "7c2468e4-0d49-3c70-b27f-b12e35f093ba" {
		t.FailNow()
	}
}
