package license

import (
	"testing"
	"time"
)

func TestValidateValid(t *testing.T) {
	ld := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	if !ld.DateExpires.After(time.Now()) {
		t.Error("License should not be expired")
	}
}

func TestValidateExpired(t *testing.T) {
	ld := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 1*time.Millisecond, "Myrtea Issuer")
	time.Sleep(10 * time.Millisecond)
	if ld.DateExpires.After(time.Now()) {
		t.Error("License should be expired")
	}
}
