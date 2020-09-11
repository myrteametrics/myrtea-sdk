package security

import "testing"

// NewNoAuth returns a pointer of NoAuth
func TestNewNoAuth(t *testing.T) {
	auth := NewDevAuth()
	if auth == nil {
		t.Error("Auth is nil")
	}
}

// Authenticate check the input credentials (which must be admin/admin in this case)
func TestNoAuthAuthenticate(t *testing.T) {
	auth := NewNoAuth()
	valid, user, err := auth.Authenticate("admin", "admin")
	if !valid {
		t.Error("Authentication should be valid")
	}
	if err != nil {
		t.Error("Should not returns an error")
	}
	if user.Login != "admin" {
		t.Error("Invalid Name")
	}
	if user.Role != 1 {
		t.Error("Invalid Role")
	}
}
