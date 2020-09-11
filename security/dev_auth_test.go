package security

import "testing"

func TestNewDevAuth(t *testing.T) {
	auth := NewDevAuth()
	if auth == nil {
		t.Error("Auth is nil")
	}
}

func TestDevAuthenticate(t *testing.T) {
	auth := NewDevAuth()
	valid, user, err := auth.Authenticate("admin", "admin")
	if !valid {
		t.Error("Authentication should be valid")
	}
	if err != nil {
		t.Error("Should not returns an error")
	}
	if user.Login != "admin" {
		t.Error("Invalid login")
	}
	if user.LastName != "admin" {
		t.Error("Invalid last name")
	}
	if user.Role != 1 {
		t.Error("Invalid role")
	}
}

func TestDevAuthenticateInvalidLogin(t *testing.T) {
	auth := NewDevAuth()
	valid, user, err := auth.Authenticate("not_a_user", "admin")
	if valid {
		t.Error("Authentication should not be valid")
	}
	if err != nil {
		t.Error(err)
	}
	if user.Login != "" {
		t.Error("An empty user should have been returned")
	}
}

func TestDevAuthenticateInvalidPassword(t *testing.T) {
	auth := NewDevAuth()
	valid, user, err := auth.Authenticate("admin", "not_a_password")
	if valid {
		t.Error("Authentication should not be valid")
	}
	if err != nil {
		t.Error(err)
	}
	if user.Login != "" {
		t.Error("An empty user should have been returned")
	}
}
