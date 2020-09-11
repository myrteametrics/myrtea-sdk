package security

// NoAuth is a basic Auth implementation requiring the tuple admin/admin to authenticate successfully
type NoAuth struct{}

// NewNoAuth returns a pointer of NoAuth
func NewNoAuth() *NoAuth {
	return &NoAuth{}
}

// Authenticate always returns an admin user without checking the login nor the password
func (auth *NoAuth) Authenticate(login string, password string) (bool, User, error) {
	user := User{
		ID:       1,
		Login:    "admin",
		LastName: "admin",
		Role:     1,
	}
	return true, user, nil
}
