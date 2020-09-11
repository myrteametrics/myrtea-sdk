package security

// DevAuth is a basic Auth implementation requiring the tuple admin/admin to authenticate successfully
type DevAuth struct{}

// NewDevAuth returns a pointer of DevAuth
func NewDevAuth() *DevAuth {
	return &DevAuth{}
}

// Authenticate check the input credentials (which must be admin/admin in this case)
func (auth *DevAuth) Authenticate(login string, password string) (bool, User, error) {
	if login == "admin" && password == "admin" {
		user := User{
			ID:       1,
			Login:    "admin",
			LastName: "admin",
			Role:     1,
		}
		return true, user, nil
	}
	return false, User{}, nil
}
