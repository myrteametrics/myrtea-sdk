package security

import (
	"errors"
	uuid "github.com/google/uuid"
	"time"
)

// User is used as the main user struct
type User struct {
	ID        uuid.UUID `json:"id"`
	Login     string    `json:"login"` // is the unique identifier of the user, through the different connection modes
	Created   time.Time `json:"created"`
	LastName  string    `json:"lastName"`
	FirstName string    `json:"firstName"`
	Email     string    `json:"email"`
	Phone     string    `json:"phone"`
}

// IsValid checks if a user is valid and has no missing mandatory fields
// * Login must not be empty
// * Login must not be shorter than 3 characters
// * Role must not be empty (or 0 value)
// * LastName must not be empty
func (user *User) IsValid() (bool, error) {
	if user.Login == "" {
		return false, errors.New("missing Login")
	}
	if len(user.Login) < 3 {
		return false, errors.New("login is too short (less than 3 charaters)")
	}
	if user.LastName == "" {
		return false, errors.New("missing Lastname")
	}
	return true, nil
}

// UserWithPassword is used to log in the user (and only this use case)
// The standard User struct must be used if the password is not required
type UserWithPassword struct {
	User
	Password string `json:"password" db:"password"`
}

// IsValid checks if a user with password is valid and has no missing mandatory fields
// * User must be valid (see User struct)
// * Password must not be empty
// * Password must not be shorter than 6 characters
func (user *UserWithPassword) IsValid() (bool, error) {
	if ok, err := user.User.IsValid(); !ok {
		return false, err
	}
	if user.Password == "" {
		return false, errors.New("missing Password")
	}
	if len(user.Password) < 6 {
		return false, errors.New("password is too short (less than 6 characters)")
	}
	return true, nil
}
