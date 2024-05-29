package security

import (
	"errors"

	"github.com/jmoiron/sqlx"
)

// DatabaseAuth is a basic Auth implementation requiring the tuple admin/admin to authenticate successfully
type DatabaseAuth struct {
	DBClient *sqlx.DB
}

// NewDatabaseAuth returns a pointer of DatabaseAuth
func NewDatabaseAuth(DBClient *sqlx.DB) *DatabaseAuth {
	return &DatabaseAuth{DBClient}
}

// Authenticate check the input credentials and returns a User the passwords matches
func (auth *DatabaseAuth) Authenticate(login string, password string) (bool, User, error) {

	query := `SELECT id, login, created, last_name, first_name, email, phone FROM users_v4
		WHERE login = :login AND (password =crypt(:password, password))`
	params := map[string]interface{}{
		"login":    login,
		"password": password,
	}
	rows, err := auth.DBClient.NamedQuery(query, params)
	if err != nil {
		return false, User{}, err
	}
	defer rows.Close()

	if rows.Next() {
		var user User
		err = rows.Scan(&user.ID, &user.Login, &user.Created, &user.LastName, &user.FirstName, &user.Email, &user.Phone)
		if err != nil {
			return false, User{}, err
		}
		return true, user, nil
	}
	return false, User{}, errors.New("Invalid credentials")
}
