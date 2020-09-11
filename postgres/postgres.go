package postgres

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Used to marshal / unmarshal PSQL Array
	"go.uber.org/zap"
)

// Credentials : Give All DB Information.
type Credentials struct {
	URL      string `json:"url,omitempty"`
	Port     string `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
	DbName   string `json:"dbname,omitempty"`
}

// DbConnection : init DB access.
func DbConnection(credentials Credentials) (*sqlx.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		credentials.URL,
		credentials.Port,
		credentials.User,
		credentials.Password,
		credentials.DbName)
	db, err := sqlx.Open("postgres", psqlInfo)
	if err != nil {
		zap.L().Error("DbConnection.Open:", zap.Error(err))
		return nil, err
	}
	if err = db.Ping(); err != nil {
		zap.L().Error("DbConnection.Ping:", zap.Error(err))
		return nil, err
	}
	return db, nil
}
