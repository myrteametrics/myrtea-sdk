package security

import (
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/myrteametrics/myrtea-sdk/v5/tests"
)

func dbInit(dbClient *sqlx.DB, t *testing.T) {
	_, err := dbClient.Exec(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_, err = dbClient.Exec(`create table IF NOT EXISTS users_v1 (
		id serial primary key not null,
		login varchar(100) not null unique,
		password varchar(100) not null,
		role integer not null,
		created timestamptz not null,
		last_name varchar(100) not null,
		first_name varchar(100) not null,
		email varchar(100) not null,
		phone varchar(100)
	);`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_, err = dbClient.Exec(`INSERT INTO users_v1 (id, login, password, role, last_name, first_name, email, created, phone) 
		VALUES (DEFAULT, 'admin', crypt('admin', gen_salt('md5')), 1, 'admin', 'admin', 'admin@admin.com', current_timestamp, '0123456789');`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = dbClient.Exec(`INSERT INTO users_v1 (id, login, password, role, last_name, first_name, email, created, phone) 
		VALUES (DEFAULT, 'test', crypt('test', gen_salt('md5')), 2, 'test', 'test', 'test@test.com', current_timestamp, '0123456789');`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func dbDestroy(dbClient *sqlx.DB, t *testing.T) {
	dbClient.Exec(`drop table users_v1;`)
	dbClient.Exec(`drop extension pgcrypto;`)
}

func TestNewDatabaseAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgresql test in short mode")
	}
	db := tests.DBClient(t)
	defer dbDestroy(db, t)
	dbInit(db, t)
	auth := NewDatabaseAuth(db)
	if auth == nil {
		t.Error("Auth should not be nil")
	}
}

func TestDatabaseAuthenticate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgresql test in short mode")
	}
	db := tests.DBClient(t)
	defer dbDestroy(db, t)
	dbInit(db, t)
	auth := NewDatabaseAuth(db)
	valid, user, err := auth.Authenticate("admin", "admin")

	if !valid {
		t.Error("Invalid credentials")
	}
	if err != nil {
		t.Error(err)
	}
	if user.Login != "admin" {
		t.Error("Invalid Name")
	}
}

func TestDatabaseAuthenticateNoTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgresql test in short mode")
	}
	db := tests.DBClient(t)
	defer dbDestroy(db, t)
	dbInit(db, t)
	auth := NewDatabaseAuth(db)
	valid, user, err := auth.Authenticate("test", "test")

	if !valid {
		t.Error("Invalid credentials")
	}
	if err != nil {
		t.Error(err)
	}
	if user.Login != "test" {
		t.Error("Invalid Name")
	}
}

func TestDatabaseAuthenticateInvalidLogin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgresql test in short mode")
	}
	db := tests.DBClient(t)
	defer dbDestroy(db, t)
	dbInit(db, t)
	auth := NewDatabaseAuth(db)
	valid, user, err := auth.Authenticate("not_a_user", "admin")

	if valid {
		t.Error("Invalid credentials")
	}
	if err == nil {
		t.Error("Should return an error")
	}
	if user.Login != "" {
		t.Error("User should be an empty struct")
	}
}

func TestDatabaseAuthenticateInvalidPassword(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgresql test in short mode")
	}
	db := tests.DBClient(t)
	defer dbDestroy(db, t)
	dbInit(db, t)
	auth := NewDatabaseAuth(db)
	valid, user, err := auth.Authenticate("admin", "not_a_password")

	if valid {
		t.Error("Invalid credentials")
	}
	if err == nil {
		t.Error("Should return an error")
	}
	if user.Login != "" {
		t.Error("User should be an empty struct")
	}
}
