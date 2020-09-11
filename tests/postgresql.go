package tests

import (
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/myrteametrics/myrtea-sdk/v4/postgres"
)

// DBClient returns a postgresql test client for integration tests
// It targets a specific hostname "postgres" in a Gitlab CI environnement
// or "localhost" by default
func DBClient(t *testing.T) *sqlx.DB {
	credentials := postgres.Credentials{
		URL:      "localhost",
		Port:     "5432",
		DbName:   "postgres",
		User:     "postgres",
		Password: "postgres",
	}
	if os.Getenv("GITLAB_CI") != "" {
		t.Log("Found GITLAB_CI environment variable")
		// credentials.URL = "gitlab-myrtea-tests-postgresql"
		// credentials.DbName = os.Getenv("POSTGRES_DB")
	}
	dbClient, err := postgres.DbConnection(credentials)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	return dbClient
}
