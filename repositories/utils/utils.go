package utils

import (
	"database/sql"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
	"regexp"
)

var (
	checkSQLFieldRegex = regexp.MustCompile("^[A-Za-z0-9_]+$")
)

// isValidTableName checks if a string is only composed of alphanumeric or underscore characters
func isValidTableName(tableName string) bool {
	return checkSQLFieldRegex.MatchString(tableName)
}

func RefreshNextIdGen(conn *sql.DB, table string) (int64, bool, error) {
	// Validate table name to prevent SQL injection
	if !isValidTableName(table) {
		err := fmt.Errorf("invalid table name: %s", table)
		zap.L().Error("SQL injection attempt detected:", zap.Error(err))
		return 0, false, err
	}

	query := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(conn).
		Select("setval(pg_get_serial_sequence('" + table + "', 'id'), coalesce(max(id),0) + 1, false)").
		From(table)

	rows, err := query.Query()
	if err != nil {
		zap.L().Error("Couldn't query the database:", zap.Error(err))
		return 0, false, err
	}
	defer rows.Close()

	var data int64
	if rows.Next() {
		err := rows.Scan(&data)
		if err != nil {
			return 0, false, err
		}
		return data, true, nil
	} else {
		return 0, false, nil
	}
}
