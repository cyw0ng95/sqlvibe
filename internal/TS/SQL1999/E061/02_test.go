package E061

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06102_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	createTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1), (5), (10), (15), (20)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"BetweenInclusive", "SELECT * FROM t1 WHERE a BETWEEN 5 AND 15"},
		{"BetweenExclusive", "SELECT * FROM t1 WHERE a BETWEEN 6 AND 14"},
		{"NotBetween", "SELECT * FROM t1 WHERE a NOT BETWEEN 5 AND 15"},
		{"BetweenSingle", "SELECT * FROM t1 WHERE a BETWEEN 10 AND 10"},
		{"BetweenLow", "SELECT * FROM t1 WHERE a BETWEEN 1 AND 5"},
		{"BetweenHigh", "SELECT * FROM t1 WHERE a BETWEEN 15 AND 20"},
		{"BetweenWithNegative", "SELECT * FROM t1 WHERE a BETWEEN -5 AND 5"},
		{"BetweenColumns", "SELECT * FROM t1 WHERE a BETWEEN (SELECT MIN(a) FROM t1) AND (SELECT MAX(a) FROM t1)"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
