package D013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D013_D01301_L1(t *testing.T) {
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
		{"CreateBooleanTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, active INTEGER, verified INTEGER)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 1, 1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 1, 1)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 0, 1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 0, 1)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 1, 0)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 1, 0)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, 0, 0)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, 0, 0)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (5, NULL, 1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (5, NULL, 1)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"SelectWhereTrue", "SELECT * FROM t1 WHERE active = 1 ORDER BY id"},
		{"SelectWhereFalse", "SELECT * FROM t1 WHERE active = 0 ORDER BY id"},
		{"SelectWhereNull", "SELECT * FROM t1 WHERE active IS NULL"},
		{"SelectWhereNotNull", "SELECT * FROM t1 WHERE active IS NOT NULL ORDER BY id"},
		{"SelectAndCondition", "SELECT * FROM t1 WHERE active = 1 AND verified = 1"},
		{"SelectOrCondition", "SELECT * FROM t1 WHERE active = 1 OR verified = 0 ORDER BY id"},
		{"SelectCountTrue", "SELECT COUNT(*) FROM t1 WHERE active = 1"},
		{"SelectCountFalse", "SELECT COUNT(*) FROM t1 WHERE active = 0"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
