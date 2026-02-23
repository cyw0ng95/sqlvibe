package L015

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_L015_L01501_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, val REAL, name TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, val REAL, name TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 3.14, 'Hello World')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 3.14, 'Hello World')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, -2.71, 'foo bar')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, -2.71, 'foo bar')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 100.5, 'SQLite')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 100.5, 'SQLite')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, NULL, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, NULL, NULL)")

	queryTests := []struct {
		name string
		sql  string
	}{
		// Scalar functions
		{"AbsPositive", "SELECT id, abs(val) FROM t1 WHERE val IS NOT NULL ORDER BY id"},
		{"AbsNegative", "SELECT abs(-5)"},
		{"UpperFunc", "SELECT id, upper(name) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"LowerFunc", "SELECT id, lower(name) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"SubstrFunc", "SELECT id, substr(name, 1, 5) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"SubstrFrom", "SELECT id, substr(name, 7) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"LengthStr", "SELECT id, length(name) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"LengthNull", "SELECT id, length(name) FROM t1 ORDER BY id"},
		{"RoundFunc", "SELECT id, round(val, 1) FROM t1 WHERE val IS NOT NULL ORDER BY id"},
		{"RoundNoScale", "SELECT round(3.7)"},
		// Aggregate functions
		{"CountStar", "SELECT COUNT(*) FROM t1"},
		{"CountCol", "SELECT COUNT(val) FROM t1"},
		{"SumFunc", "SELECT SUM(val) FROM t1"},
		{"AvgFunc", "SELECT AVG(val) FROM t1"},
		{"MinFunc", "SELECT min(val) FROM t1"},
		{"MaxFunc", "SELECT max(val) FROM t1"},
		{"MinStr", "SELECT min(name) FROM t1"},
		{"MaxStr", "SELECT max(name) FROM t1"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
