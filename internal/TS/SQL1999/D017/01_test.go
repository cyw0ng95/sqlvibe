package D017

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D017_D01701_L1(t *testing.T) {
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
		{"CreateIntervalTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, start_date TEXT, end_date TEXT, duration_days INTEGER)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, '2023-01-01', '2023-01-31', 30)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, '2023-01-01', '2023-01-31', 30)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, '2023-03-01', '2023-06-01', 92)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, '2023-03-01', '2023-06-01', 92)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, '2023-12-01', '2024-01-01', 31)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, '2023-12-01', '2024-01-01', 31)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"DateAddDay", "SELECT id, date(start_date, '+1 day') AS next_day FROM t1 ORDER BY id"},
		{"DateAddMonth", "SELECT id, date(start_date, '+1 month') AS next_month FROM t1 ORDER BY id"},
		{"DateAddYear", "SELECT id, date(start_date, '+1 year') AS next_year FROM t1 ORDER BY id"},
		{"DateSubDay", "SELECT id, date(end_date, '-1 day') AS prev_day FROM t1 ORDER BY id"},
		{"JuliandayDiff", "SELECT id, cast(julianday(end_date) - julianday(start_date) AS INTEGER) AS days FROM t1 ORDER BY id"},
		{"StrftimeWeekday", "SELECT id, strftime('%w', start_date) AS weekday FROM t1 ORDER BY id"},
		{"StrftimeDoy", "SELECT id, strftime('%j', start_date) AS doy FROM t1 ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
