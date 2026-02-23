package D015

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D015_D01501_L1(t *testing.T) {
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
		{"CreateDateTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, event_date TEXT, event_time TEXT, event_dt TEXT)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, '2023-01-15', '10:30:00', '2023-01-15 10:30:00')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, '2023-01-15', '10:30:00', '2023-01-15 10:30:00')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, '2023-06-30', '23:59:59', '2023-06-30 23:59:59')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, '2023-06-30', '23:59:59', '2023-06-30 23:59:59')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, '2024-02-29', '00:00:00', '2024-02-29 00:00:00')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, '2024-02-29', '00:00:00', '2024-02-29 00:00:00')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"DateFunction", "SELECT date('2023-01-15')"},
		{"TimeFunction", "SELECT time('10:30:00')"},
		{"DatetimeFunction", "SELECT datetime('2023-01-15 10:30:00')"},
		{"StrftimeYear", "SELECT strftime('%Y', '2023-06-15')"},
		{"StrftimeMonth", "SELECT strftime('%m', '2023-06-15')"},
		{"StrftimeDay", "SELECT strftime('%d', '2023-06-15')"},
		{"DateDiff", "SELECT julianday('2023-12-31') - julianday('2023-01-01')"},
		{"SelectDateColumn", "SELECT id, date(event_date) FROM t1 ORDER BY id"},
		{"SelectStrftimeColumn", "SELECT id, strftime('%Y', event_date) AS yr FROM t1 ORDER BY id"},
		{"SelectDateComparison", "SELECT * FROM t1 WHERE event_date > '2023-01-01' ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
