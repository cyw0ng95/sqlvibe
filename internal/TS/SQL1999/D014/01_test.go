package D014

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D014_D01401_L1(t *testing.T) {
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
		{"CreateDecimalTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, price DECIMAL(10,2), qty NUMERIC(5,0))"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 9.99, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 9.99, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 19.50, 5)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 19.50, 5)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 0.01, 100)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 0.01, 100)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, 100.00, 1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, 100.00, 1)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"SelectWhereDecimal", "SELECT * FROM t1 WHERE price > 10.00 ORDER BY id"},
		{"SelectArithmetic", "SELECT id, price * qty AS total FROM t1 ORDER BY id"},
		{"SelectRound", "SELECT id, round(price, 1) AS rounded FROM t1 ORDER BY id"},
		{"SelectSum", "SELECT sum(price) FROM t1"},
		{"SelectAvg", "SELECT AVG(price) FROM t1"},
		{"SelectMin", "SELECT min(price) FROM t1"},
		{"SelectMax", "SELECT max(price) FROM t1"},
		{"SelectAbsDecimal", "SELECT id, abs(price - 10.0) AS diff FROM t1 ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
