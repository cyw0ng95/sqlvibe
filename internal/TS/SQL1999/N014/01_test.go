package N014

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_N014_Nullif_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, val INTEGER, label TEXT)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 0, 'zero')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 0, 'zero')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 5, 'five')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 5, 'five')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 10, 'ten')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 10, 'ten')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"NullifEqualInts", "SELECT NULLIF(1, 1)"},
		{"NullifDiffInts", "SELECT NULLIF(1, 2)"},
		{"NullifEqualStrings", "SELECT NULLIF('foo', 'foo')"},
		{"NullifDiffStrings", "SELECT NULLIF('foo', 'bar')"},
		{"NullifZero", "SELECT NULLIF(0, 0)"},
		{"NullifNonZero", "SELECT NULLIF(5, 0)"},
		{"NullifColumnEqual", "SELECT id, NULLIF(val, 0) AS result FROM t1 ORDER BY id"},
		{"NullifInExpr", "SELECT id, COALESCE(NULLIF(val, 0), -1) AS result FROM t1 ORDER BY id"},
		{"NullifWithNull", "SELECT NULLIF(NULL, 1)"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
