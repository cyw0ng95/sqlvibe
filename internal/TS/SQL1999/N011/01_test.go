package N011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_N011_NullComparison_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, val INTEGER)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, NULL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 20)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, NULL)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"IsNull", "SELECT id FROM t1 WHERE val IS NULL ORDER BY id"},
		{"IsNotNull", "SELECT id FROM t1 WHERE val IS NOT NULL ORDER BY id"},
		{"NullEquality", "SELECT NULL = NULL"},
		{"NullNotEqual", "SELECT NULL != NULL"},
		{"NullLiteral", "SELECT NULL"},
		{"NullCompareValue", "SELECT NULL = 1"},
		{"CountAll", "SELECT COUNT(*) FROM t1"},
		{"CountColumn", "SELECT COUNT(val) FROM t1"},
		{"OrderByNullAsc", "SELECT id, val FROM t1 ORDER BY val ASC"},
		{"OrderByNullDesc", "SELECT id, val FROM t1 ORDER BY val DESC"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
