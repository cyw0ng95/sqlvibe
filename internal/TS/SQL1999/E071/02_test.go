package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07102_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)"},
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
		{"SubqueryInWhere", "SELECT * FROM t1 WHERE a IN (SELECT a FROM t1 WHERE a > 1)"},
		{"SubqueryNotInWhere", "SELECT * FROM t1 WHERE a NOT IN (SELECT a FROM t1 WHERE a > 2)"},
		{"SubqueryInComparison", "SELECT * FROM t1 WHERE b = (SELECT MAX(b) FROM t1)"},
		{"SubqueryExists", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t1 WHERE a = t1.a + 1)"},
		{"SubqueryNotExists", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t1 WHERE a > 10)"},
		{"SubqueryWithAND", "SELECT * FROM t1 WHERE a > 1 AND b IN (SELECT b FROM t1 WHERE a < 3)"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
