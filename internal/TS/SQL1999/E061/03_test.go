package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06103_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1), (2), (3), (4), (5)"},
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
		{"INList", "SELECT * FROM t1 WHERE a IN (1, 3, 5)"},
		{"INListSingle", "SELECT * FROM t1 WHERE a IN (3)"},
		{"NotINList", "SELECT * FROM t1 WHERE a NOT IN (1, 3, 5)"},
		{"INSubquery", "SELECT * FROM t1 WHERE a IN (SELECT a FROM t1 WHERE a > 2)"},
		{"NotINSubquery", "SELECT * FROM t1 WHERE a NOT IN (SELECT a FROM t1 WHERE a > 3)"},
		{"INEmptyList", "SELECT * FROM t1 WHERE a IN ()"},
		{"INExpression", "SELECT * FROM t1 WHERE a IN (1 + 1, 2 + 1, 3 + 1)"},
		{"INMultipleCols", "SELECT * FROM t1 WHERE a IN (SELECT a FROM t1 WHERE a < 4) AND a > 1"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
