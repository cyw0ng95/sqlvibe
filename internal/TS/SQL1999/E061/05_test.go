package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06105_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'test'), (2, NULL), (3, 'hello'), (NULL, 'world')"},
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
		{"ISNULL", "SELECT * FROM t1 WHERE a IS NULL"},
		{"ISNOTNULL", "SELECT * FROM t1 WHERE a IS NOT NULL"},
		{"ISNULLColB", "SELECT * FROM t1 WHERE b IS NULL"},
		{"ISNOTNULLColB", "SELECT * FROM t1 WHERE b IS NOT NULL"},
		{"ISNOTNULLBoth", "SELECT * FROM t1 WHERE a IS NOT NULL AND b IS NOT NULL"},
		{"ISNULLEither", "SELECT * FROM t1 WHERE a IS NULL OR b IS NULL"},
		{"ComparisonWithNULL", "SELECT * FROM t1 WHERE a = NULL"},
		{"NotEqualNULL", "SELECT * FROM t1 WHERE a != NULL"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
