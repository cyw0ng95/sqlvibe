package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06101_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1, 2), (3, 4), (5, 6), (2, 2)"},
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
		{"Equal", "SELECT * FROM t1 WHERE a = 3"},
		{"NotEqual", "SELECT * FROM t1 WHERE a != 3"},
		{"LessThan", "SELECT * FROM t1 WHERE a < 3"},
		{"LessOrEqual", "SELECT * FROM t1 WHERE a <= 3"},
		{"GreaterThan", "SELECT * FROM t1 WHERE a > 3"},
		{"GreaterOrEqual", "SELECT * FROM t1 WHERE a >= 3"},
		{"CompareColsEqual", "SELECT * FROM t1 WHERE a = b"},
		{"CompareColsLess", "SELECT * FROM t1 WHERE a < b"},
		{"CompareColsGreater", "SELECT * FROM t1 WHERE a > b"},
		{"MultipleComparisons", "SELECT * FROM t1 WHERE a >= 2 AND a <= 4"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
