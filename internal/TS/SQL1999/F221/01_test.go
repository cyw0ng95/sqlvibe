package F221

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F22101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER DEFAULT 42, name TEXT DEFAULT 'unknown')")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER DEFAULT 42, name TEXT DEFAULT 'unknown')")

	tests := []struct {
		name string
		sql  string
	}{
		{"InsertWithDefault", "INSERT INTO t1 (id) VALUES (1)"},
		{"InsertExplicitDefault", "INSERT INTO t1 (id, val) VALUES (2, DEFAULT)"},
		{"InsertAllExplicit", "INSERT INTO t1 (id, val, name) VALUES (3, 99, 'test')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "VerifyDefaults")
}
