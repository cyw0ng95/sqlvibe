package E031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03105_L1(t *testing.T) {
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
		{"TableWithPK", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)"},
		{"TableWithCompositePK", "CREATE TABLE t2 (a INTEGER, b INTEGER, c TEXT, PRIMARY KEY (a, b))"},
		{"TableWithUnique", "CREATE TABLE t3 (id INTEGER, email TEXT UNIQUE)"},
		{"TableWithMultiplePK", "CREATE TABLE t4 (x INTEGER PRIMARY KEY, y INTEGER PRIMARY KEY, z TEXT)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Note: information_schema is not supported by SQLite, so we only test sqlvibe
	queryTests := []struct {
		name string
		sql  string
	}{
		{"QueryAllKeyColumns", "SELECT * FROM information_schema.key_column_usage"},
		{"QueryByTable", "SELECT * FROM information_schema.key_column_usage WHERE table_name = 't1'"},
		{"QueryByConstraint", "SELECT * FROM information_schema.key_column_usage WHERE constraint_name = 'PRIMARY'"},
		{"QueryColumnNames", "SELECT column_name FROM information_schema.key_column_usage WHERE table_name = 't2'"},
		{"QueryBySchema", "SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'main' ORDER BY table_name"},
		{"QueryOrdinalPosition", "SELECT table_name, column_name, ordinal_position FROM information_schema.key_column_usage WHERE table_schema = 'main'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Verify we got results for key column queries
			if len(rows.Data) == 0 && (tt.name == "QueryAllKeyColumns" || tt.name == "QueryByTable") {
				t.Errorf("%s: expected non-empty results from information_schema query", tt.name)
			}
		})
	}
}
