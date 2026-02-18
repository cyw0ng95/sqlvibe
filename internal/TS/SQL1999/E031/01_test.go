package E031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03101_L1(t *testing.T) {
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
		{"SimpleTable", "CREATE TABLE t1 (a INTEGER)"},
		{"TableWithPK", "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)"},
		{"TableWithMultipleCols", "CREATE TABLE t3 (a INTEGER, b TEXT, c REAL)"},
		{"TableWithConstraints", "CREATE TABLE t4 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"},
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
		{"QueryAllTables", "SELECT * FROM information_schema.tables"},
		{"QueryMainSchema", "SELECT * FROM information_schema.tables WHERE table_schema = 'main'"},
		{"QueryTableNames", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"},
		{"QueryTableTypes", "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Verify we got results
			if len(rows.Data) == 0 {
				t.Errorf("%s: expected non-empty results from information_schema query", tt.name)
			}
		})
	}
}
