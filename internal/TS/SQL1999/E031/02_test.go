package E031

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03102_L1(t *testing.T) {
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
		{"TableWithIntegers", "CREATE TABLE t1 (id INTEGER, val INTEGER)"},
		{"TableWithText", "CREATE TABLE t2 (id INTEGER, name TEXT)"},
		{"TableWithReal", "CREATE TABLE t3 (id INTEGER, price REAL)"},
		{"TableWithBLOB", "CREATE TABLE t4 (id INTEGER, data BLOB)"},
		{"TableWithMixed", "CREATE TABLE t5 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, balance REAL)"},
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
		{"QueryAllColumns", "SELECT * FROM information_schema.columns"},
		{"QueryByTable", "SELECT * FROM information_schema.columns WHERE table_name = 't1'"},
		{"QueryColumnNames", "SELECT column_name FROM information_schema.columns WHERE table_name = 't1'"},
		{"QueryDataTypes", "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 't2'"},
		{"QueryIsNullable", "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 't3'"},
		{"QueryBySchema", "SELECT * FROM information_schema.columns WHERE table_schema = 'main' ORDER BY table_name, column_name"},
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
