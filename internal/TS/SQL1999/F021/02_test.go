package F021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F02102_L1(t *testing.T) {
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
		{"SimpleIntTable", "CREATE TABLE t1 (a INTEGER)"},
		{"SimpleTextTable", "CREATE TABLE t2 (a TEXT)"},
		{"SimpleRealTable", "CREATE TABLE t3 (a REAL)"},
		{"SimpleBlobTable", "CREATE TABLE t4 (a BLOB)"},
		{"MixedTypes", "CREATE TABLE t5 (a INTEGER, b TEXT, c REAL, d BLOB)"},
		{"WithPK", "CREATE TABLE t6 (id INTEGER PRIMARY KEY, val TEXT)"},
		{"WithNotNull", "CREATE TABLE t7 (a INTEGER NOT NULL)"},
		{"WithDefault", "CREATE TABLE t8 (a INTEGER DEFAULT 0)"},
		{"MultipleTables", "CREATE TABLE t9 (id INTEGER PRIMARY KEY); CREATE TABLE t10 (id INTEGER PRIMARY KEY)"},
		{"TableWithUnderscore", "CREATE TABLE test_table (id INTEGER)"},
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
		{"QueryAllTables", "SELECT * FROM information_schema.tables"},
		{"QueryMainSchema", "SELECT * FROM information_schema.tables WHERE table_schema = 'main'"},
		{"QueryTableNames", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"},
		{"QueryTableTypes", "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main'"},
		{"QueryOrderByTableName", "SELECT * FROM information_schema.tables ORDER BY table_name"},
		{"QueryTableNamesOnly", "SELECT table_name FROM information_schema.tables ORDER BY table_name"},
		{"QuerySchemaAndType", "SELECT table_schema, table_type FROM information_schema.tables"},
		{"QueryWithMultipleTables", "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Verify query succeeds for information_schema
		})
	}
}
