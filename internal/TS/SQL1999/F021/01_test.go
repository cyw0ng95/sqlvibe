package F021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F02101_L1(t *testing.T) {
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
		{"WithMultiplePK", "CREATE TABLE t9 (a INTEGER, b TEXT, PRIMARY KEY (a, b))"},
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
		{"QueryAllColumns", "SELECT * FROM information_schema.columns"},
		{"QueryByTableName", "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = 't1'"},
		{"QueryBySchema", "SELECT * FROM information_schema.columns WHERE table_schema = 'main'"},
		{"QueryOrderByTableName", "SELECT * FROM information_schema.columns ORDER BY table_name, column_name"},
		{"QueryColumnNamesOnly", "SELECT column_name FROM information_schema.columns ORDER BY table_name, column_name"},
		{"QueryDataTypes", "SELECT table_name, column_name, data_type FROM information_schema.columns ORDER BY table_name, column_name"},
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
