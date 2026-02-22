package F021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F02104_L1(t *testing.T) {
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
		{"TableWithPK", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)"},
		{"TableWithUnique", "CREATE TABLE t2 (id INTEGER UNIQUE, val TEXT)"},
		{"TableWithCheck", "CREATE TABLE t3 (id INTEGER, val TEXT CHECK(id > 0))"},
		{"TableWithMultiplePK", "CREATE TABLE t4 (a INTEGER, b INTEGER, PRIMARY KEY (a, b))"},
		{"TableWithMultipleConstraints", "CREATE TABLE t5 (id INTEGER PRIMARY KEY UNIQUE, val TEXT NOT NULL, age INTEGER CHECK(age >= 0))"},
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
		{"QueryAllConstraints", "SELECT * FROM information_schema.table_constraints"},
		{"QueryByTableName", "SELECT * FROM information_schema.table_constraints WHERE table_name = 't1'"},
		{"QueryBySchema", "SELECT * FROM information_schema.table_constraints WHERE table_schema = 'main'"},
		{"QueryConstraintTypes", "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_schema = 'main'"},
		{"QueryPKConstraints", "SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY'"},
		{"QueryUniqueConstraints", "SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'UNIQUE'"},
		{"QueryCheckConstraints", "SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'CHECK'"},
		{"QueryOrderByTable", "SELECT * FROM information_schema.table_constraints ORDER BY table_name, constraint_name"},
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
