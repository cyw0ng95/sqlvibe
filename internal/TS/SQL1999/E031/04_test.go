package E031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03104_L1(t *testing.T) {
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
		{"TableWithUnique", "CREATE TABLE t2 (id INTEGER, email TEXT UNIQUE)"},
		{"TableWithCheck", "CREATE TABLE t3 (id INTEGER, age INTEGER CHECK (age >= 0))"},
		{"TableWithMultiplePK", "CREATE TABLE t4 (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY (a, b))"},
		{"TableWithNotNull", "CREATE TABLE t5 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"},
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
		{"QueryByTable", "SELECT * FROM information_schema.table_constraints WHERE table_name = 't1'"},
		{"QueryPKConstraints", "SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY'"},
		{"QueryUniqueConstraints", "SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'UNIQUE'"},
		{"QueryCheckConstraints", "SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'CHECK'"},
		{"QueryBySchema", "SELECT * FROM information_schema.table_constraints WHERE table_schema = 'main' ORDER BY table_name"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
