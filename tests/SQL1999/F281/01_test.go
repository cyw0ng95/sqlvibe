package F281

import (
	"database/sql"

	_ "github.com/cyw0ng95/sqlvibe/driver"
	"testing"

	"github.com/cyw0ng95/sqlvibe/tests/SQL1999"
)

func TestSQL1999_F301_F28101_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sql.Open("sqlvibe", sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	tests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, data TEXT)"},
		{"CreateTable2", "CREATE TABLE t2 (id INTEGER, name TEXT, value REAL)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
