package F311

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F31101_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"CreateSchema", "CREATE TABLE t1 (a INTEGER)"},
		{"CreateTable", "CREATE TABLE t2 (a INTEGER, b TEXT)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F31102_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER)")

	sqlvibeDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1")
	sqliteDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1")

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM v1", "SelectFromView")
}

func TestSQL1999_F301_F31103_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER)")

	sqlvibeDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1 WHERE a > 0")
	sqliteDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1 WHERE a > 0")

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM v1", "SelectFromViewWithWhere")
}

func TestSQL1999_F301_F31104_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"Grant", "CREATE TABLE t1 (a INTEGER)"},
		{"Revoke", "DROP TABLE t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
