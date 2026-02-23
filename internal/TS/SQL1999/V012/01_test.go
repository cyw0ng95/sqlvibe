package V012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_V012_DropView_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'a')")
	sqlvibeDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'a')")
	sqliteDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1")

	tests := []struct {
		name string
		sql  string
	}{
		{"DropView", "DROP VIEW v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V012_DropViewIfExistsExisting_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	sqlvibeDB.Exec("CREATE VIEW v1 AS SELECT id FROM t1")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE VIEW v1 AS SELECT id FROM t1")

	tests := []struct {
		name string
		sql  string
	}{
		{"DropViewIfExistsExisting", "DROP VIEW IF EXISTS v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V012_DropViewIfExistsNonExistent_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	tests := []struct {
		name string
		sql  string
	}{
		{"DropViewIfExistsNonExistent", "DROP VIEW IF EXISTS no_such_view"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
