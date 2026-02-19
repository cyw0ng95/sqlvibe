package F431

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F43101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'test')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'test')")

	tests := []struct {
		name string
		sql  string
	}{
		{"DeclareCursor", "SELECT * FROM t1"},
		{"DeclareCursorWithOrder", "SELECT * FROM t1 ORDER BY a"},
		{"DeclareCursorWithWhere", "SELECT * FROM t1 WHERE a > 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F43102_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'a')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'a')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'b')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'b')")

	tests := []struct {
		name string
		sql  string
	}{
		{"FetchFirst", "SELECT * FROM t1"},
		{"FetchNext", "SELECT * FROM t1"},
		{"FetchPrior", "SELECT * FROM t1 ORDER BY a DESC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
