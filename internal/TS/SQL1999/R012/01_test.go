package R012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_R012_R01201_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'Bob')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'Bob')")

	alterTests := []struct {
		name string
		sql  string
	}{
		{"AddColumn", "ALTER TABLE t1 ADD COLUMN age INTEGER"},
		{"AddColumnWithDefault", "ALTER TABLE t1 ADD COLUMN score REAL DEFAULT 0.0"},
	}
	for _, tt := range alterTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAfterAddColumn", "SELECT * FROM t1 ORDER BY id"},
		{"SelectNewColumn", "SELECT id, name, age FROM t1 ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_R012_R01202_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE old_name (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE old_name (id INTEGER PRIMARY KEY, val TEXT)")
	sqlvibeDB.Exec("INSERT INTO old_name VALUES (1, 'hello')")
	sqliteDB.Exec("INSERT INTO old_name VALUES (1, 'hello')")

	renameTests := []struct {
		name string
		sql  string
	}{
		{"RenameTable", "ALTER TABLE old_name RENAME TO new_name"},
	}
	for _, tt := range renameTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAfterRename", "SELECT * FROM new_name ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
