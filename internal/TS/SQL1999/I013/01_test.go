package I013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_I013_I01301_L1(t *testing.T) {
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
		{"CreateUniqueTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)"},
		{"CreateCompositeUnique", "CREATE TABLE t2 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a, b))"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Valid unique inserts
	validTests := []struct {
		name string
		sql  string
	}{
		{"InsertUnique1", "INSERT INTO t1 VALUES (1, 'alice@example.com', 'Alice')"},
		{"InsertUnique2", "INSERT INTO t1 VALUES (2, 'bob@example.com', 'Bob')"},
		// Multiple NULLs are allowed in a UNIQUE column
		{"InsertNull1", "INSERT INTO t1 VALUES (3, NULL, 'Carol')"},
		{"InsertNull2", "INSERT INTO t1 VALUES (4, NULL, 'Dave')"},
		{"InsertComposite1", "INSERT INTO t2 VALUES (1, 1, 1)"},
		{"InsertComposite2", "INSERT INTO t2 VALUES (2, 1, 2)"},
		{"InsertComposite3", "INSERT INTO t2 VALUES (3, 2, 1)"},
	}
	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Duplicate insert should fail in both
	dupTests := []struct {
		name string
		sql  string
	}{
		{"DuplicateEmail", "INSERT INTO t1 VALUES (5, 'alice@example.com', 'Alice2')"},
		{"DuplicateComposite", "INSERT INTO t2 VALUES (4, 1, 1)"},
	}
	for _, tt := range dupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"SelectNullEmails", "SELECT * FROM t1 WHERE email IS NULL ORDER BY id"},
		{"SelectComposite", "SELECT * FROM t2 ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
