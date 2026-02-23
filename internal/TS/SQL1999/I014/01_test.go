package I014

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_I014_I01401_L1(t *testing.T) {
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
		{"CreateNotNullTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)"},
		{"CreateNotNullWithDefault", "CREATE TABLE t2 (id INTEGER PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active', count INTEGER NOT NULL DEFAULT 0)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Valid inserts
	validTests := []struct {
		name string
		sql  string
	}{
		{"InsertValid1", "INSERT INTO t1 VALUES (1, 'Alice', 30)"},
		{"InsertValid2", "INSERT INTO t1 VALUES (2, 'Bob', NULL)"},
		{"InsertWithDefault", "INSERT INTO t2 (id) VALUES (1)"},
		{"InsertExplicit", "INSERT INTO t2 VALUES (2, 'inactive', 5)"},
	}
	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// NOT NULL violation - both should fail
	violationTests := []struct {
		name string
		sql  string
	}{
		{"NullNotNullCol", "INSERT INTO t1 VALUES (3, NULL, 25)"},
	}
	for _, tt := range violationTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"SelectDefault", "SELECT * FROM t2 ORDER BY id"},
		{"SelectNotNull", "SELECT * FROM t1 WHERE age IS NOT NULL"},
		{"SelectNullAge", "SELECT * FROM t1 WHERE age IS NULL"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
