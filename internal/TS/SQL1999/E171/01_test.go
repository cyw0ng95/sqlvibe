package E171

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_E171_01_L1 tests SQLSTATE error codes
func TestSQL1999_E171_01_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqliteDB.Close()

	// Create table
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)", "CreateTable")

	// Test duplicate key error (should produce SQLSTATE 23000)
	_, err1 := sqlvibeDB.Exec("INSERT INTO t1 VALUES (1)")
	_, err2 := sqlvibeDB.Exec("INSERT INTO t1 VALUES (1)") // Duplicate key

	if err2 == nil {
		t.Error("Expected error for duplicate primary key, got nil")
	}
	_ = err1
	// TODO: Check SQLSTATE error code when implemented
}
