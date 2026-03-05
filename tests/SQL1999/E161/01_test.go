package E161

import (
	"database/sql"

	_ "github.com/cyw0ng95/sqlvibe/driver"
	"testing"

	"github.com/cyw0ng95/sqlvibe/tests/SQL1999"
)

// TestSQL1999_E161_01_L1 tests SQL comments using double minus (--)
func TestSQL1999_E161_01_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := sql.Open("sqlvibe", sqlvibePath)
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqliteDB.Close()

	// Test comments
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "-- This is a comment\nCREATE TABLE t1 (id INTEGER)", "CreateWithComment")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1) -- inline comment", "InsertWithInlineComment")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 -- comment", "SelectWithComment")
}
