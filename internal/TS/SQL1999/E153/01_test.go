package E153

import (
"database/sql"
"testing"

"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_E153_01_L1 tests updatable queries with subqueries
func TestSQL1999_E153_01_L1(t *testing.T) {
sqlvibePath := ":memory:"
sqlitePath := ":memory:"

sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
defer sqlvibeDB.Close()
sqliteDB, _ := sql.Open("sqlite", sqlitePath)
defer sqliteDB.Close()

// Create test table
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (id INTEGER, val INTEGER)", "CreateTable")
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)", "Insert")

// Test UPDATE with subquery
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "UPDATE t1 SET val = (SELECT MAX(val) FROM t1) WHERE id = 1", "UpdateWithSubquery")
SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "SelectAfterUpdate")
}
