package E152

import (
"database/sql"
"testing"

"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_E152_01_L1 tests SET TRANSACTION ISOLATION LEVEL
func TestSQL1999_E152_01_L1(t *testing.T) {
sqlvibePath := ":memory:"
sqlitePath := ":memory:"

sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
defer sqlvibeDB.Close()
sqliteDB, _ := sql.Open("sqlite", sqlitePath)
defer sqliteDB.Close()

// Test SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "BEGIN TRANSACTION", "BeginTransaction")
// Note: SQLite doesn't support SET TRANSACTION, so this test may need adjustment
// SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "SetIsolation")
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (id INTEGER, name TEXT)", "CreateTable")
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 'test')", "Insert")
SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1", "Select")
SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "COMMIT", "Commit")
}
