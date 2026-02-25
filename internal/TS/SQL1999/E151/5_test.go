package E151

import (
	"database/sql"
	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	"testing"
)

func TestSQL1999_F301_E1515_L1(t *testing.T) {
	sqlvibeDB, _ := sqlvibe.Open(":memory:")
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:")
	defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b TEXT)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "BEGIN TRANSACTION", "BeginTransaction")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 'test')", "InsertValues")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "COMMIT", "CommitTransaction")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1", "SelectAfterCommit")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (2, 'hello')", "InsertForRollback")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "ROLLBACK", "RollbackTransaction")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1", "SelectAfterRollback")
}
