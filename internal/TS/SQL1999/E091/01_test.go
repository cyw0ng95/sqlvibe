package E091

import (
	"database/sql"
	"testing"
	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E09101_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqliteDB.Close()

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b TEXT)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 'test'), (2, 'hello')", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1", "SelectAll")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT a FROM t1", "SelectCol")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 AS t", "SelectWithAlias")
}
