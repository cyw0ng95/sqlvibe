package E091

import (
	"database/sql"
	"testing"
	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E09110_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqliteDB.Close()

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b INTEGER, parent_a INTEGER)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 10, NULL), (2, 20, 1), (3, 30, 2)", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT t1.a, t1.b, t2.b AS parent_b FROM t1 LEFT JOIN t1 AS t2 ON t1.parent_a = t2.a", "SelfJoin")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT t1.a, t1.b, t2.b AS child_b FROM t1 LEFT JOIN t1 AS t2 ON t1.a = t2.parent_a", "SelfJoinChildren")
}
