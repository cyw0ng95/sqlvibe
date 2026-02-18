package E091

import (
	"database/sql"
	"testing"
	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E09105_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqliteDB.Close()

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b TEXT)", "CreateTable1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t2 (x INTEGER, y TEXT)", "CreateTable2")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 'test'), (2, 'hello')", "Insert1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t2 VALUES (1, 'world'), (3, 'foo')", "Insert2")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.a = t2.x", "FullOuterJoin")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 FULL JOIN t2 ON t1.a = t2.x", "FullJoin")
}
