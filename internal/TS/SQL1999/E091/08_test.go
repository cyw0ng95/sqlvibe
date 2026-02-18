package E091

import (
	"database/sql"
	"testing"
	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E09108_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := sqlvibe.Open(sqlvibePath)
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqliteDB.Close()

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b TEXT)", "CreateTable1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t2 (a INTEGER, c TEXT)", "CreateTable2")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 'test')", "Insert1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t2 VALUES (1, 'world')", "Insert2")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 JOIN t2 USING (a)", "JoinUsing")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 INNER JOIN t2 USING (a)", "InnerJoinUsing")
}
