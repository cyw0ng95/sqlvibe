package E101

import (
	"database/sql"

	_ "github.com/cyw0ng95/sqlvibe/driver"
	"github.com/cyw0ng95/sqlvibe/tests/SQL1999"
	"testing"
)

func TestSQL1999_F301_E10104_L1(t *testing.T) {
	sqlvibeDB, _ := sql.Open("sqlvibe", ":memory:")
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:")
	defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1)", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT a AS col1 FROM t1", "SelectAlias")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT a * 2 AS doubled FROM t1", "SelectAliasExpr")
}
