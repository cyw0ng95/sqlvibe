package E101

import (
	"database/sql"

	_ "github.com/cyw0ng95/sqlvibe/driver"
	"github.com/cyw0ng95/sqlvibe/tests/SQL1999"
	"testing"
)

func TestSQL1999_F301_E10103_L1(t *testing.T) {
	sqlvibeDB, _ := sql.Open("sqlvibe", ":memory:")
	defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:")
	defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1), (2), (2), (3)", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT ALL a FROM t1", "SelectAll")
}
