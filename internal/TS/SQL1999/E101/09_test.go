package E101

import ("database/sql"; "testing"; "github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"; "github.com/sqlvibe/sqlvibe/pkg/sqlvibe")
func TestSQL1999_F301_E10109_L1(t *testing.T) {
	sqlvibeDB, _ := sqlvibe.Open(":memory:"); defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:"); defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, a INTEGER)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 2)", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT a FROM t1", "SelectDuplicateCol")
}
