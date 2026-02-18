package E131
import ("database/sql"; "testing"; "github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"; "github.com/sqlvibe/sqlvibe/pkg/sqlvibe")
func TestSQL1999_F301_E1315_L1(t *testing.T) {
	sqlvibeDB, _ := sqlvibe.Open(":memory:"); defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:"); defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b TEXT)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, 'test'), (2, 'hello')", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 WHERE a > 1", "SelectWhere")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT a, COUNT(*) FROM t1 GROUP BY a", "SelectGroupBy")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT a, COUNT(*) FROM t1 GROUP BY a HAVING a > 0", "SelectHaving")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY a", "SelectOrderBy")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 LIMIT 1", "SelectLimit")
}
