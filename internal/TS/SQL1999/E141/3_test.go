package E141
import ("database/sql"; "testing"; "github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"; "github.com/sqlvibe/sqlvibe/pkg/sqlvibe")
func TestSQL1999_F301_E1413_L1(t *testing.T) {
	sqlvibeDB, _ := sqlvibe.Open(":memory:"); defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:"); defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER, b INTEGER)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO t1 VALUES (1, NULL), (2, NULL)", "InsertValues")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 WHERE a = NULL", "SelectNullComparison")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 WHERE a IS NULL", "SelectIsNull")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 WHERE a IS NOT NULL", "SelectIsNotNull")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT COALESCE(a, 0) FROM t1", "SelectCoalesce")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY a NULLS FIRST", "SelectNullsFirst")
}
