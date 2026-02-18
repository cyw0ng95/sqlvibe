package E121
import ("database/sql"; "testing"; "github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"; "github.com/sqlvibe/sqlvibe/pkg/sqlvibe")
func TestSQL1999_F301_E1215_L1(t *testing.T) {
	sqlvibeDB, _ := sqlvibe.Open(":memory:"); defer sqlvibeDB.Close()
	sqliteDB, _ := sql.Open("sqlite", ":memory:"); defer sqliteDB.Close()
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER)", "CreateTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "DROP TABLE t1", "DropTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t2 (a INTEGER)", "CreateTable2")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "DROP TABLE IF EXISTS t2", "DropTableIfExists")
}
