package F032

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03203_L1(t *testing.T) {
	t.Skip("Known limitation: CASCADE drop with dependent views not fully supported")

	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER)")
	sqlvibeDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1")
	sqliteDB.Exec("CREATE VIEW v1 AS SELECT * FROM t1")

	// CASCADE drop should also drop dependent views
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "DROP TABLE t1", "DropTableWithDependentView")
}
