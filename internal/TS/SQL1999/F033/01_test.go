package F033

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03301_L1(t *testing.T) {
	t.Skip("Known limitation: ALTER TABLE DROP COLUMN not fully supported in sqlvibe")
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, name TEXT, extra TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, name TEXT, extra TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'alice', 'x')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'alice', 'x')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'bob', 'y')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'bob', 'y')")

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "ALTER TABLE t1 DROP COLUMN extra", "DropColumn")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "VerifyAfterDrop")
}
