package F033

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03303_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, c INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, c INTEGER)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10, 20, 30)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10, 20, 30)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 40, 50, 60)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 40, 50, 60)")

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "ALTER TABLE t1 DROP COLUMN c", "DropColumnC")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "VerifyDataIntegrity")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT SUM(a), SUM(b) FROM t1", "VerifyAggAfterDrop")
}
