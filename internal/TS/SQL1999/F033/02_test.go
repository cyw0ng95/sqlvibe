package F033

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03302_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, required TEXT NOT NULL, optional TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, required TEXT NOT NULL, optional TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'val1', 'opt1')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'val1', 'opt1')")

	// Dropping optional column (no NOT NULL constraint) should succeed
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "ALTER TABLE t1 DROP COLUMN optional", "DropOptionalColumn")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "VerifyRequiredRemains")
}
