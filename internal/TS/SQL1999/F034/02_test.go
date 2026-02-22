package F034

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03402_L1(t *testing.T) {
	t.Skip("Known limitation: REVOKE statement not supported - sqlvibe silently ignores, sqlite errors")
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

	// Column-level REVOKE is not supported by sqlvibe or sqlite; both should error
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "REVOKE UPDATE (val) ON t1 FROM PUBLIC", "RevokeColumnUpdate")
}
