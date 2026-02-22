package F221

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F22102_L1(t *testing.T) {
	t.Skip("Known limitation: DEFAULT keyword in UPDATE SET clause not supported by sqlite")
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER DEFAULT 100)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER DEFAULT 100)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 5)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 5)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 10)")

	tests := []struct {
		name string
		sql  string
	}{
		{"UpdateWithDefault", "UPDATE t1 SET val = DEFAULT WHERE id = 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "VerifyUpdate")
}
