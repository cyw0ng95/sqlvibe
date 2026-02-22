package F121

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F12102_L1(t *testing.T) {
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
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 20)")

	// Verify row count reporting via COUNT aggregate as a proxy for diagnostics
	tests := []struct {
		name string
		sql  string
	}{
		{"RowCountSelect", "SELECT COUNT(*) FROM t1"},
		{"RowCountAfterInsert", "SELECT COUNT(*) FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
