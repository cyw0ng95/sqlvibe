package F441

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F44101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (10)")

	tests := []struct {
		name string
		sql  string
	}{
		{"ScalarSubquery", "SELECT (SELECT MAX(a) FROM t1) FROM t1"},
		{"ScalarSubquery2", "SELECT (SELECT MIN(a) FROM t1)"},
		{"ScalarSubqueryInWhere", "SELECT * FROM t1 WHERE a > (SELECT AVG(a) FROM t1)"},
		{"ScalarSubqueryInExpr", "SELECT a + (SELECT 1) FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
