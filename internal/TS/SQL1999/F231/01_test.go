package F231

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F23101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2)")

	tests := []struct {
		name string
		sql  string
	}{
		{"TableExpression", "SELECT * FROM (VALUES (1), (2)) AS t(a)"},
		{"TableExpression2", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)"},
		{"TableExpression3", "WITH cte AS (SELECT * FROM t1) SELECT * FROM cte"},
		{"TableExpression4", "WITH cte AS (SELECT a FROM t1 WHERE a > 1) SELECT * FROM cte"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
