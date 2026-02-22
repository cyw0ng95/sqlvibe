package F371

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F37101_L1(t *testing.T) {
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
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (NULL)")

	tests := []struct {
		name string
		sql  string
	}{
		{"NullInExpr", "SELECT a + 1 FROM t1"},
		{"NullInWhere", "SELECT * FROM t1 WHERE a IS NULL"},
		{"NullInJoin", "SELECT * FROM t1 AS t1a JOIN t1 AS t1b ON t1a.a = t1b.a"},
		{"NullInAggregate", "SELECT COUNT(*), AVG(a) FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
