package F011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F01101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER, b INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER, b INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 20)")

	tests := []struct {
		name string
		sql  string
	}{
		{"SubqueryInFrom", "SELECT * FROM (SELECT a, b FROM t1)"},
		{"SubqueryWithAlias", "SELECT * FROM (SELECT a, b FROM t1) AS subq"},
		{"SubqueryWithAlias2", "SELECT subq.a, subq.b FROM (SELECT a, b FROM t1) AS subq"},
		{"SubqueryWithWhere", "SELECT * FROM (SELECT a, b FROM t1 WHERE a > 1) AS subq"},
		{"SubqueryWithGroupBy", "SELECT subq.a, COUNT(*) FROM (SELECT a, b FROM t1) AS subq GROUP BY subq.a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F01102_L1(t *testing.T) {
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
	sqlvibeDB.Exec("CREATE TABLE t2 (b INTEGER)")
	sqliteDB.Exec("CREATE TABLE t2 (b INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2)")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (1)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (1)")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (3)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (3)")

	tests := []struct {
		name string
		sql  string
	}{
		{"MultipleSubqueries", "SELECT * FROM (SELECT a FROM t1) AS t1sub, (SELECT b FROM t2) AS t2sub"},
		{"SubqueryJoin", "SELECT t1sub.a, t2sub.b FROM (SELECT a FROM t1) AS t1sub, (SELECT b FROM t2) AS t2sub"},
		{"NestedSubqueries", "SELECT * FROM (SELECT * FROM (SELECT a FROM t1) AS inner1) AS outer1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
