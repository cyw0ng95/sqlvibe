package F401

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F40101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	sqlvibeDB.Exec("CREATE TABLE t2 (a INTEGER, c TEXT)")
	sqliteDB.Exec("CREATE TABLE t2 (a INTEGER, c TEXT)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'x')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'x')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'y')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'y')")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (1, 'p')")
	sqliteDB.Exec("INSERT INTO t2 VALUES (1, 'p')")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (3, 'q')")
	sqliteDB.Exec("INSERT INTO t2 VALUES (3, 'q')")

	tests := []struct {
		name string
		sql  string
	}{
		{"NaturalJoin", "SELECT * FROM t1 NATURAL JOIN t2"},
		{"NaturalJoinLeft", "SELECT * FROM t1 NATURAL LEFT JOIN t2"},
		{"NaturalJoinRight", "SELECT * FROM t1 NATURAL RIGHT JOIN t2"},
		{"UsingJoin", "SELECT * FROM t1 JOIN t2 USING (a)"},
		{"UsingJoinLeft", "SELECT * FROM t1 LEFT JOIN t2 USING (a)"},
		{"UsingJoinMultiple", "CREATE TABLE t3 (a INTEGER, b TEXT, c TEXT); INSERT INTO t3 VALUES (1, 'x', 'p'); SELECT * FROM t1 JOIN t3 USING (a, b)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
