package F471

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F47103_L1(t *testing.T) {
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
	sqlvibeDB.Exec("CREATE TABLE t2 (id INTEGER, score INTEGER)")
	sqliteDB.Exec("CREATE TABLE t2 (id INTEGER, score INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 20)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 30)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 30)")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (1, 5)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (1, 5)")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (2, 15)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (2, 15)")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (3, 25)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (3, 25)")

	tests := []struct {
		name string
		sql  string
	}{
		{"OrderByScalarSubquery", "SELECT t1.id FROM t1 ORDER BY (SELECT score FROM t2 WHERE t2.id = t1.id)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
