package F411

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F41101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'a')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'a')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, NULL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 'c')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 'c')")

	tests := []struct {
		name string
		sql  string
	}{
		{"OrderByNullsFirst", "SELECT * FROM t1 ORDER BY b NULLS FIRST"},
		{"OrderByNullsLast", "SELECT * FROM t1 ORDER BY b NULLS LAST"},
		{"OrderByNullsFirstDesc", "SELECT * FROM t1 ORDER BY b DESC NULLS FIRST"},
		{"OrderByNullsLastDesc", "SELECT * FROM t1 ORDER BY b DESC NULLS LAST"},
		{"OrderByNullsFirstInt", "SELECT * FROM t1 ORDER BY a NULLS FIRST"},
		{"OrderByNullsLastInt", "SELECT * FROM t1 ORDER BY a NULLS LAST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
