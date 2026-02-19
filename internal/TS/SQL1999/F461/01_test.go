package F461

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F46101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (a TEXT)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES ('hello')")
	sqliteDB.Exec("INSERT INTO t1 VALUES ('hello')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES ('world')")
	sqliteDB.Exec("INSERT INTO t1 VALUES ('world')")

	tests := []struct {
		name string
		sql  string
	}{
		{"CharsetSupport", "SELECT * FROM t1"},
		{"CharsetSupport2", "SELECT LENGTH(a) FROM t1"},
		{"CharsetSupport3", "SELECT UPPER(a), LOWER(a) FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
