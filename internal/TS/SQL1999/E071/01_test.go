package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'alice', 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'alice', 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'bob', 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'bob', 20)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 'carol', 30)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 'carol', 30)")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectAllColumns", "SELECT * FROM t1"},
		{"SelectNamedColumn", "SELECT id FROM t1"},
		{"SelectMultipleColumns", "SELECT id, name FROM t1"},
		{"WhereColumnRef", "SELECT id, name FROM t1 WHERE val > 10"},
		{"WhereColumnEquals", "SELECT name FROM t1 WHERE id = 2"},
		{"WhereColumnAnd", "SELECT name FROM t1 WHERE id > 1 AND val < 30"},
		{"WhereColumnOr", "SELECT name FROM t1 WHERE id = 1 OR id = 3"},
		{"SelectExpression", "SELECT id, val * 2 FROM t1"},
		{"OrderByColumn", "SELECT id, name FROM t1 ORDER BY val DESC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
