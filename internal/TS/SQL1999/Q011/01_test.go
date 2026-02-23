package Q011

import (
	"database/sql"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func setup(t *testing.T) (*sqlvibe.Database, *sql.DB) {
	t.Helper()
	sv, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	sl, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return sv, sl
}

// TestSQL1999_Q011_BasicSelect_L1 tests basic SELECT operations.
func TestSQL1999_Q011_BasicSelect_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t1 (id INTEGER, name TEXT, score INTEGER)")
	sl.Exec("CREATE TABLE t1 (id INTEGER, name TEXT, score INTEGER)")
	sv.Exec("INSERT INTO t1 VALUES (1, 'Alice', 90)")
	sl.Exec("INSERT INTO t1 VALUES (1, 'Alice', 90)")
	sv.Exec("INSERT INTO t1 VALUES (2, 'Bob', 75)")
	sl.Exec("INSERT INTO t1 VALUES (2, 'Bob', 75)")
	sv.Exec("INSERT INTO t1 VALUES (3, 'Carol', 85)")
	sl.Exec("INSERT INTO t1 VALUES (3, 'Carol', 85)")

	tests := []struct{ name, sql string }{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"SelectColumns", "SELECT id, name FROM t1 ORDER BY id"},
		{"SelectWithAlias", "SELECT id AS num, name AS fullname FROM t1 ORDER BY id"},
		{"SelectDistinct", "SELECT DISTINCT score FROM (SELECT 90 AS score UNION ALL SELECT 75 UNION ALL SELECT 90) ORDER BY score"},
		{"SelectWhere", "SELECT name FROM t1 WHERE score > 80 ORDER BY name"},
		{"SelectOrderBy", "SELECT * FROM t1 ORDER BY score DESC"},
		{"SelectLimit", "SELECT * FROM t1 ORDER BY id LIMIT 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
