package Q041

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

// TestSQL1999_Q041_Union_L1 tests UNION set operation.
func TestSQL1999_Q041_Union_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t1 (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE t1 (id INTEGER, name TEXT)")
	sv.Exec("CREATE TABLE t2 (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE t2 (id INTEGER, name TEXT)")

	sv.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sl.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sv.Exec("INSERT INTO t1 VALUES (2, 'Bob')")
	sl.Exec("INSERT INTO t1 VALUES (2, 'Bob')")
	sv.Exec("INSERT INTO t2 VALUES (2, 'Bob')")
	sl.Exec("INSERT INTO t2 VALUES (2, 'Bob')")
	sv.Exec("INSERT INTO t2 VALUES (3, 'Carol')")
	sl.Exec("INSERT INTO t2 VALUES (3, 'Carol')")

	tests := []struct{ name, sql string }{
		{"Union", "SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id"},
		{"UnionDeduplicates", "SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q041_UnionAll_L1 tests UNION ALL set operation.
func TestSQL1999_Q041_UnionAll_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE a (val INTEGER)")
	sl.Exec("CREATE TABLE a (val INTEGER)")
	sv.Exec("CREATE TABLE b (val INTEGER)")
	sl.Exec("CREATE TABLE b (val INTEGER)")

	sv.Exec("INSERT INTO a VALUES (1)")
	sl.Exec("INSERT INTO a VALUES (1)")
	sv.Exec("INSERT INTO a VALUES (2)")
	sl.Exec("INSERT INTO a VALUES (2)")
	sv.Exec("INSERT INTO b VALUES (2)")
	sl.Exec("INSERT INTO b VALUES (2)")
	sv.Exec("INSERT INTO b VALUES (3)")
	sl.Exec("INSERT INTO b VALUES (3)")

	tests := []struct{ name, sql string }{
		{"UnionAll", "SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val"},
		{"UnionAllDuplicates", "SELECT COUNT(*) FROM (SELECT val FROM a UNION ALL SELECT val FROM b)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q041_UnionMixed_L1 tests UNION and UNION ALL with mixed sources.
func TestSQL1999_Q041_UnionMixed_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE x (val INTEGER)")
	sl.Exec("CREATE TABLE x (val INTEGER)")
	sv.Exec("CREATE TABLE y (val INTEGER)")
	sl.Exec("CREATE TABLE y (val INTEGER)")

	sv.Exec("INSERT INTO x VALUES (10)")
	sl.Exec("INSERT INTO x VALUES (10)")
	sv.Exec("INSERT INTO x VALUES (20)")
	sl.Exec("INSERT INTO x VALUES (20)")
	sv.Exec("INSERT INTO y VALUES (20)")
	sl.Exec("INSERT INTO y VALUES (20)")
	sv.Exec("INSERT INTO y VALUES (30)")
	sl.Exec("INSERT INTO y VALUES (30)")

	tests := []struct{ name, sql string }{
		{"UnionDistinct", "SELECT val FROM x UNION SELECT val FROM y ORDER BY val"},
		{"UnionAllKeepDups", "SELECT val FROM x UNION ALL SELECT val FROM y ORDER BY val"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
