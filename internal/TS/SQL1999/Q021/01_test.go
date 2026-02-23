package Q021

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

func createJoinTables(sv *sqlvibe.Database, sl *sql.DB) {
	sv.Exec("CREATE TABLE dept (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE dept (id INTEGER, name TEXT)")
	sv.Exec("CREATE TABLE emp (id INTEGER, dept_id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE emp (id INTEGER, dept_id INTEGER, name TEXT)")

	sv.Exec("INSERT INTO dept VALUES (1, 'Engineering')")
	sl.Exec("INSERT INTO dept VALUES (1, 'Engineering')")
	sv.Exec("INSERT INTO dept VALUES (2, 'HR')")
	sl.Exec("INSERT INTO dept VALUES (2, 'HR')")
	sv.Exec("INSERT INTO dept VALUES (3, 'Finance')")
	sl.Exec("INSERT INTO dept VALUES (3, 'Finance')")

	sv.Exec("INSERT INTO emp VALUES (1, 1, 'Alice')")
	sl.Exec("INSERT INTO emp VALUES (1, 1, 'Alice')")
	sv.Exec("INSERT INTO emp VALUES (2, 1, 'Bob')")
	sl.Exec("INSERT INTO emp VALUES (2, 1, 'Bob')")
	sv.Exec("INSERT INTO emp VALUES (3, 2, 'Carol')")
	sl.Exec("INSERT INTO emp VALUES (3, 2, 'Carol')")
	sv.Exec("INSERT INTO emp VALUES (4, NULL, 'Dave')")
	sl.Exec("INSERT INTO emp VALUES (4, NULL, 'Dave')")
}

// TestSQL1999_Q021_InnerJoin_L1 tests INNER JOIN operations.
func TestSQL1999_Q021_InnerJoin_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createJoinTables(sv, sl)

	tests := []struct{ name, sql string }{
		{"InnerJoin", "SELECT e.name, d.name FROM emp e INNER JOIN dept d ON e.dept_id = d.id ORDER BY e.name"},
		{"InnerJoinMultiCond", "SELECT e.name, d.name FROM emp e INNER JOIN dept d ON e.dept_id = d.id AND d.id = 1 ORDER BY e.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q021_LeftJoin_L1 tests LEFT JOIN operations.
func TestSQL1999_Q021_LeftJoin_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createJoinTables(sv, sl)

	tests := []struct{ name, sql string }{
		{"LeftJoin", "SELECT e.name, d.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id ORDER BY e.name"},
		{"LeftJoinNullDept", "SELECT e.name, d.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id WHERE d.id IS NULL ORDER BY e.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q021_SelfJoin_L1 tests self-join.
func TestSQL1999_Q021_SelfJoin_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE node (id INTEGER, parent_id INTEGER, label TEXT)")
	sl.Exec("CREATE TABLE node (id INTEGER, parent_id INTEGER, label TEXT)")
	sv.Exec("INSERT INTO node VALUES (1, NULL, 'root')")
	sl.Exec("INSERT INTO node VALUES (1, NULL, 'root')")
	sv.Exec("INSERT INTO node VALUES (2, 1, 'child1')")
	sl.Exec("INSERT INTO node VALUES (2, 1, 'child1')")
	sv.Exec("INSERT INTO node VALUES (3, 1, 'child2')")
	sl.Exec("INSERT INTO node VALUES (3, 1, 'child2')")

	tests := []struct{ name, sql string }{
		{"SelfJoin", "SELECT c.label, p.label AS parent FROM node c INNER JOIN node p ON c.parent_id = p.id ORDER BY c.label"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
