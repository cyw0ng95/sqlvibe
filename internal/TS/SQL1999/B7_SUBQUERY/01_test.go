package B7_SUBQUERY

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/glebarez/go-sqlite"
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

// TestSQL1999_B7_ScalarSubquery_L1 tests scalar subquery behavior.
func TestSQL1999_B7_ScalarSubquery_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sl.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sv.Exec("INSERT INTO t VALUES (1, 'x')")
	sl.Exec("INSERT INTO t VALUES (1, 'x')")
	sv.Exec("INSERT INTO t VALUES (2, 'y')")
	sl.Exec("INSERT INTO t VALUES (2, 'y')")

	tests := []struct{ name, sql string }{
		{"ScalarInWhere", "SELECT a FROM t WHERE a = (SELECT MIN(a) FROM t)"},
		{"ScalarInSelect", "SELECT a, (SELECT MAX(a) FROM t) AS mx FROM t ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B7_CorrelatedSubquery_L1 tests correlated subqueries.
func TestSQL1999_B7_CorrelatedSubquery_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE emp (id INTEGER, dept TEXT, salary INTEGER)")
	sl.Exec("CREATE TABLE emp (id INTEGER, dept TEXT, salary INTEGER)")
	rows := [][]string{
		{"1", "'eng'", "100"}, {"2", "'eng'", "200"},
		{"3", "'hr'", "150"}, {"4", "'hr'", "120"},
	}
	for _, r := range rows {
		sv.Exec("INSERT INTO emp VALUES (" + r[0] + ", " + r[1] + ", " + r[2] + ")")
		sl.Exec("INSERT INTO emp VALUES (" + r[0] + ", " + r[1] + ", " + r[2] + ")")
	}

	tests := []struct{ name, sql string }{
		{"CorrelatedWhere", "SELECT id, salary FROM emp e WHERE salary > (SELECT AVG(salary) FROM emp WHERE dept = e.dept) ORDER BY id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B7_Exists_L1 tests EXISTS and NOT EXISTS subqueries.
func TestSQL1999_B7_Exists_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t1 (a INTEGER)")
	sl.Exec("CREATE TABLE t1 (a INTEGER)")
	sv.Exec("CREATE TABLE t2 (b INTEGER)")
	sl.Exec("CREATE TABLE t2 (b INTEGER)")
	sv.Exec("INSERT INTO t1 VALUES (1)")
	sl.Exec("INSERT INTO t1 VALUES (1)")
	sv.Exec("INSERT INTO t1 VALUES (2)")
	sl.Exec("INSERT INTO t1 VALUES (2)")
	sv.Exec("INSERT INTO t2 VALUES (1)")
	sl.Exec("INSERT INTO t2 VALUES (1)")

	tests := []struct{ name, sql string }{
		{"Exists", "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE b = t1.a) ORDER BY a"},
		{"NotExists", "SELECT a FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE b = t1.a) ORDER BY a"},
		{"ExistsEmpty", "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE b = 999) ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B7_InSubquery_L1 tests IN / NOT IN with subqueries.
func TestSQL1999_B7_InSubquery_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t1 (a INTEGER)")
	sl.Exec("CREATE TABLE t1 (a INTEGER)")
	sv.Exec("CREATE TABLE t2 (b INTEGER)")
	sl.Exec("CREATE TABLE t2 (b INTEGER)")
	for _, v := range []string{"1", "2", "3"} {
		sv.Exec("INSERT INTO t1 VALUES (" + v + ")")
		sl.Exec("INSERT INTO t1 VALUES (" + v + ")")
	}
	sv.Exec("INSERT INTO t2 VALUES (1)")
	sl.Exec("INSERT INTO t2 VALUES (1)")
	sv.Exec("INSERT INTO t2 VALUES (3)")
	sl.Exec("INSERT INTO t2 VALUES (3)")

	tests := []struct{ name, sql string }{
		{"InSubquery", "SELECT a FROM t1 WHERE a IN (SELECT b FROM t2) ORDER BY a"},
		{"NotInSubquery", "SELECT a FROM t1 WHERE a NOT IN (SELECT b FROM t2) ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B7_InWithNulls_L1 tests IN / NOT IN behavior when subquery contains NULL.
func TestSQL1999_B7_InWithNulls_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	for _, v := range []string{"1", "2", "NULL"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"InWithNull", "SELECT a FROM t WHERE a IN (1, NULL) ORDER BY a"},
		{"NotInWithNull", "SELECT a FROM t WHERE a NOT IN (1, NULL) ORDER BY a"},
		{"InLiteralList", "SELECT a FROM t WHERE a IN (1, 2) ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B7_NestedSubquery_L1 tests nested subqueries.
func TestSQL1999_B7_NestedSubquery_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER, b INTEGER)")
	for _, v := range [][]string{{"1", "10"}, {"2", "20"}, {"3", "30"}} {
		sv.Exec("INSERT INTO t VALUES (" + v[0] + ", " + v[1] + ")")
		sl.Exec("INSERT INTO t VALUES (" + v[0] + ", " + v[1] + ")")
	}

	tests := []struct{ name, sql string }{
		{"NestedSubquery", "SELECT a FROM (SELECT a FROM t WHERE b > 10) AS sub ORDER BY a"},
		{"DoubleNested", "SELECT a FROM (SELECT a FROM (SELECT a, b FROM t) WHERE a > 1) AS sub ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B7_ScalarMultiRowError_L1 tests that scalar subquery returning multiple rows is handled.
func TestSQL1999_B7_ScalarMultiRowError_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	sv.Exec("INSERT INTO t VALUES (1)")
	sl.Exec("INSERT INTO t VALUES (1)")
	sv.Exec("INSERT INTO t VALUES (2)")
	sl.Exec("INSERT INTO t VALUES (2)")

	// Both sqlvibe and SQLite should either error or return a result for scalar subquery
	// returning multiple rows. CompareQueryResults handles the both-error case.
	SQL1999.CompareQueryResults(t, sv, sl, "SELECT (SELECT a FROM t)", "ScalarMultiRow")
}
