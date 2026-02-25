package B1_NULL

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

// TestSQL1999_B1_IsNull_L1 tests IS NULL and IS NOT NULL predicates.
func TestSQL1999_B1_IsNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sl.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sv.Exec("INSERT INTO t VALUES (1, 'x')")
	sl.Exec("INSERT INTO t VALUES (1, 'x')")
	sv.Exec("INSERT INTO t VALUES (NULL, NULL)")
	sl.Exec("INSERT INTO t VALUES (NULL, NULL)")
	sv.Exec("INSERT INTO t VALUES (2, NULL)")
	sl.Exec("INSERT INTO t VALUES (2, NULL)")

	tests := []struct{ name, sql string }{
		{"IsNull", "SELECT a FROM t WHERE a IS NULL"},
		{"IsNotNull", "SELECT a FROM t WHERE a IS NOT NULL"},
		{"IsNullText", "SELECT b FROM t WHERE b IS NULL"},
		{"IsNotNullText", "SELECT b FROM t WHERE b IS NOT NULL"},
		{"NullEquality", "SELECT a FROM t WHERE a = NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_Coalesce_L1 tests COALESCE with multiple NULLs.
func TestSQL1999_B1_Coalesce_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"CoalesceAllNull", "SELECT COALESCE(NULL, NULL, NULL)"},
		{"CoalesceFirst", "SELECT COALESCE(1, 2, 3)"},
		{"CoalesceSecond", "SELECT COALESCE(NULL, 2, 3)"},
		{"CoalesceThird", "SELECT COALESCE(NULL, NULL, 3)"},
		{"CoalesceString", "SELECT COALESCE(NULL, 'hello')"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_Ifnull_L1 tests IFNULL and NULLIF.
func TestSQL1999_B1_Ifnull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"IfnullNull", "SELECT IFNULL(NULL, 42)"},
		{"IfnullNotNull", "SELECT IFNULL(10, 42)"},
		{"NullifEqual", "SELECT NULLIF(5, 5)"},
		{"NullifNotEqual", "SELECT NULLIF(5, 6)"},
		{"NullifNull", "SELECT NULLIF(NULL, 5)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_NullInAggregates_L1 tests NULL handling in COUNT/SUM/AVG.
func TestSQL1999_B1_NullInAggregates_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	sv.Exec("INSERT INTO t VALUES (1)")
	sl.Exec("INSERT INTO t VALUES (1)")
	sv.Exec("INSERT INTO t VALUES (NULL)")
	sl.Exec("INSERT INTO t VALUES (NULL)")
	sv.Exec("INSERT INTO t VALUES (3)")
	sl.Exec("INSERT INTO t VALUES (3)")

	tests := []struct{ name, sql string }{
		{"CountStar", "SELECT COUNT(*) FROM t"},
		{"CountCol", "SELECT COUNT(a) FROM t"},
		{"SumWithNull", "SELECT SUM(a) FROM t"},
		{"AvgWithNull", "SELECT AVG(a) FROM t"},
		{"MaxWithNull", "SELECT MAX(a) FROM t"},
		{"MinWithNull", "SELECT MIN(a) FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_NullArithmetic_L1 tests NULL propagation in arithmetic.
func TestSQL1999_B1_NullArithmetic_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"NullPlusInt", "SELECT NULL + 1"},
		{"IntPlusNull", "SELECT 1 + NULL"},
		{"NullTimesInt", "SELECT NULL * 5"},
		{"NullConcat", "SELECT NULL || 'hello'"},
		{"NullMinus", "SELECT 10 - NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_DistinctWithNull_L1 tests DISTINCT behavior with NULLs.
func TestSQL1999_B1_DistinctWithNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	for _, v := range []string{"1", "NULL", "1", "NULL", "2"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"SelectDistinct", "SELECT DISTINCT a FROM t ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_OrderByNull_L1 tests ORDER BY behavior with NULLs.
func TestSQL1999_B1_OrderByNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	for _, v := range []string{"3", "NULL", "1", "NULL", "2"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"OrderAsc", "SELECT a FROM t ORDER BY a ASC"},
		{"OrderDesc", "SELECT a FROM t ORDER BY a DESC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B1_NullInWhere_L1 tests NULL comparisons in WHERE clauses.
func TestSQL1999_B1_NullInWhere_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sl.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sv.Exec("INSERT INTO t VALUES (1, 'a')")
	sl.Exec("INSERT INTO t VALUES (1, 'a')")
	sv.Exec("INSERT INTO t VALUES (NULL, 'b')")
	sl.Exec("INSERT INTO t VALUES (NULL, 'b')")

	tests := []struct{ name, sql string }{
		{"NullNotIn", "SELECT b FROM t WHERE a NOT IN (1, 2)"},
		{"IsNullInSelect", "SELECT a IS NULL FROM t"},
		{"CaseNull", "SELECT CASE WHEN a IS NULL THEN 'null' ELSE 'not_null' END FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
