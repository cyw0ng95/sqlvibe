package B8_EXPRESSION

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

// TestSQL1999_B8_OperatorPrecedence_L1 tests arithmetic operator precedence.
func TestSQL1999_B8_OperatorPrecedence_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"AddMul", "SELECT 2 + 3 * 4"},
		{"MulAdd", "SELECT 3 * 4 + 2"},
		{"Parens", "SELECT (2 + 3) * 4"},
		{"SubDiv", "SELECT 10 - 4 / 2"},
		{"UnaryMinus", "SELECT -2 * 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B8_CaseWhen_L1 tests CASE WHEN expressions including NULL handling.
func TestSQL1999_B8_CaseWhen_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	for _, v := range []string{"1", "2", "NULL", "3"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"CaseSimple", "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t ORDER BY a"},
		{"CaseNull", "SELECT CASE WHEN a IS NULL THEN 'null' ELSE 'not_null' END FROM t ORDER BY a"},
		{"CaseNoElse", "SELECT CASE WHEN a = 1 THEN 'one' END FROM t ORDER BY a"},
		{"CaseValue", "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B8_Between_L1 tests BETWEEN operator including NULL behavior.
func TestSQL1999_B8_Between_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	for _, v := range []string{"1", "5", "10", "NULL"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"BetweenBasic", "SELECT a FROM t WHERE a BETWEEN 3 AND 8 ORDER BY a"},
		{"NotBetween", "SELECT a FROM t WHERE a NOT BETWEEN 3 AND 8 ORDER BY a"},
		{"BetweenWithNull", "SELECT a FROM t WHERE a BETWEEN 1 AND 10 ORDER BY a"},
		{"BetweenNull", "SELECT NULL BETWEEN 1 AND 10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B8_LogicalOperators_L1 tests AND/OR/NOT with NULL.
func TestSQL1999_B8_LogicalOperators_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"TrueAndTrue", "SELECT 1 AND 1"},
		{"TrueAndFalse", "SELECT 1 AND 0"},
		{"NullAndTrue", "SELECT NULL AND 1"},
		{"TrueOrFalse", "SELECT 1 OR 0"},
		{"NullOrFalse", "SELECT NULL OR 0"},
		{"FalseOrFalse", "SELECT 0 OR 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B8_ComplexExpressions_L1 tests complex nested expressions.
func TestSQL1999_B8_ComplexExpressions_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b INTEGER, c TEXT)")
	sl.Exec("CREATE TABLE t (a INTEGER, b INTEGER, c TEXT)")
	sv.Exec("INSERT INTO t VALUES (3, 4, 'hello')")
	sl.Exec("INSERT INTO t VALUES (3, 4, 'hello')")
	sv.Exec("INSERT INTO t VALUES (NULL, 2, 'world')")
	sl.Exec("INSERT INTO t VALUES (NULL, 2, 'world')")

	tests := []struct{ name, sql string }{
		{"ComplexArith", "SELECT a * b + COALESCE(a, 0) FROM t ORDER BY a"},
		{"MixedConds", "SELECT c FROM t WHERE b > 1 ORDER BY c"},
		{"NestedCase", "SELECT CASE WHEN a IS NULL THEN COALESCE(b, 0) ELSE a + b END FROM t ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B8_ComparisonOperators_L1 tests comparison operators.
func TestSQL1999_B8_ComparisonOperators_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"Eq", "SELECT 1 = 1"},
		{"Neq", "SELECT 1 != 2"},
		{"Lt", "SELECT 1 < 2"},
		{"Gt", "SELECT 2 > 1"},
		{"Lte", "SELECT 1 <= 1"},
		{"Gte", "SELECT 2 >= 2"},
		{"NullEq", "SELECT NULL = NULL"},
		{"NullNeq", "SELECT NULL != NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
