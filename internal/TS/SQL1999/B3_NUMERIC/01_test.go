package B3_NUMERIC

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

// TestSQL1999_B3_FloatArithmetic_L1 tests basic float arithmetic behavior.
func TestSQL1999_B3_FloatArithmetic_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"IntDiv", "SELECT 7 / 2"},
		{"RealDiv", "SELECT 7.0 / 2"},
		{"RealAdd", "SELECT 0.1 + 0.2"},
		{"RealMul", "SELECT 0.1 * 10"},
		{"NegReal", "SELECT -3.5 * 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B3_DivisionByZero_L1 tests division by zero returns NULL.
func TestSQL1999_B3_DivisionByZero_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"IntDivByZero", "SELECT 1 / 0"},
		{"RealDivByZero", "SELECT 1.0 / 0"},
		{"ZeroDivByZero", "SELECT 0 / 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B3_Modulo_L1 tests modulo operator including negative values.
func TestSQL1999_B3_Modulo_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"BasicMod", "SELECT 10 % 3"},
		{"NegativeMod", "SELECT -10 % 3"},
		{"ModNegDivisor", "SELECT 10 % -3"},
		{"ModZero", "SELECT 5 % 1"},
		{"ModByZero", "SELECT 5 % 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B3_MinMaxBoundaries_L1 tests MIN/MAX with boundary values.
func TestSQL1999_B3_MinMaxBoundaries_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")
	for _, v := range []string{"-9223372036854775808", "0", "9223372036854775807", "1", "-1"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"MaxInt", "SELECT MAX(a) FROM t"},
		{"MinInt", "SELECT MIN(a) FROM t"},
		{"AbsValue", "SELECT ABS(-42)"},
		{"AbsMin", "SELECT ABS(MIN(a)) FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B3_ScientificNotation_L1 tests scientific notation literals.
func TestSQL1999_B3_ScientificNotation_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"SciPos", "SELECT 1e3"},
		{"SciNeg", "SELECT 1e-3"},
		{"SciMul", "SELECT 2.5e2 * 4"},
		{"SciAdd", "SELECT 1e2 + 200"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B3_NumericFunctions_L1 tests numeric built-in functions.
func TestSQL1999_B3_NumericFunctions_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"AbsPos", "SELECT ABS(10)"},
		{"AbsNeg", "SELECT ABS(-10)"},
		{"AbsNull", "SELECT ABS(NULL)"},
		{"AbsZero", "SELECT ABS(0)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
