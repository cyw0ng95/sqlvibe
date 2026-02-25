package B2_TYPECONV

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

// TestSQL1999_B2_ImplicitStringToNum_L1 tests implicit string-to-number coercion.
func TestSQL1999_B2_ImplicitStringToNum_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"StringPlusInt", "SELECT '3' + 2"},
		{"StringTimesInt", "SELECT '4' * 3"},
		{"NonNumericString", "SELECT 'abc' + 1"},
		{"NumericStringCmp", "SELECT '10' > '9'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B2_CastBasic_L1 tests CAST to various types.
func TestSQL1999_B2_CastBasic_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"CastToInt", "SELECT CAST('42' AS INTEGER)"},
		{"CastToReal", "SELECT CAST('3.14' AS REAL)"},
		{"CastToText", "SELECT CAST(100 AS TEXT)"},
		{"CastIntToReal", "SELECT CAST(5 AS REAL)"},
		{"CastRealToInt", "SELECT CAST(3.7 AS INTEGER)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B2_CastNull_L1 tests CAST on NULL values.
func TestSQL1999_B2_CastNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"CastNullToInt", "SELECT CAST(NULL AS INTEGER)"},
		{"CastNullToText", "SELECT CAST(NULL AS TEXT)"},
		{"CastNullToReal", "SELECT CAST(NULL AS REAL)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B2_CastBoundary_L1 tests CAST on boundary/edge values.
func TestSQL1999_B2_CastBoundary_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"CastFloatString", "SELECT CAST('1e2' AS REAL)"},
		{"CastNegative", "SELECT CAST('-99' AS INTEGER)"},
		{"CastZero", "SELECT CAST('0' AS INTEGER)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B2_TypeAffinity_L1 tests column type affinity behavior.
func TestSQL1999_B2_TypeAffinity_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (i INTEGER, r REAL, tx TEXT, n NUMERIC)")
	sl.Exec("CREATE TABLE t (i INTEGER, r REAL, tx TEXT, n NUMERIC)")
	sv.Exec("INSERT INTO t VALUES ('42', '3.14', 100, 5)")
	sl.Exec("INSERT INTO t VALUES ('42', '3.14', 100, 5)")

	tests := []struct{ name, sql string }{
		{"SelectAll", "SELECT * FROM t"},
		{"SelectInt", "SELECT i FROM t"},
		{"SelectReal", "SELECT r FROM t"},
		{"SelectText", "SELECT tx FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B2_TypeCoercionInExpr_L1 tests type coercion in expressions.
func TestSQL1999_B2_TypeCoercionInExpr_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"IntRealAdd", "SELECT 1 + 1.5"},
		{"IntRealMul", "SELECT 2 * 2.5"},
		{"TextConcat", "SELECT 'a' || 'b'"},
		{"MixedTypes", "SELECT 1 + '2' + 3.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
