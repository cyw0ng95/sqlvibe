package E011

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01106_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t_int (a INTEGER)")
	sqliteDB.Exec("CREATE TABLE t_int (a INTEGER)")
	sqlvibeDB.Exec("CREATE TABLE t_real (a REAL)")
	sqliteDB.Exec("CREATE TABLE t_real (a REAL)")
	sqlvibeDB.Exec("CREATE TABLE t_decimal (a DECIMAL(10,2))")
	sqliteDB.Exec("CREATE TABLE t_decimal (a DECIMAL(10,2))")
	sqlvibeDB.Exec("CREATE TABLE t_numeric (a NUMERIC(10,2))")
	sqliteDB.Exec("CREATE TABLE t_numeric (a NUMERIC(10,2))")

	sqlvibeDB.Exec("CREATE TABLE num_test (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, dec_val DECIMAL(10,2), num_val NUMERIC(10,2))")
	sqliteDB.Exec("CREATE TABLE num_test (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, dec_val DECIMAL(10,2), num_val NUMERIC(10,2))")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertInt", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (1, 10, 0, 0, 0)"},
		{"InsertReal", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (2, 0, 3.14, 0, 0)"},
		{"InsertDecimal", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (3, 0, 0, 25.50, 0)"},
		{"InsertNumeric", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (4, 0, 0, 0, 30.75)"},
		{"InsertMixed", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (5, 5, 2.5, 10.0, 15.0)"},
		{"InsertNegative", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (6, -10, -5.5, -20.0, -25.0)"},
		{"InsertZero", "INSERT INTO num_test (id, int_val, real_val, dec_val, num_val) VALUES (7, 0, 0.0, 0.0, 0.0)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM num_test ORDER BY id", "VerifyInserts")

	implicitConvTests := []struct {
		name string
		sql  string
	}{
		{"IntToReal", "SELECT int_val + 0.5 FROM num_test WHERE id = 1"},
		{"RealToInt", "SELECT real_val * 2 FROM num_test WHERE id = 2"},
		{"IntToDecimal", "SELECT int_val + 1.23 FROM num_test WHERE id = 1"},
		{"DecimalToReal", "SELECT dec_val / 2.0 FROM num_test WHERE id = 3"},
		{"RealToDecimal", "SELECT real_val + 1.0 FROM num_test WHERE id = 2"},
		{"IntDecimalMix", "SELECT int_val + dec_val FROM num_test WHERE id = 4"},
		{"NumericToReal", "SELECT num_val + 1.0 FROM num_test WHERE id = 4"},
		{"IntNumericMix", "SELECT int_val + num_val FROM num_test WHERE id = 5"},
		{"AllTypes", "SELECT int_val + real_val + dec_val + num_val FROM num_test WHERE id = 5"},
		{"NegativeCasting", "SELECT int_val + -5.5 FROM num_test WHERE id = 1"},
		{"ZeroCasting", "SELECT int_val + 0.0 FROM num_test WHERE id = 1"},
	}

	for _, tt := range implicitConvTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	arithmeticTests := []struct {
		name string
		sql  string
	}{
		{"IntRealAdd", "SELECT 10 + 5.5"},
		{"IntRealSub", "SELECT 20 - 3.14"},
		{"IntRealMul", "SELECT 7 * 2.5"},
		{"IntRealDiv", "SELECT 100 / 3.0"},
		{"RealDecimalAdd", "SELECT 2.5 + 1.23"},
		{"RealDecimalMul", "SELECT 1.5 * 2.50"},
		{"IntDecimalMul", "SELECT 5 * 10.25"},
		{"DecimalDivInt", "SELECT 100.0 / 4"},
		{"NegativeReal", "SELECT -10 + 5.5"},
		{"NegativeDecimal", "SELECT -20 - 3.14"},
		{"ZeroAdd", "SELECT 0 + 5.5"},
		{"ZeroMul", "SELECT 0 * 5.5"},
		{"OneMul", "SELECT 1 * 5.5"},
		{"OneAdd", "SELECT 1 + 5.5"},
	}

	for _, tt := range arithmeticTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertCastTests := []struct {
		name string
		sql  string
	}{
		{"InsertRealIntoInt", "INSERT INTO t_int VALUES (3.7)"},
		{"InsertIntIntoReal", "INSERT INTO t_real VALUES (5)"},
		{"InsertDecimalIntoInt", "INSERT INTO t_int VALUES (7.9)"},
		{"InsertIntIntoDecimal", "INSERT INTO t_decimal VALUES (10)"},
		{"InsertRealIntoDecimal", "INSERT INTO t_decimal VALUES (5.5)"},
		{"InsertIntIntoNumeric", "INSERT INTO t_numeric VALUES (15)"},
		{"InsertDecimalIntoNumeric", "INSERT INTO t_numeric VALUES (25.75)"},
		{"InsertNegativeReal", "INSERT INTO t_int VALUES (-3.7)"},
		{"InsertNegativeDecimal", "INSERT INTO t_decimal VALUES (-8.25)"},
	}

	for _, tt := range insertCastTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t_int ORDER BY a", "VerifyIntTable")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t_real ORDER BY a", "VerifyRealTable")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t_decimal ORDER BY a", "VerifyDecimalTable")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t_numeric ORDER BY a", "VerifyNumericTable")

	comparisonTests := []struct {
		name string
		sql  string
	}{
		{"IntVsReal", "SELECT 10 > 5.5"},
		{"RealVsInt", "SELECT 3.14 < 10"},
		{"IntVsDecimal", "SELECT 5 > 3.25"},
		{"DecimalVsInt", "SELECT 2.5 < 8"},
		{"RealVsDecimal", "SELECT 1.5 > 1.25"},
		{"MixedCompare", "SELECT 10 + 5.0 = 15"},
		{"IntVsNumeric", "SELECT 10 > 5.5"},
		{"RealVsNumeric", "SELECT 3.14 < 5.5"},
		{"CompareNegative", "SELECT -10 < 5.5"},
		{"CompareZero", "SELECT 0 > -0.5"},
	}

	for _, tt := range comparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	resultTypeTests := []struct {
		name string
		sql  string
	}{
		{"AddIntReal", "SELECT typeof(10 + 5.5)"},
		{"SubIntDecimal", "SELECT typeof(10 - 1.23)"},
		{"MulRealInt", "SELECT typeof(2.5 * 3)"},
		{"DivIntReal", "SELECT typeof(10 / 4.0)"},
		{"DivMixed", "SELECT typeof(10.0 / 4)"},
		{"ModWithReal", "SELECT typeof(10 % 3.0)"},
		{"NegIntPlusReal", "SELECT typeof(-10 + 5.5)"},
		{"ZeroPlusReal", "SELECT typeof(0 + 5.5)"},
	}

	for _, tt := range resultTypeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	complexArithmetic := []struct {
		name string
		sql  string
	}{
		{"ChainMixed", "SELECT 1 + 2.5 + 3"},
		{"MulAddMixed", "SELECT 2 * 3 + 4.5"},
		{"DivMulMixed", "SELECT 10.0 / 2 * 3"},
		{"NestedMixed", "SELECT (1 + 2.5) * (3 - 1.0)"},
		{"FuncMixed", "SELECT ABS(-5) + 2.5"},
		{"CaseMixed", "SELECT CASE WHEN 1 > 0.5 THEN 10 ELSE 5.5 END"},
		{"ComplexChain", "SELECT 1 + 2.5 * 3 - 4.0 / 2"},
		{"NestedParens", "SELECT ((1 + 2) * 3) + 4.5"},
		{"MultipleTypes", "SELECT 1 + 2.5 + 3 + 4.5 + 5"},
		{"NegativeChain", "SELECT -1 + -2.5 + -3"},
	}

	for _, tt := range complexArithmetic {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	functionWithCasting := []struct {
		name string
		sql  string
	}{
		{"ABS_IntReal", "SELECT ABS(-10 + 5.5)"},
		{"ROUND_IntReal", "SELECT ROUND(10.5 + 0.5)"},
		{"CEIL_IntReal", "SELECT CEIL(5 + 2.5)"},
		{"FLOOR_IntReal", "SELECT FLOOR(8 + 1.9)"},
		{"FuncInFunc", "SELECT ABS(ROUND(-5.6 + 0.5))"},
	}

	for _, tt := range functionWithCasting {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE expr_test (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE expr_test (id INTEGER PRIMARY KEY, val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO expr_test VALUES (1, 10), (2, 20), (3, 30)")
	sqliteDB.Exec("INSERT INTO expr_test VALUES (1, 10), (2, 20), (3, 30)")

	exprCastInColumns := []struct {
		name string
		sql  string
	}{
		{"ColPlusReal", "SELECT val + 5.0 FROM expr_test ORDER BY id"},
		{"ColMinusReal", "SELECT val - 5.5 FROM expr_test ORDER BY id"},
		{"ColMulReal", "SELECT val * 2.5 FROM expr_test ORDER BY id"},
		{"ColDivReal", "SELECT val / 4.0 FROM expr_test ORDER BY id"},
		{"RealPlusCol", "SELECT 5.0 + val FROM expr_test ORDER BY id"},
		{"RealMulCol", "SELECT 2.5 * val FROM expr_test ORDER BY id"},
		{"ComplexCol", "SELECT (val + 5.0) * 2 - 10.0 FROM expr_test ORDER BY id"},
	}

	for _, tt := range exprCastInColumns {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
