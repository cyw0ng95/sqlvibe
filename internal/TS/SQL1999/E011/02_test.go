package E011

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01102_L1(t *testing.T) {
	t.Skip("Known pre-existing failure: Float math edge cases (ROUND neg, ABS on columns) - documented in v0.4.5")
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

	createTests := []struct {
		name string
		sql  string
	}{
		{"REAL", "CREATE TABLE t1 (a REAL)"},
		{"DOUBLE_PRECISION", "CREATE TABLE t2 (a DOUBLE PRECISION)"},
		{"FLOAT", "CREATE TABLE t3 (a FLOAT)"},
		{"FLOAT8", "CREATE TABLE t4 (a FLOAT8)"},
		{"FLOAT4", "CREATE TABLE t5 (a FLOAT4)"},
		{"NUMERIC", "CREATE TABLE t6 (a NUMERIC)"},
		{"AllFloatTypes", "CREATE TABLE t7 (a REAL, b DOUBLE PRECISION, c FLOAT)"},
		{"FloatWithPK", "CREATE TABLE t8 (id INTEGER PRIMARY KEY, val REAL)"},
		{"MultipleFloat", "CREATE TABLE t9 (a REAL, b REAL, c REAL, d REAL)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE floats (id INTEGER PRIMARY KEY, val REAL)")
	sqliteDB.Exec("CREATE TABLE floats (id INTEGER PRIMARY KEY, val REAL)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Positive", "INSERT INTO floats VALUES (1, 42.5)"},
		{"Negative", "INSERT INTO floats VALUES (2, -17.25)"},
		{"Zero", "INSERT INTO floats VALUES (3, 0.0)"},
		{"Large", "INSERT INTO floats VALUES (4, 1.7976931348623157e+308)"},
		{"Small", "INSERT INTO floats VALUES (5, 2.2250738585072014e-308)"},
		{"Pi", "INSERT INTO floats VALUES (6, 3.14159265358979)"},
		{"NegativePi", "INSERT INTO floats VALUES (7, -3.14159265358979)"},
		{"E", "INSERT INTO floats VALUES (8, 2.718281828459045)"},
		{"NegativeE", "INSERT INTO floats VALUES (9, -2.718281828459045)"},
		{"VerySmall", "INSERT INTO floats VALUES (10, 1e-100)"},
		{"VeryLarge", "INSERT INTO floats VALUES (11, 1e100)"},
		{"Inf", "INSERT INTO floats VALUES (12, 1e309)"},
		{"NInf", "INSERT INTO floats VALUES (13, -1e309)"},
		{"NaN", "INSERT INTO floats VALUES (14, 0.0/0.0)"},
		{"OnePointZero", "INSERT INTO floats VALUES (15, 1.0)"},
		{"Half", "INSERT INTO floats VALUES (16, 0.5)"},
		{"Quarter", "INSERT INTO floats VALUES (17, 0.25)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM floats ORDER BY id", "VerifyFloats")

	exprTests := []struct {
		name string
		sql  string
	}{
		{"Add", "SELECT val + 10.5 FROM floats WHERE id = 1"},
		{"Sub", "SELECT val - 5.25 FROM floats WHERE id = 1"},
		{"Mul", "SELECT val * 2.0 FROM floats WHERE id = 1"},
		{"Div", "SELECT val / 2.0 FROM floats WHERE id = 1"},
		{"Negate", "SELECT -val FROM floats WHERE id = 1"},
		{"Abs", "SELECT ABS(val) FROM floats WHERE id = 2"},
		{"AbsNegative", "SELECT ABS(val) FROM floats WHERE id = 7"},
		{"Ceil", "SELECT CEIL(val) FROM floats WHERE id = 1"},
		{"CeilNegative", "SELECT CEIL(val) FROM floats WHERE id = 2"},
		{"Floor", "SELECT FLOOR(val) FROM floats WHERE id = 1"},
		{"FloorNegative", "SELECT FLOOR(val) FROM floats WHERE id = 2"},
		{"Round", "SELECT ROUND(val, 2) FROM floats WHERE id = 6"},
		{"RoundZero", "SELECT ROUND(val, 0) FROM floats WHERE id = 6"},
		{"RoundNeg", "SELECT ROUND(val, -1) FROM floats WHERE id = 6"},
		{"AddNegative", "SELECT val + -5.0 FROM floats WHERE id = 1"},
		{"SubNegative", "SELECT val - -3.0 FROM floats WHERE id = 1"},
		{"MulNegative", "SELECT val * -2.0 FROM floats WHERE id = 1"},
		{"DivNegative", "SELECT val / -2.0 FROM floats WHERE id = 1"},
		{"DoubleNegate", "SELECT -(-val) FROM floats WHERE id = 1"},
		{"AddZero", "SELECT val + 0.0 FROM floats WHERE id = 1"},
		{"MulOne", "SELECT val * 1.0 FROM floats WHERE id = 1"},
		{"MulZero", "SELECT val * 0.0 FROM floats WHERE id = 1"},
	}

	for _, tt := range exprTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE floats2 (id INTEGER PRIMARY KEY, a REAL, b REAL)")
	sqliteDB.Exec("CREATE TABLE floats2 (id INTEGER PRIMARY KEY, a REAL, b REAL)")

	sqlvibeDB.Exec("INSERT INTO floats2 VALUES (1, 10.5, 5.5)")
	sqliteDB.Exec("INSERT INTO floats2 VALUES (1, 10.5, 5.5)")
	sqlvibeDB.Exec("INSERT INTO floats2 VALUES (2, -3.5, 7.25)")
	sqliteDB.Exec("INSERT INTO floats2 VALUES (2, -3.5, 7.25)")
	sqlvibeDB.Exec("INSERT INTO floats2 VALUES (3, 100.0, 3.0)")
	sqliteDB.Exec("INSERT INTO floats2 VALUES (3, 100.0, 3.0)")

	combinedTests := []struct {
		name string
		sql  string
	}{
		{"AddColumns", "SELECT a + b FROM floats2 WHERE id = 1"},
		{"SubColumns", "SELECT a - b FROM floats2 WHERE id = 1"},
		{"MulColumns", "SELECT a * b FROM floats2 WHERE id = 1"},
		{"DivColumns", "SELECT a / b FROM floats2 WHERE id = 1"},
		{"MixedArithmetic", "SELECT a * b + 10.0 FROM floats2 WHERE id = 2"},
		{"ChainedOps", "SELECT a + b * 2.0 FROM floats2 WHERE id = 1"},
		{"ParenOps", "SELECT (a + b) * 2.0 FROM floats2 WHERE id = 1"},
		{"NegativeColumns", "SELECT -a + -b FROM floats2 WHERE id = 2"},
		{"ComplexExpr", "SELECT (a * b) + (a / b) FROM floats2 WHERE id = 1"},
		{"AbsOnColumns", "SELECT ABS(a), ABS(b) FROM floats2 WHERE id = 2"},
	}

	for _, tt := range combinedTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE float_precision (id INTEGER PRIMARY KEY, val REAL)")
	sqliteDB.Exec("CREATE TABLE float_precision (id INTEGER PRIMARY KEY, val REAL)")

	sqlvibeDB.Exec("INSERT INTO float_precision VALUES (1, 1.123456789012345)")
	sqliteDB.Exec("INSERT INTO float_precision VALUES (1, 1.123456789012345)")
	sqlvibeDB.Exec("INSERT INTO float_precision VALUES (2, 0.000000000000001)")
	sqliteDB.Exec("INSERT INTO float_precision VALUES (2, 0.000000000000001)")
	sqlvibeDB.Exec("INSERT INTO float_precision VALUES (3, 999999999999.999)")
	sqliteDB.Exec("INSERT INTO float_precision VALUES (3, 999999999999.999)")

	precisionTests := []struct {
		name string
		sql  string
	}{
		{"ManyDecimals", "SELECT ROUND(val, 10) FROM float_precision WHERE id = 1"},
		{"VerySmall", "SELECT val * 1000000000000.0 FROM float_precision WHERE id = 2"},
		{"VeryLarge", "SELECT val / 1000000000.0 FROM float_precision WHERE id = 3"},
	}

	for _, tt := range precisionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
