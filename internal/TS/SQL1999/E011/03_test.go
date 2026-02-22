package E011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01103_L1(t *testing.T) {
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
		{"DECIMAL_Precision", "CREATE TABLE t1 (a DECIMAL(10))"},
		{"DECIMAL_PrecisionScale", "CREATE TABLE t2 (a DECIMAL(10,2))"},
		{"DECIMAL_ZeroScale", "CREATE TABLE t3 (a DECIMAL(10,0))"},
		{"DECIMAL_MaxPrecision", "CREATE TABLE t4 (a DECIMAL(18,6))"},
		{"NUMERIC_Precision", "CREATE TABLE t5 (a NUMERIC(10))"},
		{"NUMERIC_PrecisionScale", "CREATE TABLE t6 (a NUMERIC(10,2))"},
		{"NUMERIC_ZeroScale", "CREATE TABLE t7 (a NUMERIC(10,0))"},
		{"NUMERIC_MaxPrecision", "CREATE TABLE t8 (a NUMERIC(18,6))"},
		{"AllDecimalTypes", "CREATE TABLE t9 (a DECIMAL(5,2), b NUMERIC(10,3), c DECIMAL(15,4))"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE decimals (id INTEGER PRIMARY KEY, val DECIMAL(10,2))")
	sqliteDB.Exec("CREATE TABLE decimals (id INTEGER PRIMARY KEY, val DECIMAL(10,2))")

	sqlvibeDB.Exec("CREATE TABLE numerics (id INTEGER PRIMARY KEY, val NUMERIC(10,2))")
	sqliteDB.Exec("CREATE TABLE numerics (id INTEGER PRIMARY KEY, val NUMERIC(10,2))")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"DecimalPositive", "INSERT INTO decimals VALUES (1, 123.45)"},
		{"DecimalNegative", "INSERT INTO decimals VALUES (2, -67.89)"},
		{"DecimalZero", "INSERT INTO decimals VALUES (3, 0.00)"},
		{"DecimalLarge", "INSERT INTO decimals VALUES (4, 99999999.99)"},
		{"DecimalSmall", "INSERT INTO decimals VALUES (5, -99999999.99)"},
		{"DecimalWhole", "INSERT INTO decimals VALUES (6, 100.00)"},
		{"NumericPositive", "INSERT INTO numerics VALUES (1, 123.45)"},
		{"NumericNegative", "INSERT INTO numerics VALUES (2, -67.89)"},
		{"NumericZero", "INSERT INTO numerics VALUES (3, 0.00)"},
		{"NumericLarge", "INSERT INTO numerics VALUES (4, 99999999.99)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM decimals ORDER BY id", "VerifyDecimals")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM numerics ORDER BY id", "VerifyNumerics")

	sqlvibeDB.Exec("CREATE TABLE scale_test (id INTEGER PRIMARY KEY, val DECIMAL(5,2))")
	sqliteDB.Exec("CREATE TABLE scale_test (id INTEGER PRIMARY KEY, val DECIMAL(5,2))")

	scaleTests := []struct {
		name string
		sql  string
	}{
		{"ScaleExceedTruncate", "INSERT INTO scale_test VALUES (1, 123.456)"},
		{"ScaleExceedRound", "INSERT INTO scale_test VALUES (2, 123.457)"},
		{"ScaleExact", "INSERT INTO scale_test VALUES (3, 123.45)"},
		{"ScaleMoreDigits", "INSERT INTO scale_test VALUES (4, 1234.567)"},
		{"ScaleNegativeExceed", "INSERT INTO scale_test VALUES (5, -123.456)"},
	}

	for _, tt := range scaleTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM scale_test ORDER BY id", "VerifyScaleTest")

	exprTests := []struct {
		name string
		sql  string
	}{
		{"DecimalAdd", "SELECT val + 10.5 FROM decimals WHERE id = 1"},
		{"DecimalSub", "SELECT val - 5.25 FROM decimals WHERE id = 1"},
		{"DecimalMul", "SELECT val * 2 FROM decimals WHERE id = 1"},
		{"DecimalDiv", "SELECT val / 4 FROM decimals WHERE id = 1"},
		{"DecimalNegate", "SELECT -val FROM decimals WHERE id = 1"},
		{"DecimalAbs", "SELECT ABS(val) FROM decimals WHERE id = 2"},
		{"NumericAdd", "SELECT val + 10.5 FROM numerics WHERE id = 1"},
		{"NumericMul", "SELECT val * 2 FROM numerics WHERE id = 1"},
		{"ScaleTestAdd", "SELECT val + 0.1 FROM scale_test WHERE id = 1"},
		{"ScaleTestMul", "SELECT val * 2 FROM scale_test WHERE id = 1"},
	}

	for _, tt := range exprTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE decimal_ops (id INTEGER PRIMARY KEY, a DECIMAL(10,2), b DECIMAL(10,2))")
	sqliteDB.Exec("CREATE TABLE decimal_ops (id INTEGER PRIMARY KEY, a DECIMAL(10,2), b DECIMAL(10,2))")

	sqlvibeDB.Exec("INSERT INTO decimal_ops VALUES (1, 100.50, 50.25)")
	sqliteDB.Exec("INSERT INTO decimal_ops VALUES (1, 100.50, 50.25)")

	decimalOpsTests := []struct {
		name string
		sql  string
	}{
		{"AddColumns", "SELECT a + b FROM decimal_ops WHERE id = 1"},
		{"SubColumns", "SELECT a - b FROM decimal_ops WHERE id = 1"},
		{"MulColumns", "SELECT a * b FROM decimal_ops WHERE id = 1"},
		{"DivColumns", "SELECT a / b FROM decimal_ops WHERE id = 1"},
	}

	for _, tt := range decimalOpsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE precision_test (id INTEGER PRIMARY KEY, val DECIMAL(5,2))")
	sqliteDB.Exec("CREATE TABLE precision_test (id INTEGER PRIMARY KEY, val DECIMAL(5,2))")

	precisionTests := []struct {
		name string
		sql  string
	}{
		{"PrecisionMax", "INSERT INTO precision_test VALUES (1, 999.99)"},
		{"PrecisionOverflow", "INSERT INTO precision_test VALUES (2, 100000.00)"},
		{"PrecisionNegativeOverflow", "INSERT INTO precision_test VALUES (3, -100000.00)"},
	}

	for _, tt := range precisionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
