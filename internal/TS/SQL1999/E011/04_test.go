package E011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01104_L1(t *testing.T) {
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

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE arith_test (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)", "CreateTable")

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO arith_test VALUES (1, 10, 3)", "Insert1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO arith_test VALUES (2, 20, 4)", "Insert2")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO arith_test VALUES (3, -5, 7)", "Insert3")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO arith_test VALUES (4, 0, 8)", "Insert4")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO arith_test VALUES (5, 100, 50)", "Insert5")

	basicArithTests := []struct {
		name string
		sql  string
	}{
		{"Addition", "SELECT a + b FROM arith_test WHERE id = 1"},
		{"Subtraction", "SELECT a - b FROM arith_test WHERE id = 1"},
		{"Multiplication", "SELECT a * b FROM arith_test WHERE id = 1"},
		{"Division", "SELECT a / b FROM arith_test WHERE id = 1"},
		{"Modulo", "SELECT a % b FROM arith_test WHERE id = 1"},
		{"UnaryMinus", "SELECT -a FROM arith_test WHERE id = 1"},
		{"UnaryMinusAll", "SELECT -a, -b FROM arith_test WHERE id = 3"},
		{"AdditionNegative", "SELECT a + b FROM arith_test WHERE id = 3"},
		{"SubtractionNegative", "SELECT a - b FROM arith_test WHERE id = 3"},
		{"MultiplicationNegative", "SELECT a * b FROM arith_test WHERE id = 3"},
		{"ZeroAddition", "SELECT a + b FROM arith_test WHERE id = 4"},
		{"ZeroSubtraction", "SELECT a - b FROM arith_test WHERE id = 4"},
		{"ZeroMultiplication", "SELECT a * b FROM arith_test WHERE id = 4"},
		{"LargeNumbers", "SELECT a + b, a - b, a * b, a / b FROM arith_test WHERE id = 5"},
	}

	for _, tt := range basicArithTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	precedenceTests := []struct {
		name string
		sql  string
	}{
		{"MulBeforeAdd", "SELECT 2 + 3 * 4"},
		{"MulBeforeSub", "SELECT 10 - 2 * 3"},
		{"DivBeforeAdd", "SELECT 1 + 10 / 2"},
		{"ModBeforeAdd", "SELECT 1 + 10 % 3"},
		{"AllOperators", "SELECT 1 + 2 * 3 - 4 / 2"},
		{"ParenOverride", "SELECT (1 + 2) * 3"},
		{"NestedParens", "SELECT (1 + (2 * 3))"},
		{"ComplexExpr", "SELECT 10 + 20 * 30 - 40 / 2 % 5"},
	}

	for _, tt := range precedenceTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE real_test (id INTEGER PRIMARY KEY, x REAL, y REAL)", "CreateRealTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO real_test VALUES (1, 10.5, 3.2)", "InsertReal1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO real_test VALUES (2, -5.5, 2.0)", "InsertReal2")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO real_test VALUES (3, 0.0, 4.5)", "InsertReal3")

	realArithTests := []struct {
		name string
		sql  string
	}{
		{"RealAddition", "SELECT x + y FROM real_test WHERE id = 1"},
		{"RealSubtraction", "SELECT x - y FROM real_test WHERE id = 1"},
		{"RealMultiplication", "SELECT x * y FROM real_test WHERE id = 1"},
		{"RealDivision", "SELECT x / y FROM real_test WHERE id = 1"},
		{"RealNegative", "SELECT x + y FROM real_test WHERE id = 2"},
		{"RealZero", "SELECT x + y FROM real_test WHERE id = 3"},
		{"RealUnaryMinus", "SELECT -x FROM real_test WHERE id = 1"},
	}

	for _, tt := range realArithTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE mixed_test (id INTEGER PRIMARY KEY, i INTEGER, r REAL)", "CreateMixedTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO mixed_test VALUES (1, 10, 3.5)", "InsertMixed1")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO mixed_test VALUES (2, 5, 2.5)", "InsertMixed2")

	mixedTypeTests := []struct {
		name string
		sql  string
	}{
		{"IntPlusReal", "SELECT i + r FROM mixed_test WHERE id = 1"},
		{"IntMinusReal", "SELECT i - r FROM mixed_test WHERE id = 1"},
		{"IntTimesReal", "SELECT i * r FROM mixed_test WHERE id = 1"},
		{"IntDivReal", "SELECT i / r FROM mixed_test WHERE id = 1"},
		{"RealPlusInt", "SELECT r + i FROM mixed_test WHERE id = 1"},
		{"RealMinusInt", "SELECT r - i FROM mixed_test WHERE id = 1"},
		{"MixedInExpr", "SELECT i + r * 2 FROM mixed_test WHERE id = 1"},
		{"MixedWithNegative", "SELECT i + r FROM mixed_test WHERE id = 2"},
	}

	for _, tt := range mixedTypeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	constantExprTests := []struct {
		name string
		sql  string
	}{
		{"ConstantAddition", "SELECT 5 + 3"},
		{"ConstantSubtraction", "SELECT 10 - 4"},
		{"ConstantMultiplication", "SELECT 6 * 7"},
		{"ConstantDivision", "SELECT 20 / 4"},
		{"ConstantModulo", "SELECT 17 % 5"},
		{"ConstantAllOps", "SELECT 1 + 2 * 3 - 4 / 2 + 10 % 3"},
		{"ConstantNegative", "SELECT -10 + 5"},
		{"ConstantDoubleNegative", "SELECT -(-5)"},
		{"ConstantZero", "SELECT 0 + 0"},
		{"ConstantOne", "SELECT 1 * 1 + 1 - 1"},
	}

	for _, tt := range constantExprTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	columnArithTests := []struct {
		name string
		sql  string
	}{
		{"ColPlusCol", "SELECT a + b FROM arith_test ORDER BY id"},
		{"ColMinusCol", "SELECT a - b FROM arith_test ORDER BY id"},
		{"ColTimesCol", "SELECT a * b FROM arith_test ORDER BY id"},
		{"ColDivCol", "SELECT a / b FROM arith_test ORDER BY id"},
		{"ColModCol", "SELECT a % b FROM arith_test ORDER BY id"},
		{"ColNegate", "SELECT -a FROM arith_test ORDER BY id"},
		{"ColChainOps", "SELECT a + b * a - b FROM arith_test WHERE id = 1"},
	}

	for _, tt := range columnArithTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE divzero_test (id INTEGER PRIMARY KEY, val INTEGER)", "CreateDivZeroTable")
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "INSERT INTO divzero_test VALUES (1, 10)", "InsertDivZero")

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT val / 0 FROM divzero_test", "DivisionByZeroInt")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT val % 0 FROM divzero_test", "ModuloByZero")
}
