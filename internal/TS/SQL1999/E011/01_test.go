package E011

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01101_L1(t *testing.T) {
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
		{"INTEGER", "CREATE TABLE t1 (a INTEGER)"},
		{"INT", "CREATE TABLE t2 (a INT)"},
		{"SMALLINT", "CREATE TABLE t3 (a SMALLINT)"},
		{"BIGINT", "CREATE TABLE t4 (a BIGINT)"},
		{"TINYINT", "CREATE TABLE t5 (a TINYINT)"},
		{"MEDIUMINT", "CREATE TABLE t6 (a MEDIUMINT)"},
		{"INT2", "CREATE TABLE t7 (a INT2)"},
		{"INT8", "CREATE TABLE t8 (a INT8)"},
		{"AllIntegerTypes", "CREATE TABLE t9 (a INTEGER, b INT, c SMALLINT, d BIGINT)"},
		{"MultipleColumns", "CREATE TABLE t10 (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)"},
		{"WithPK", "CREATE TABLE t11 (id INTEGER PRIMARY KEY, val INTEGER)"},
		{"WithMultiplePK", "CREATE TABLE t12 (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY (a, b))"},
		{"WithNotNull", "CREATE TABLE t13 (a INTEGER NOT NULL)"},
		{"WithDefault", "CREATE TABLE t14 (a INTEGER DEFAULT 0)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE integers (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE integers (id INTEGER PRIMARY KEY, val INTEGER)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Positive", "INSERT INTO integers VALUES (1, 42)"},
		{"Negative", "INSERT INTO integers VALUES (2, -17)"},
		{"Zero", "INSERT INTO integers VALUES (3, 0)"},
		{"Large", "INSERT INTO integers VALUES (4, 2147483647)"},
		{"Small", "INSERT INTO integers VALUES (5, -2147483648)"},
		{"MaxInt", "INSERT INTO integers VALUES (6, 9223372036854775807)"},
		{"MinInt", "INSERT INTO integers VALUES (7, -9223372036854775808)"},
		{"One", "INSERT INTO integers VALUES (8, 1)"},
		{"NegativeOne", "INSERT INTO integers VALUES (9, -1)"},
		{"PowersOf2", "INSERT INTO integers VALUES (10, 1024)"},
		{"PowersOf10", "INSERT INTO integers VALUES (11, 1000000000)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM integers ORDER BY id", "VerifyIntegers")

	exprTests := []struct {
		name string
		sql  string
	}{
		{"Add", "SELECT val + 10 FROM integers WHERE id = 1"},
		{"Sub", "SELECT val - 5 FROM integers WHERE id = 1"},
		{"Mul", "SELECT val * 2 FROM integers WHERE id = 1"},
		{"Div", "SELECT val / 2 FROM integers WHERE id = 1"},
		{"Mod", "SELECT val % 10 FROM integers WHERE id = 1"},
		{"Negate", "SELECT -val FROM integers WHERE id = 1"},
		{"AddNegative", "SELECT val + -5 FROM integers WHERE id = 1"},
		{"SubNegative", "SELECT val - -3 FROM integers WHERE id = 1"},
		{"MulNegative", "SELECT val * -2 FROM integers WHERE id = 1"},
		{"DivNegative", "SELECT val / -2 FROM integers WHERE id = 1"},
		{"ModNegative", "SELECT val % -3 FROM integers WHERE id = 1"},
		{"DoubleNegate", "SELECT -(-val) FROM integers WHERE id = 1"},
		{"AddZero", "SELECT val + 0 FROM integers WHERE id = 1"},
		{"MulOne", "SELECT val * 1 FROM integers WHERE id = 1"},
		{"MulZero", "SELECT val * 0 FROM integers WHERE id = 1"},
	}

	for _, tt := range exprTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE int_math (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	sqliteDB.Exec("CREATE TABLE int_math (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")

	sqlvibeDB.Exec("INSERT INTO int_math VALUES (1, 10, 3)")
	sqliteDB.Exec("INSERT INTO int_math VALUES (1, 10, 3)")
	sqlvibeDB.Exec("INSERT INTO int_math VALUES (2, -5, 2)")
	sqliteDB.Exec("INSERT INTO int_math VALUES (2, -5, 2)")
	sqlvibeDB.Exec("INSERT INTO int_math VALUES (3, 100, 7)")
	sqliteDB.Exec("INSERT INTO int_math VALUES (3, 100, 7)")

	columnMathTests := []struct {
		name string
		sql  string
	}{
		{"AddCol", "SELECT a + b FROM int_math WHERE id = 1"},
		{"SubCol", "SELECT a - b FROM int_math WHERE id = 1"},
		{"MulCol", "SELECT a * b FROM int_math WHERE id = 1"},
		{"DivCol", "SELECT a / b FROM int_math WHERE id = 1"},
		{"ModCol", "SELECT a % b FROM int_math WHERE id = 1"},
		{"ChainedOps", "SELECT a + b * 2 FROM int_math WHERE id = 1"},
		{"ParenOps", "SELECT (a + b) * 2 FROM int_math WHERE id = 1"},
		{"NegativeCol", "SELECT a + -b FROM int_math WHERE id = 1"},
		{"AllNegatives", "SELECT -a + -b FROM int_math WHERE id = 2"},
		{"ComplexExpr", "SELECT (a + b) * (a - b) FROM int_math WHERE id = 1"},
	}

	for _, tt := range columnMathTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
