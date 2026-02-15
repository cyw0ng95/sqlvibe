package E011

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01102_L1(t *testing.T) {
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
		{"AllFloatTypes", "CREATE TABLE t4 (a REAL, b DOUBLE PRECISION, c FLOAT)"},
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
		{"Ceil", "SELECT ABS(val) FROM floats WHERE id = 1"},
		{"Floor", "SELECT ABS(val) FROM floats WHERE id = 1"},
		{"Round", "SELECT ROUND(val, 2) FROM floats WHERE id = 6"},
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

	combinedTests := []struct {
		name string
		sql  string
	}{
		{"AddColumns", "SELECT a + b FROM floats2 WHERE id = 1"},
		{"SubColumns", "SELECT a - b FROM floats2 WHERE id = 1"},
		{"MulColumns", "SELECT a * b FROM floats2 WHERE id = 1"},
		{"DivColumns", "SELECT a / b FROM floats2 WHERE id = 1"},
		{"MixedArithmetic", "SELECT a * b + 10.0 FROM floats2 WHERE id = 2"},
	}

	for _, tt := range combinedTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
