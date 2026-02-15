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
		{"AllIntegerTypes", "CREATE TABLE t5 (a INTEGER, b INT, c SMALLINT, d BIGINT)"},
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
	}

	for _, tt := range exprTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
