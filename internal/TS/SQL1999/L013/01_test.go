package L013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_L013_L01301_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, s TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, s TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10, 3, 'hello')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10, 3, 'hello')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 20, 7, 'world')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 20, 7, 'world')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 5, 5, 'foo')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 5, 5, 'foo')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, 0, 0, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, 0, 0, NULL)")

	queryTests := []struct {
		name string
		sql  string
	}{
		// Arithmetic expressions
		{"ArithAdd", "SELECT id, a + b AS sum FROM t1 ORDER BY id"},
		{"ArithSub", "SELECT id, a - b AS diff FROM t1 ORDER BY id"},
		{"ArithMul", "SELECT id, a * b AS prod FROM t1 ORDER BY id"},
		{"ArithDiv", "SELECT id, a / b AS quot FROM t1 WHERE b != 0 ORDER BY id"},
		{"ArithMod", "SELECT id, a % b AS mod FROM t1 WHERE b != 0 ORDER BY id"},
		// String expressions
		{"StrConcat", "SELECT id, s || '!' AS exclaim FROM t1 WHERE s IS NOT NULL ORDER BY id"},
		{"StrLiteral", "SELECT 'hello' || ' ' || 'world'"},
		// CASE expressions
		{"CaseSimple", "SELECT id, CASE WHEN a > 10 THEN 'big' WHEN a > 5 THEN 'medium' ELSE 'small' END AS size FROM t1 ORDER BY id"},
		{"CaseNullCheck", "SELECT id, CASE WHEN s IS NULL THEN 'null' ELSE s END AS val FROM t1 ORDER BY id"},
		{"CaseArith", "SELECT id, CASE WHEN a = b THEN 'equal' ELSE 'not equal' END AS eq FROM t1 ORDER BY id"},
		// Comparison expressions
		{"CompareEq", "SELECT id, a = b AS is_eq FROM t1 ORDER BY id"},
		{"CompareGt", "SELECT id, a > b AS is_gt FROM t1 ORDER BY id"},
		// Literal expressions
		{"LiteralInt", "SELECT 42"},
		{"LiteralStr", "SELECT 'test'"},
		{"LiteralArith", "SELECT 2 + 3 * 4"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
