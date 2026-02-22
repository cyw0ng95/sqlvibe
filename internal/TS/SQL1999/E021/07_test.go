package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02107_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE concat_test (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
	sqliteDB.Exec("CREATE TABLE concat_test (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"HelloWorld", "INSERT INTO concat_test VALUES (1, 'Hello', 'World')"},
		{"EmptyString", "INSERT INTO concat_test VALUES (2, '', 'World')"},
		{"SpaceString", "INSERT INTO concat_test VALUES (3, 'Hello ', 'World')"},
		{"Numbers", "INSERT INTO concat_test VALUES (4, '123', '456')"},
		{"EmptyBoth", "INSERT INTO concat_test VALUES (5, '', '')"},
		{"NULLValue", "INSERT INTO concat_test VALUES (6, NULL, 'test')"},
		{"Unicode", "INSERT INTO concat_test VALUES (7, '你好', '世界')"},
		{"Long", "INSERT INTO concat_test VALUES (8, 'This is a long string ', 'that continues here')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	concatTests := []struct {
		name string
		sql  string
	}{
		{"ConcatColumns", "SELECT a || b FROM concat_test WHERE id = 1"},
		{"ConcatLiteral", "SELECT 'Hello' || ' ' || 'World'"},
		{"ConcatWithSpace", "SELECT a || ' ' || b FROM concat_test WHERE id = 1"},
		{"ConcatEmpty", "SELECT a || b FROM concat_test WHERE id = 2"},
		{"ConcatNumbers", "SELECT a || b FROM concat_test WHERE id = 4"},
		{"ConcatThree", "SELECT a || b || '!' FROM concat_test WHERE id = 1"},
		{"ConcatWithNumbers", "SELECT a || '123' FROM concat_test WHERE id = 1"},
		{"ConcatEmptyBoth", "SELECT a || b FROM concat_test WHERE id = 5"},
		{"ConcatNULL", "SELECT a || b FROM concat_test WHERE id = 6"},
		{"ConcatUnicode", "SELECT a || b FROM concat_test WHERE id = 7"},
		{"ConcatLong", "SELECT a || b FROM concat_test WHERE id = 8"},
		{"ConcatWithNumbers2", "SELECT 'num:' || 123"},
		{"ConcatMultiple", "SELECT 'a' || 'b' || 'c' || 'd' || 'e'"},
		{"ConcatSpaceLiteral", "SELECT ' ' || 'test' || ' '"},
		{"ConcatAt", "SELECT 'user@' || 'example.com'"},
		{"ConcatWithParen", "SELECT '(' || 'test' || ')'"},
	}

	for _, tt := range concatTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
