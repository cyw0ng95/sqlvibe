package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02103_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE literals (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE literals (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"SimpleString", "INSERT INTO literals VALUES (1, 'hello')"},
		{"EmptyString", "INSERT INTO literals VALUES (2, '')"},
		{"WithSpaces", "INSERT INTO literals VALUES (3, 'hello world')"},
		{"WithNumbers", "INSERT INTO literals VALUES (4, 'abc123def')"},
		{"WithSpecialChars", "INSERT INTO literals VALUES (5, 'test@#$%')"},
		{"WithDoubleQuotes", "INSERT INTO literals VALUES (6, 'say \"hello\"')"},
		{"WithNewline", "INSERT INTO literals VALUES (8, 'line1\nline2')"},
		{"WithTab", "INSERT INTO literals VALUES (9, 'col1\tcol2')"},
		{"Unicode", "INSERT INTO literals VALUES (10, 'café naïve résumé')"},
		{"Chinese", "INSERT INTO literals VALUES (11, '你好世界')"},
		{"Japanese", "INSERT INTO literals VALUES (12, 'こんにちは')"},
		{"Emoji", "INSERT INTO literals VALUES (13, 'hello😀world')"},
		{"Mixed", "INSERT INTO literals VALUES (14, 'Test123!@# αβγδ')"},
		{"Backslash", "INSERT INTO literals VALUES (15, 'path\\\\to\\\\file')"},
		{"Percent", "INSERT INTO literals VALUES (16, '100%')"},
		{"Underscore", "INSERT INTO literals VALUES (17, 'hello_world')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM literals ORDER BY id", "VerifyLiterals")

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectLiteral", "SELECT 'hello world'"},
		{"SelectEmptyLiteral", "SELECT ''"},
		{"SelectNumberLiteral", "SELECT '123'"},
		{"SelectMixedLiteral", "SELECT 'abc' || '123'"},
		{"SelectConcatLiterals", "SELECT 'Hello' || ' ' || 'World'"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
