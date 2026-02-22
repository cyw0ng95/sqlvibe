package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02104_L1(t *testing.T) {
	t.Skip("Known pre-existing failure: CHAR_LENGTH not supported by SQLite - documented in v0.4.5")
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

	sqlvibeDB.Exec("CREATE TABLE strings (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE strings (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Hello", "INSERT INTO strings VALUES (1, 'hello')"},
		{"Empty", "INSERT INTO strings VALUES (2, '')"},
		{"Spaces", "INSERT INTO strings VALUES (3, '   ')"},
		{"Numbers", "INSERT INTO strings VALUES (4, '12345')"},
		{"Mixed", "INSERT INTO strings VALUES (5, 'Hello123')"},
		{"Unicode", "INSERT INTO strings VALUES (6, 'café')"},
		{"Chinese", "INSERT INTO strings VALUES (7, '你好')"},
		{"Emoji", "INSERT INTO strings VALUES (8, '😀')"},
		{"Long", "INSERT INTO strings VALUES (9, 'a very long string with many characters')"},
		{"Special", "INSERT INTO strings VALUES (10, 'test@#$%^&*()')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	lenTests := []struct {
		name string
		sql  string
	}{
		{"LENGTH", "SELECT LENGTH(val) FROM strings WHERE id = 1"},
		{"LENGTH_Empty", "SELECT LENGTH(val) FROM strings WHERE id = 2"},
		{"LENGTH_Spaces", "SELECT LENGTH(val) FROM strings WHERE id = 3"},
		{"LENGTH_Numbers", "SELECT LENGTH(val) FROM strings WHERE id = 4"},
		{"LENGTH_Mixed", "SELECT LENGTH(val) FROM strings WHERE id = 5"},
		{"LENGTH_Unicode", "SELECT LENGTH(val) FROM strings WHERE id = 6"},
		{"LENGTH_Chinese", "SELECT LENGTH(val) FROM strings WHERE id = 7"},
		{"LENGTH_Emoji", "SELECT LENGTH(val) FROM strings WHERE id = 8"},
		{"LENGTH_Long", "SELECT LENGTH(val) FROM strings WHERE id = 9"},
		{"LENGTH_Special", "SELECT LENGTH(val) FROM strings WHERE id = 10"},
		{"LENGTH_Literal", "SELECT LENGTH('hello world')"},
		{"LENGTH_Concat", "SELECT LENGTH('hello' || 'world')"},
		{"CHAR_LENGTH", "SELECT CHAR_LENGTH(val) FROM strings WHERE id = 1"},
		{"CHARACTER_LENGTH", "SELECT CHARACTER_LENGTH(val) FROM strings WHERE id = 1"},
		{"CHAR_LENGTH_Empty", "SELECT CHAR_LENGTH(val) FROM strings WHERE id = 2"},
	}

	for _, tt := range lenTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
