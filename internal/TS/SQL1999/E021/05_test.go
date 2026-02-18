package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02105_L1(t *testing.T) {
	t.Skip("Known pre-existing failure: OCTET_LENGTH not supported by SQLite - documented in v0.4.5")
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

	sqlvibeDB.Exec("CREATE TABLE octet_test (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE octet_test (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Hello", "INSERT INTO octet_test VALUES (1, 'hello')"},
		{"Empty", "INSERT INTO octet_test VALUES (2, '')"},
		{"Unicode", "INSERT INTO octet_test VALUES (3, 'café')"},
		{"Chinese", "INSERT INTO octet_test VALUES (4, '你好')"},
		{"Japanese", "INSERT INTO octet_test VALUES (5, 'こんにちは')"},
		{"Emoji", "INSERT INTO octet_test VALUES (6, '😀')"},
		{"Mixed", "INSERT INTO octet_test VALUES (7, 'Hello世界')"},
		{"Numbers", "INSERT INTO octet_test VALUES (8, '1234567890')"},
		{"Special", "INSERT INTO octet_test VALUES (9, 'test@#$%')"},
		{"Long", "INSERT INTO octet_test VALUES (10, 'a very long string with many characters for testing')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	octetTests := []struct {
		name string
		sql  string
	}{
		{"OCTET_LENGTH", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 1"},
		{"OCTET_LENGTH_Empty", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 2"},
		{"OCTET_LENGTH_Unicode", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 3"},
		{"OCTET_LENGTH_Chinese", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 4"},
		{"OCTET_LENGTH_Japanese", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 5"},
		{"OCTET_LENGTH_Emoji", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 6"},
		{"OCTET_LENGTH_Mixed", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 7"},
		{"OCTET_LENGTH_Numbers", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 8"},
		{"OCTET_LENGTH_Special", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 9"},
		{"OCTET_LENGTH_Long", "SELECT OCTET_LENGTH(val) FROM octet_test WHERE id = 10"},
		{"OCTET_LENGTH_Literal", "SELECT OCTET_LENGTH('hello')"},
		{"LENGTH_ForComparison", "SELECT LENGTH(val) FROM octet_test WHERE id = 1"},
		{"LENGTH_Unicode", "SELECT LENGTH(val) FROM octet_test WHERE id = 3"},
	}

	for _, tt := range octetTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
