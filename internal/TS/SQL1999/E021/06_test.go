package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02106_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE substr_test (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE substr_test (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Hello", "INSERT INTO substr_test VALUES (1, 'hello')"},
		{"World", "INSERT INTO substr_test VALUES (2, 'world')"},
		{"Empty", "INSERT INTO substr_test VALUES (3, '')"},
		{"Numbers", "INSERT INTO substr_test VALUES (4, '1234567890')"},
		{"Spaces", "INSERT INTO substr_test VALUES (5, '  hello  ')"},
		{"Special", "INSERT INTO substr_test VALUES (6, 'a,b,c,d,e')"},
		{"Long", "INSERT INTO substr_test VALUES (7, 'abcdefghijklmnopqrstuvwxyz')"},
		{"Unicode", "INSERT INTO substr_test VALUES (8, '你好世界')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	substrTests := []struct {
		name string
		sql  string
	}{
		{"Substr_From1", "SELECT SUBSTR(val, 1) FROM substr_test WHERE id = 1"},
		{"Substr_From2", "SELECT SUBSTR(val, 2) FROM substr_test WHERE id = 1"},
		{"Substr_From3_Len2", "SELECT SUBSTR(val, 3, 2) FROM substr_test WHERE id = 1"},
		{"Substr_From1_Len2", "SELECT SUBSTR(val, 1, 2) FROM substr_test WHERE id = 1"},
		{"Substr_From2_Len3", "SELECT SUBSTR(val, 2, 3) FROM substr_test WHERE id = 1"},
		{"Substr_OutOfRange", "SELECT SUBSTR(val, 10, 5) FROM substr_test WHERE id = 1"},
		{"Substr_Empty", "SELECT SUBSTR(val, 1) FROM substr_test WHERE id = 3"},
		{"SUBSTRING", "SELECT SUBSTRING(val, 1, 2) FROM substr_test WHERE id = 1"},
		{"SUBSTRING_From3", "SELECT SUBSTRING(val, 3) FROM substr_test WHERE id = 1"},
		{"SUBSTRING_From1_Len3", "SELECT SUBSTRING(val, 1, 3) FROM substr_test WHERE id = 1"},
		{"Substr_Negative", "SELECT SUBSTR(val, -1) FROM substr_test WHERE id = 1"},
		{"Substr_Negative_Len", "SELECT SUBSTR(val, -3, 2) FROM substr_test WHERE id = 1"},
		{"Substr_Zero", "SELECT SUBSTR(val, 0, 5) FROM substr_test WHERE id = 1"},
		{"Substr_Long", "SELECT SUBSTR(val, 1, 10) FROM substr_test WHERE id = 7"},
		{"Substr_Long_From", "SELECT SUBSTR(val, 10) FROM substr_test WHERE id = 7"},
		{"Substr_Unicode", "SELECT SUBSTR(val, 1, 2) FROM substr_test WHERE id = 8"},
		{"Substr_Special", "SELECT SUBSTR(val, 1, 3) FROM substr_test WHERE id = 6"},
		{"Substr_Literal", "SELECT SUBSTR('hello world', 1, 5)"},
		{"Substr_Literal2", "SELECT SUBSTR('hello world', 7)"},
		{"Substr_AfterComma", "SELECT SUBSTR(val, 3) FROM substr_test WHERE id = 6"},
	}

	for _, tt := range substrTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
