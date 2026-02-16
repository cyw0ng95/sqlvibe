package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
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
	}

	for _, tt := range substrTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
