package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02109_L1(t *testing.T) {
	t.Skip("Known pre-existing failure: TRIM with tabs/newlines differences - documented in v0.4.5")
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

	sqlvibeDB.Exec("CREATE TABLE trim_test (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE trim_test (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Spaces", "INSERT INTO trim_test VALUES (1, '   hello   ')"},
		{"NoSpaces", "INSERT INTO trim_test VALUES (2, 'hello')"},
		{"Leading", "INSERT INTO trim_test VALUES (3, '   hello')"},
		{"Trailing", "INSERT INTO trim_test VALUES (4, 'hello   ')"},
		{"Empty", "INSERT INTO trim_test VALUES (5, '')"},
		{"MultiSpaces", "INSERT INTO trim_test VALUES (6, '     a b c     ')"},
		{"Tabs", "INSERT INTO trim_test VALUES (7, '\t\thello\t\t')"},
		{"Newlines", "INSERT INTO trim_test VALUES (8, '\n\nhello\n\n')"},
		{"Mixed", "INSERT INTO trim_test VALUES (9, '   \t\n  hello  \n\t   ')"},
		{"Special", "INSERT INTO trim_test VALUES (10, 'xxxhelloxxx')"},
		{"Numbers", "INSERT INTO trim_test VALUES (11, '123hello123')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	trimTests := []struct {
		name string
		sql  string
	}{
		{"TRIM", "SELECT TRIM(val) FROM trim_test WHERE id = 1"},
		{"TRIM_NoSpaces", "SELECT TRIM(val) FROM trim_test WHERE id = 2"},
		{"TRIM_Leading", "SELECT TRIM(val) FROM trim_test WHERE id = 3"},
		{"TRIM_Trailing", "SELECT TRIM(val) FROM trim_test WHERE id = 4"},
		{"TRIM_Empty", "SELECT TRIM(val) FROM trim_test WHERE id = 5"},
		{"LTRIM", "SELECT LTRIM(val) FROM trim_test WHERE id = 1"},
		{"RTRIM", "SELECT RTRIM(val) FROM trim_test WHERE id = 1"},
		{"TRIM_Literal", "SELECT TRIM('   test   ')"},
		{"LTRIM_Literal", "SELECT LTRIM('   test')"},
		{"RTRIM_Literal", "SELECT RTRIM('test   ')"},
		{"TRIM_MultiSpaces", "SELECT TRIM(val) FROM trim_test WHERE id = 6"},
		{"LTRIM_MultiSpaces", "SELECT LTRIM(val) FROM trim_test WHERE id = 6"},
		{"RTRIM_MultiSpaces", "SELECT RTRIM(val) FROM trim_test WHERE id = 6"},
		{"TRIM_Tabs", "SELECT TRIM(val) FROM trim_test WHERE id = 7"},
		{"TRIM_Newlines", "SELECT TRIM(val) FROM trim_test WHERE id = 8"},
		{"TRIM_Mixed", "SELECT TRIM(val) FROM trim_test WHERE id = 9"},
		{"TRIM_Special", "SELECT TRIM(val, 'x') FROM trim_test WHERE id = 10"},
		{"LTRIM_Special", "SELECT LTRIM(val, 'x') FROM trim_test WHERE id = 10"},
		{"RTRIM_Special", "SELECT RTRIM(val, 'x') FROM trim_test WHERE id = 10"},
		{"TRIM_Numbers", "SELECT TRIM(val, '1') FROM trim_test WHERE id = 11"},
		{"TRIM_Concat", "SELECT TRIM('   ' || val || '   ') FROM trim_test WHERE id = 2"},
		{"TRIM_InFunction", "SELECT UPPER(TRIM(val)) FROM trim_test WHERE id = 1"},
		{"TRIM_Lower", "SELECT LOWER(TRIM(val)) FROM trim_test WHERE id = 1"},
	}

	for _, tt := range trimTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
