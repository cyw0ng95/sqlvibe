package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02112_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE comp_test (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE comp_test (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"AAA", "INSERT INTO comp_test VALUES (1, 'aaa')"},
		{"BBB", "INSERT INTO comp_test VALUES (2, 'bbb')"},
		{"ABC", "INSERT INTO comp_test VALUES (3, 'abc')"},
		{"XYZ", "INSERT INTO comp_test VALUES (4, 'xyz')"},
		{"Empty", "INSERT INTO comp_test VALUES (5, '')"},
		{"Same", "INSERT INTO comp_test VALUES (6, 'aaa')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	compTests := []struct {
		name string
		sql  string
	}{
		{"Equal", "SELECT val = 'aaa' FROM comp_test WHERE id = 1"},
		{"NotEqual", "SELECT val != 'bbb' FROM comp_test WHERE id = 1"},
		{"LessThan", "SELECT val < 'bbb' FROM comp_test WHERE id = 1"},
		{"GreaterThan", "SELECT val > 'aaa' FROM comp_test WHERE id = 2"},
		{"LessOrEqual", "SELECT val <= 'aaa' FROM comp_test WHERE id = 1"},
		{"GreaterOrEqual", "SELECT val >= 'bbb' FROM comp_test WHERE id = 2"},
		{"OrderByAsc", "SELECT val FROM comp_test WHERE id IN (1,2,3,4) ORDER BY val ASC"},
		{"OrderByDesc", "SELECT val FROM comp_test WHERE id IN (1,2,3,4) ORDER BY val DESC"},
		{"NullComparison", "SELECT val IS NULL FROM comp_test WHERE id = 5"},
		{"NotNullComparison", "SELECT val IS NOT NULL FROM comp_test WHERE id = 1"},
	}

	for _, tt := range compTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
