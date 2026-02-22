package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
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
		{"UpperAAA", "INSERT INTO comp_test VALUES (7, 'AAA')"},
		{"Numbers", "INSERT INTO comp_test VALUES (8, '123')"},
		{"AlphaNum", "INSERT INTO comp_test VALUES (9, 'abc123')"},
		{"Special", "INSERT INTO comp_test VALUES (10, '!@#$')"},
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
		{"EqualEmpty", "SELECT val = '' FROM comp_test WHERE id = 5"},
		{"NotEqualEmpty", "SELECT val != '' FROM comp_test WHERE id = 1"},
		{"LessThanEmpty", "SELECT val < 'a' FROM comp_test WHERE id = 5"},
		{"GreaterThanEmpty", "SELECT val > '' FROM comp_test WHERE id = 1"},
		{"CaseSensitive", "SELECT val = 'AAA' FROM comp_test WHERE id = 1"},
		{"CaseInsensitive", "SELECT val = 'AAA' FROM comp_test WHERE id = 7"},
		{"Between", "SELECT val BETWEEN 'aaa' AND 'bbb' FROM comp_test WHERE id = 1"},
		{"NotBetween", "SELECT val NOT BETWEEN 'aaa' AND 'bbb' FROM comp_test WHERE id = 4"},
		{"InList", "SELECT val IN ('aaa', 'bbb', 'ccc') FROM comp_test WHERE id = 1"},
		{"NotInList", "SELECT val NOT IN ('aaa', 'bbb') FROM comp_test WHERE id = 4"},
		{"LikePattern", "SELECT val LIKE 'a%' FROM comp_test WHERE id = 1"},
		{"LikeUnderscore", "SELECT val LIKE 'a__' FROM comp_test WHERE id = 1"},
		{"NotLike", "SELECT val NOT LIKE 'a%' FROM comp_test WHERE id = 2"},
		{"LikeEscape", "SELECT val LIKE 'a%' FROM comp_test WHERE id = 3"},
		{"LikeNumbers", "SELECT val LIKE '%123%' FROM comp_test WHERE id = 9"},
		{"LikeSpecial", "SELECT val LIKE '%@%' FROM comp_test WHERE id = 10"},
		{"GLOB_Pattern", "SELECT val GLOB '*a*' FROM comp_test WHERE id = 1"},
		{"GLOB_Upper", "SELECT val GLOB '*A*' FROM comp_test WHERE id = 1"},
		{"OrderByMultiple", "SELECT val FROM comp_test ORDER BY val ASC, id DESC"},
		{"OrderByWithWhere", "SELECT val FROM comp_test WHERE id > 3 ORDER BY val ASC"},
		{"CompareConcat", "SELECT val || 'suffix' = 'aaasuffix' FROM comp_test WHERE id = 1"},
		{"CompareFunction", "SELECT UPPER(val) = 'AAA' FROM comp_test WHERE id = 1"},
		{"CompareInFunction", "SELECT LOWER(val) IN ('aaa', 'bbb') FROM comp_test WHERE id = 1"},
	}

	for _, tt := range compTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
