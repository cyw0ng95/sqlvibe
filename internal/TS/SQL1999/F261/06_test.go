package F261

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F262_F26104_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE products (id INTEGER, name TEXT, category TEXT, price REAL, quantity INTEGER)"},
		{"InsertData", "INSERT INTO products VALUES (1, 'Apple', 'Fruit', 1.99, 100), (2, 'Banana', 'Fruit', 0.99, 150), (3, 'Orange', 'Fruit', 1.49, 200), (4, 'Carrot', 'Vegetable', 0.79, 80), (5, 'Bread', 'Bakery', 2.49, 50), (6, NULL, 'Bakery', 5.99, 30)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	coalesceTests := []struct {
		name string
		sql  string
	}{
		{"CoalesceBasic", "SELECT COALESCE(name, 'Unknown') FROM products"},
		{"CoalesceNull", "SELECT COALESCE(NULL, 'Default') FROM products WHERE id = 1"},
		{"CoalesceMultiple", "SELECT COALESCE(NULL, NULL, 'Final') FROM products WHERE id = 1"},
		{"CoalesceColumns", "SELECT COALESCE(name, category) FROM products WHERE id = 6"},
		{"CoalesceExpression", "SELECT COALESCE(LOWER(name), 'N/A') FROM products"},
		{"CoalesceWithNumbers", "SELECT COALESCE(price, 0.00) FROM products"},
		{"CoalesceMixed", "SELECT COALESCE(name, 'Product #' || id) FROM products WHERE id = 6"},
		{"CoalesceAllNull", "SELECT COALESCE(NULL, NULL) FROM products WHERE id = 1"},
		{"CoalesceInWhere", "SELECT * FROM products WHERE COALESCE(name, 'Unknown') = 'Unknown'"},
		{"CoalesceInOrderBy", "SELECT id, COALESCE(name, 'Unknown') FROM products ORDER BY COALESCE(name, 'Unknown')"},
	}

	for _, tt := range coalesceTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F263_F26105_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE test_table (id INTEGER, val INTEGER)"},
		{"InsertData", "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 20), (4, 30), (5, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullifTests := []struct {
		name string
		sql  string
	}{
		{"NullifEqual", "SELECT NULLIF(val, 20) FROM test_table"},
		{"NullifNotEqual", "SELECT NULLIF(val, 99) FROM test_table"},
		{"NullifNull", "SELECT NULLIF(val, NULL) FROM test_table WHERE id = 5"},
		{"NullifBothNull", "SELECT NULLIF(NULL, NULL) FROM test_table WHERE id = 1"},
		{"NullifExpression", "SELECT NULLIF(val * 2, 40) FROM test_table WHERE id = 2"},
		{"NullifStrings", "SELECT NULLIF('hello', 'hello') FROM test_table"},
		{"NullifDifferentStrings", "SELECT NULLIF('hello', 'world') FROM test_table"},
		{"NullifInWhere", "SELECT * FROM test_table WHERE NULLIF(val, 20) IS NULL"},
		{"NullifInGroupBy", "SELECT NULLIF(val, 20), COUNT(*) FROM test_table GROUP BY NULLIF(val, 20)"},
		{"NullifInOrderBy", "SELECT id, val FROM test_table ORDER BY NULLIF(val, 20) IS NULL DESC"},
		{"NullifWithCoalesce", "SELECT COALESCE(NULLIF(val, 20), 0) FROM test_table"},
		{"NullifInCalculation", "SELECT val + NULLIF(val, 20) FROM test_table"},
		{"NullifComparison", "SELECT NULLIF(val > 20, 0) FROM test_table"},
		{"NullifCase", "SELECT NULLIF(CASE WHEN val = 20 THEN 1 ELSE 0 END, 0) FROM test_table"},
	}

	for _, tt := range nullifTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F264_F26106_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER, grade TEXT)"},
		{"InsertData", "INSERT INTO scores VALUES (1, 'Alice', 95, 'A'), (2, 'Bob', 85, 'B'), (3, 'Charlie', 75, 'C'), (4, 'Diana', 65, 'D'), (5, 'Eve', 55, 'F'), (6, 'Frank', 85, 'B'), (7, 'Grace', 95, 'A'), (8, NULL, 75, 'C')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	caseTests := []struct {
		name string
		sql  string
	}{
		{"SimpleCase", "SELECT name, CASE WHEN score >= 90 THEN 'Excellent' WHEN score >= 80 THEN 'Good' WHEN score >= 70 THEN 'Average' ELSE 'Poor' END FROM scores"},
		{"SimpleCaseDefault", "SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'D' END FROM scores"},
		{"SearchedCase", "SELECT name, CASE score WHEN 95 THEN 'Excellent' WHEN 85 THEN 'Good' ELSE 'Average' END FROM scores"},
		{"CaseWithNull", "SELECT name, CASE WHEN name IS NULL THEN 'Unknown' ELSE name END FROM scores"},
		{"CaseInWhere", "SELECT * FROM scores WHERE CASE WHEN score >= 80 THEN 1 ELSE 0 END = 1"},
		{"CaseInOrderBy", "SELECT * FROM scores ORDER BY CASE WHEN score >= 90 THEN 1 WHEN score >= 80 THEN 2 ELSE 3 END"},
		{"CaseInGroupBy", "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END, COUNT(*) FROM scores GROUP BY CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END"},
		{"CaseInHaving", "SELECT CASE WHEN score >= 80 THEN 'Pass' ELSE 'Fail' END, COUNT(*) FROM scores GROUP BY CASE WHEN score >= 80 THEN 'Pass' ELSE 'Fail' END HAVING COUNT(*) > 1"},
		{"CaseWithExpression", "SELECT name, CASE WHEN score + 5 >= 100 THEN 'Bonus' ELSE 'No Bonus' END FROM scores"},
		{"CaseWithMultiple", "SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END FROM scores"},
		{"CaseWithSubquery", "SELECT name, score, CASE WHEN score > (SELECT AVG(score) FROM scores) THEN 'Above Average' ELSE 'Below Average' END FROM scores"},
		{"NestedCase", "SELECT name, CASE WHEN score >= 80 THEN CASE WHEN grade = 'A' THEN 'High A' ELSE 'High B' END ELSE 'Low' END FROM scores"},
		{"CaseWithAnd", "SELECT name, CASE WHEN score >= 80 AND score < 90 THEN 'High B' ELSE 'Other' END FROM scores"},
		{"CaseWithOr", "SELECT name, CASE WHEN score >= 90 OR grade = 'A' THEN 'Excellent' ELSE 'Good' END FROM scores"},
		{"CaseInSelectList", "SELECT id, name, score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS letter_grade FROM scores"},
		{"CaseWithAggregates", "SELECT CASE WHEN COUNT(*) > 2 THEN 'Many' ELSE 'Few' END, AVG(score) FROM scores WHERE CASE WHEN score >= 80 THEN 1 ELSE 0 END = 1 GROUP BY CASE WHEN COUNT(*) > 2 THEN 'Many' ELSE 'Few' END"},
		{"CaseNullResult", "SELECT name, CASE WHEN score = 100 THEN 'Perfect' ELSE NULL END FROM scores"},
		{"CaseWithCoalesce", "SELECT COALESCE(CASE WHEN grade = 'A' THEN 'Excellent' WHEN grade = 'B' THEN 'Good' ELSE 'Average' END, 'N/A') FROM scores"},
		{"CaseInJoinCondition", "SELECT s1.name, s2.name FROM scores s1 JOIN scores s2 ON s1.score < s2.score AND CASE WHEN s1.grade = 'A' THEN 1 ELSE 0 END = 1"},
		{"CaseWithDistinct", "SELECT DISTINCT CASE WHEN score >= 90 THEN 'High' ELSE 'Low' END FROM scores"},
		{"CaseInLimit", "SELECT name FROM scores ORDER BY score DESC LIMIT CASE WHEN (SELECT COUNT(*) FROM scores) > 5 THEN 5 ELSE 10 END"},
		{"CaseStringComparison", "SELECT name, CASE WHEN grade = 'A' THEN 'Excellent' WHEN grade = 'B' THEN 'Good' WHEN grade = 'C' THEN 'Average' ELSE 'Poor' END FROM scores"},
		{"CaseWithFunctions", "SELECT name, CASE WHEN UPPER(grade) = 'A' THEN 'Great' ELSE 'OK' END FROM scores"},
		{"CaseInUpdate", "UPDATE scores SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'D' END WHERE grade IS NULL"},
	}

	for _, tt := range caseTests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "CaseInUpdate" {
				SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
			} else {
				SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
			}
		})
	}
}
