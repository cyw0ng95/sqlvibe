package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06115_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE products (id INTEGER, name TEXT, price REAL, category TEXT, stock INTEGER)"},
		{"InsertData", "INSERT INTO products VALUES (1, 'Apple', 1.99, 'Fruit', 100), (2, 'Banana', 0.99, 'Fruit', 150), (3, 'Orange', 1.49, 'Fruit', 200), (4, 'Carrot', 0.79, 'Vegetable', 80), (5, 'Bread', 2.49, 'Bakery', 50), (6, 'Milk', 3.99, 'Dairy', 60), (7, 'Cheese', 5.99, 'Dairy', 40), (8, 'Yogurt', 1.99, 'Dairy', 70)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	betweenTests := []struct {
		name string
		sql  string
	}{
		{"BetweenNumbers", "SELECT * FROM products WHERE price BETWEEN 1.00 AND 2.00"},
		{"BetweenStrings", "SELECT * FROM products WHERE name BETWEEN 'Apple' AND 'Cheese'"},
		{"BetweenExclusive", "SELECT * FROM products WHERE id BETWEEN 2 AND 5"},
		{"BetweenWithNot", "SELECT * FROM products WHERE price NOT BETWEEN 1.00 AND 3.00"},
		{"BetweenNull", "SELECT * FROM products WHERE price BETWEEN NULL AND 10.00"},
		{"BetweenExpression", "SELECT * FROM products WHERE price * 10 BETWEEN 10 AND 20"},
		{"BetweenColumns", "SELECT * FROM products WHERE id BETWEEN 1 AND stock"},
		{"MultipleBetween", "SELECT * FROM products WHERE price BETWEEN 1.00 AND 2.00 AND stock BETWEEN 50 AND 100"},
		{"BetweenOrderBy", "SELECT * FROM products WHERE price BETWEEN 1.00 AND 3.00 ORDER BY price"},
		{"BetweenWithGroupBy", "SELECT category, COUNT(*) FROM products WHERE price BETWEEN 1.00 AND 3.00 GROUP BY category"},
	}

	for _, tt := range betweenTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E06116_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary REAL)"},
		{"InsertData", "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 75000), (2, 'Bob', 'Engineering', 80000), (3, 'Charlie', 'Sales', 65000), (4, 'Diana', 'HR', 60000), (5, 'Eve', 'Marketing', 70000), (6, 'Frank', 'Sales', 68000), (7, 'Grace', 'Engineering', 82000), (8, 'Henry', 'HR', 62000)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	inTests := []struct {
		name string
		sql  string
	}{
		{"InNumbers", "SELECT * FROM employees WHERE id IN (1, 3, 5, 7)"},
		{"InStrings", "SELECT * FROM employees WHERE department IN ('Engineering', 'HR')"},
		{"InWithNot", "SELECT * FROM employees WHERE department NOT IN ('Sales', 'Marketing')"},
		{"InWithSubquery", "SELECT * FROM employees WHERE id IN (SELECT id FROM employees WHERE salary > 70000)"},
		{"InNull", "SELECT * FROM employees WHERE department IN (NULL)"},
		{"InSingleValue", "SELECT * FROM employees WHERE id IN (3)"},
		{"InAllValues", "SELECT * FROM employees WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8)"},
		{"InDuplicateValues", "SELECT * FROM employees WHERE id IN (1, 1, 2, 2, 3)"},
		{"InOrderBy", "SELECT * FROM employees WHERE department IN ('Engineering', 'Sales') ORDER BY salary DESC"},
		{"InWithGroupBy", "SELECT department, COUNT(*) FROM employees WHERE department IN ('Engineering', 'Sales') GROUP BY department"},
	}

	for _, tt := range inTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	likeTests := []struct {
		name string
		sql  string
	}{
		{"LikePattern", "SELECT * FROM employees WHERE name LIKE 'A%'"},
		{"LikeContains", "SELECT * FROM employees WHERE name LIKE '%a%'"},
		{"LikeEndsWith", "SELECT * FROM employees WHERE name LIKE '%e'"},
		{"LikeNot", "SELECT * FROM employees WHERE name NOT LIKE 'A%'"},
		{"LikeCaseSensitive", "SELECT * FROM employees WHERE name LIKE 'a%'"},
		{"LikeEscape", "SELECT * FROM employees WHERE name LIKE 'A%%' ESCAPE '%'"},
		{"LikeMultiple", "SELECT * FROM employees WHERE name LIKE '%a%' AND department LIKE 'E%'"},
		{"LikeOrderBy", "SELECT * FROM employees WHERE name LIKE 'A%' ORDER BY name"},
		{"LikeLimit", "SELECT * FROM employees WHERE name LIKE '%a%' LIMIT 5"},
		{"LikeWithGroupBy", "SELECT department, COUNT(*) FROM employees WHERE name LIKE '%a%' GROUP BY department"},
	}

	for _, tt := range likeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	isNullTests := []struct {
		name string
		sql  string
	}{
		{"IsNull", "SELECT * FROM employees WHERE name IS NULL"},
		{"IsNotNull", "SELECT * FROM employees WHERE name IS NOT NULL"},
		{"IsNullWithComparison", "SELECT * FROM employees WHERE name IS NULL AND department = 'Engineering'"},
		{"IsNotNullWithComparison", "SELECT * FROM employees WHERE name IS NOT NULL AND salary > 70000"},
		{"IsNullInSubquery", "SELECT * FROM employees WHERE id IN (SELECT id FROM employees WHERE name IS NULL)"},
		{"IsNullOrderBy", "SELECT * FROM employees WHERE name IS NULL ORDER BY id"},
		{"IsNullWithLimit", "SELECT * FROM employees WHERE name IS NOT NULL LIMIT 3"},
		{"IsNullWithGroupBy", "SELECT department, COUNT(*) FROM employees WHERE name IS NOT NULL GROUP BY department"},
		{"IsNullWithHaving", "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 2"},
	}

	for _, tt := range isNullTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
