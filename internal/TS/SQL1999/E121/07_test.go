package E121

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E12107_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE products (id INTEGER, name TEXT, price REAL, quantity INTEGER, category TEXT)"},
		{"InsertData", "INSERT INTO products VALUES (1, 'Apple', 1.99, 100, 'Fruit'), (2, 'Banana', 0.99, 150, 'Fruit'), (3, 'Orange', 1.49, 200, 'Fruit'), (4, 'Milk', 3.99, 50, 'Dairy'), (5, 'Cheese', 5.99, 30, 'Dairy'), (6, 'Bread', 2.49, 40, 'Bakery'), (7, 'Yogurt', 1.99, 70, 'Dairy')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	orderByTests := []struct {
		name string
		sql  string
	}{
		{"OrderBySingleColumn", "SELECT id, name, price FROM products ORDER BY price"},
		{"OrderBySingleColumnDesc", "SELECT id, name, price FROM products ORDER BY price DESC"},
		{"OrderByMultipleColumns", "SELECT id, name, price FROM products ORDER BY price, name"},
		{"OrderByMultipleColumnsDesc", "SELECT id, name, price FROM products ORDER BY price DESC, name DESC"},
		{"OrderByExpression", "SELECT id, name, price FROM products ORDER BY price * quantity"},
		{"OrderByExpressionDesc", "SELECT id, name, price FROM products ORDER BY price * quantity DESC"},
		{"OrderByCase", "SELECT id, name, price FROM products ORDER BY CASE WHEN category = 'Fruit' THEN 0 ELSE 1 END, name"},
		{"OrderByFunction", "SELECT id, name, price FROM products ORDER BY LENGTH(name)"},
		{"OrderByCast", "SELECT id, name, price FROM products ORDER BY CAST(price AS INTEGER)"},
		{"OrderByNullsFirst", "SELECT id, name, price FROM products WHERE category = 'Bakery' ORDER BY quantity IS NULL FIRST"},
		{"OrderByNullsLast", "SELECT id, name, price FROM products WHERE category = 'Dairy' ORDER BY quantity IS NULL LAST"},
		{"OrderByLimit", "SELECT id, name, price FROM products ORDER BY price DESC LIMIT 3"},
		{"OrderByLimitOffset", "SELECT id, name, price FROM products ORDER BY price LIMIT 3 OFFSET 2"},
		{"OrderByWithWhere", "SELECT id, name, price FROM products WHERE price > 2.00 ORDER BY price"},
		{"OrderByDistinct", "SELECT DISTINCT category, AVG(price) FROM products ORDER BY AVG(price) DESC"},
		{"OrderByGroupBy", "SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category ORDER BY AVG(price) DESC"},
		{"OrderByWithExpressionWhere", "SELECT id, name, price FROM products WHERE price > (SELECT AVG(price) FROM products WHERE category = 'Fruit') ORDER BY price"},
		{"OrderByComplex", "SELECT id, name, price, quantity FROM products ORDER BY price / quantity DESC"},
		{"OrderBySubstring", "SELECT id, name, SUBSTR(name, 1, 1) AS first_char, price FROM products ORDER BY first_char"},
		{"OrderByRandom", "SELECT id, name, price FROM products ORDER BY RANDOM()"},
	}

	for _, tt := range orderByTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E12108_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE test_data (id INTEGER, a INTEGER, b INTEGER, c INTEGER)"},
		{"InsertData", "INSERT INTO test_data VALUES (1, 10, 20, 30), (2, 15, 25, 35), (3, 20, 40, 60), (4, 25, 50, 75), (5, 30, 60, 90), (6, 35, 70, 105), (7, 40, 80, 120)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	expressionOrderByTests := []struct {
		name string
		sql  string
	}{
		{"OrderBySum", "SELECT * FROM test_data ORDER BY a + b + c"},
		{"OrderBySumDesc", "SELECT * FROM test_data ORDER BY a + b + c DESC"},
		{"OrderByDifference", "SELECT * FROM test_data ORDER BY a - b"},
		{"OrderByProduct", "SELECT * FROM test_data ORDER BY a * b"},
		{"OrderByDivision", "SELECT * FROM test_data ORDER BY a / b"},
		{"OrderByModulo", "SELECT * FROM test_data ORDER BY a % b"},
		{"OrderByAvg", "SELECT * FROM test_data ORDER BY (a + b + c) / 3"},
		{"OrderByAbs", "SELECT * FROM test_data ORDER BY ABS(a - b)"},
		{"OrderByFunction", "SELECT * FROM test_data ORDER BY ROUND(a + b + c, -2)"},
		{"OrderByExpressionCase", "SELECT * FROM test_data ORDER BY CASE WHEN a > b THEN 1 ELSE 0 END"},
		{"OrderByMultipleExpr", "SELECT * FROM test_data ORDER BY a * 2 + b * 3"},
		{"OrderByComplexExpr", "SELECT * FROM test_data ORDER BY (a + b) * c DESC"},
		{"OrderByNestedExpr", "SELECT * FROM test_data ORDER BY a * (b + c)"},
		{"OrderByWithWhere", "SELECT * FROM test_data WHERE a > 20 ORDER BY a"},
		{"OrderByWithLimit", "SELECT * FROM test_data ORDER BY a + b + c LIMIT 3"},
		{"OrderByWithOffset", "SELECT * FROM test_data ORDER BY id LIMIT 3 OFFSET 2"},
		{"OrderByNullExpression", "SELECT * FROM test_data WHERE a IS NULL ORDER BY IS NULL"},
		{"OrderByConstant", "SELECT * FROM test_data ORDER BY 100"},
		{"OrderByNegative", "SELECT * FROM test_data ORDER BY -a"},
		{"OrderBySquare", "SELECT * FROM test_data ORDER BY a * a"},
		{"OrderBySqrt", "SELECT * FROM test_data ORDER BY a * a"},
		{"OrderByPower", "SELECT * FROM test_data ORDER BY POWER(a, 2)"},
		{"OrderByLog", "SELECT * FROM test_data ORDER BY a"},
		{"OrderByConcatenation", "SELECT * FROM test_data ORDER BY a || b || c"},
		{"OrderByCoalesce", "SELECT * FROM test_data ORDER BY COALESCE(a, 0) + COALESCE(b, 0) + COALESCE(c, 0)"},
		{"OrderByNullif", "SELECT * FROM test_data ORDER BY NULLIF(a, b) + NULLIF(b, c) + NULLIF(c, a)"},
		{"OrderByCastExpr", "SELECT * FROM test_data ORDER BY CAST(a AS REAL) + CAST(b AS REAL) + CAST(c AS REAL)"},
		{"OrderByInExpr", "SELECT * FROM test_data WHERE a IN (10, 20, 30) ORDER BY a"},
		{"OrderBySubquery", "SELECT * FROM test_data WHERE id IN (SELECT id FROM test_data WHERE a < 30 ORDER BY a DESC LIMIT 5) ORDER BY id"},
	}

	for _, tt := range expressionOrderByTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
