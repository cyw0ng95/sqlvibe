package E101

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E10113_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE orders (id INTEGER, customer TEXT, product TEXT, quantity INTEGER, price REAL)"},
		{"InsertData", "INSERT INTO orders VALUES (1, 'Alice', 'Apple', 5, 1.99), (2, 'Bob', 'Banana', 10, 0.99), (3, 'Charlie', 'Orange', 7, 1.49), (4, 'Diana', 'Apple', 3, 1.99), (5, 'Eve', 'Banana', 2, 0.99), (6, 'Frank', 'Orange', 4, 1.49)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertAllColumns", "INSERT INTO orders VALUES (7, 'Grace', 'Milk', 6, 3.99)"},
		{"InsertSpecificColumns", "INSERT INTO orders (id, customer, product) VALUES (8, 'Henry', 'Bread')"},
		{"InsertMultipleRows", "INSERT INTO orders VALUES (9, 'Ivy', 'Apple', 2, 1.99), (10, 'Jack', 'Orange', 3, 1.49), (11, 'Kate', 'Milk', 5, 3.99)"},
		{"InsertWithNull", "INSERT INTO orders (id, customer, product, quantity) VALUES (12, 'Leo', NULL, NULL)"},
		{"InsertExpression", "INSERT INTO orders (id, customer, quantity, price) VALUES (13, 'Mary', 10, 2.99)"},
		{"InsertNegative", "INSERT INTO orders (id, customer, quantity, price) VALUES (14, 'Nancy', -5, -1.99)"},
		{"InsertZero", "INSERT INTO orders (id, customer, quantity, price) VALUES (15, 'Oscar', 0, 0.00)"},
		{"InsertLargeNumber", "INSERT INTO orders (id, customer, quantity) VALUES (16, 'Paul', 999999)"},
		{"InsertFloat", "INSERT INTO orders (id, price) VALUES (17, 'Quinn', 1.50)"},
		{"InsertWithDefault", "INSERT INTO orders (id, customer) VALUES (18, 'Rachel')"},
		{"InsertSubquery", "INSERT INTO orders (id, customer, product, quantity) SELECT id, customer, product, quantity * 2 FROM orders WHERE id = 1"},
		{"InsertDerivedTable", "INSERT INTO orders (id, customer) SELECT MAX(id) + 1, 'NewCustomer' FROM orders"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectAfterInsert := []struct {
		name string
		sql  string
	}{
		{"SelectAfterInserts", "SELECT * FROM orders ORDER BY id"},
		{"SelectCustomerCount", "SELECT customer, COUNT(*) FROM orders GROUP BY customer"},
		{"SelectProductTotal", "SELECT product, SUM(quantity) FROM orders GROUP BY product"},
		{"SelectMaxQuantity", "SELECT MAX(quantity) FROM orders"},
		{"SelectMinPrice", "SELECT MIN(price) FROM orders"},
		{"SelectTotalValue", "SELECT SUM(quantity * price) FROM orders"},
		{"SelectDistinctCustomers", "SELECT DISTINCT customer FROM orders"},
		{"SelectWithWhere", "SELECT * FROM orders WHERE quantity > 5"},
		{"SelectWithOrderBy", "SELECT * FROM orders ORDER BY quantity DESC, customer ASC"},
	}

	for _, tt := range selectAfterInsert {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E10114_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE employees (id INTEGER, name TEXT, salary REAL, department TEXT, bonus REAL)"},
		{"InsertData", "INSERT INTO employees VALUES (1, 'Alice', 75000, 'Engineering', 5000), (2, 'Bob', 80000, 'Engineering', 8000), (3, 'Charlie', 65000, 'Sales', 3000), (4, 'Diana', 70000, 'Sales', 7000), (5, 'Eve', 60000, 'HR', 2000), (6, 'Frank', 82000, 'Engineering', 10000), (7, 'Grace', 62000, 'HR', 2500)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	updateTests := []struct {
		name string
		sql  string
	}{
		{"UpdateSingleColumn", "UPDATE employees SET salary = 76000 WHERE id = 1"},
		{"UpdateMultipleColumns", "UPDATE employees SET salary = 81000, bonus = 8500 WHERE id = 2"},
		{"UpdateAllRows", "UPDATE employees SET bonus = bonus + 500"},
		{"UpdateWithExpression", "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'"},
		{"UpdateWithFunction", "UPDATE employees SET salary = ROUND(salary, -2) WHERE id = 3"},
		{"UpdateWithSubquery", "UPDATE employees SET salary = (SELECT AVG(salary) FROM employees WHERE department = 'Engineering') * 0.9 WHERE department = 'Sales'"},
		{"UpdateWithCase", "UPDATE employees SET bonus = CASE WHEN salary > 70000 THEN salary * 0.1 ELSE salary * 0.05 END WHERE department = 'HR'"},
		{"UpdateMultipleWhere", "UPDATE employees SET salary = salary + 1000 WHERE department = 'Sales' AND salary < 68000"},
		{"UpdateWithAnd", "UPDATE employees SET bonus = 6000 WHERE salary > 60000 AND salary < 80000"},
		{"UpdateWithOr", "UPDATE employees SET bonus = 4000 WHERE salary < 65000 OR salary > 80000"},
		{"UpdateWithNot", "UPDATE employees SET bonus = 3000 WHERE NOT (department = 'Engineering')"},
		{"UpdateWithIn", "UPDATE employees SET salary = 85000 WHERE id IN (1, 2, 6)"},
		{"UpdateWithSubqueryIn", "UPDATE employees SET salary = (SELECT MAX(salary) FROM employees WHERE department = 'Engineering') WHERE id = 3"},
		{"UpdateWithExists", "UPDATE employees SET bonus = 5000 WHERE EXISTS (SELECT 1 FROM employees WHERE department = 'Engineering' AND salary > 80000)"},
		{"UpdateWithNotExists", "UPDATE employees SET bonus = 2000 WHERE NOT EXISTS (SELECT 1 FROM employees WHERE department = 'Engineering' AND id < employees.id)"},
		{"UpdateWithNull", "UPDATE employees SET bonus = NULL WHERE id = 7"},
		{"UpdateNullColumn", "UPDATE employees SET department = NULL WHERE id = 4"},
		{"UpdateZero", "UPDATE employees SET salary = 0 WHERE id = 999"},
		{"UpdateLimit", "UPDATE employees SET bonus = 3500 WHERE id IN (SELECT id FROM employees ORDER BY id LIMIT 3)"},
		{"UpdateOrderBy", "UPDATE employees SET salary = salary + 100 WHERE id IN (SELECT id FROM employees ORDER BY salary DESC LIMIT 2)"},
		{"UpdateComplexExpression", "UPDATE employees SET bonus = salary * 0.08 + CASE WHEN salary > 75000 THEN 1000 ELSE 0 END WHERE department = 'Engineering'"},
		{"UpdateWithMath", "UPDATE employees SET salary = salary + bonus WHERE id IN (1, 2, 6)"},
		{"UpdateStringConcat", "UPDATE employees SET name = name || ' (Updated)' WHERE id = 1"},
		{"UpdateBetween", "UPDATE employees SET salary = salary * 1.05 WHERE salary BETWEEN 65000 AND 75000"},
		{"UpdateWithLike", "UPDATE employees SET bonus = 5000 WHERE name LIKE 'A%'"},
		{"UpdateWithAggregate", "UPDATE employees SET salary = (SELECT AVG(salary) FROM employees WHERE department = 'Engineering') WHERE department = 'Sales' AND salary < (SELECT AVG(salary) FROM employees WHERE department = 'Engineering')"},
		{"UpdateNoRowsAffected", "UPDATE employees SET salary = 90000 WHERE id = 999"},
	}

	for _, tt := range updateTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectAfterUpdate := []struct {
		name string
		sql  string
	}{
		{"SelectAfterUpdate", "SELECT * FROM employees WHERE id IN (1, 2) ORDER BY id"},
		{"SelectAvgByDept", "SELECT department, AVG(salary) FROM employees GROUP BY department"},
		{"SelectWithUpdatedValues", "SELECT id, name, salary, bonus FROM employees WHERE id = 1"},
		{"SelectSumBonus", "SELECT SUM(bonus) FROM employees WHERE department = 'Engineering'"},
		{"SelectMaxSalary", "SELECT MAX(salary) FROM employees"},
		{"SelectUpdatedCount", "SELECT COUNT(*) FROM employees WHERE salary > 80000"},
		{"SelectWithWhere", "SELECT * FROM employees WHERE salary = 81000 ORDER BY id"},
		{"SelectGroupByHaving", "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 65000"},
		{"SelectNullBonus", "SELECT * FROM employees WHERE bonus IS NULL"},
	}

	for _, tt := range selectAfterUpdate {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E10115_L1(t *testing.T) {
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
		{"InsertData", "INSERT INTO products VALUES (1, 'Apple', 1.99, 100, 'Fruit'), (2, 'Banana', 0.99, 150, 'Fruit'), (3, 'Orange', 1.49, 200, 'Fruit'), (4, 'Carrot', 0.79, 80, 'Vegetable'), (5, 'Milk', 3.99, 50, 'Dairy'), (6, 'Cheese', 5.99, 30, 'Dairy'), (7, 'Bread', 2.49, 40, 'Bakery'), (8, 'Yogurt', 1.99, 70, 'Dairy')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	deleteTests := []struct {
		name string
		sql  string
	}{
		{"DeleteById", "DELETE FROM products WHERE id = 8"},
		{"DeleteByWhere", "DELETE FROM products WHERE quantity < 40"},
		{"DeleteMultiple", "DELETE FROM products WHERE id IN (7, 8)"},
		{"DeleteAll", "DELETE FROM products WHERE category = 'Bakery'"},
		{"DeleteWithAnd", "DELETE FROM products WHERE price > 2.00 AND quantity < 50"},
		{"DeleteWithOr", "DELETE FROM products WHERE quantity < 50 OR quantity > 100"},
		{"DeleteWithNot", "DELETE FROM products WHERE NOT (category = 'Fruit')"},
		{"DeleteWithIn", "DELETE FROM products WHERE category IN ('Dairy', 'Bakery')"},
		{"DeleteWithSubquery", "DELETE FROM products WHERE id IN (SELECT id FROM products WHERE quantity < 50 LIMIT 2)"},
		{"DeleteWithExists", "DELETE FROM products WHERE EXISTS (SELECT 1 FROM products p WHERE p.quantity > 100 AND products.id = p.id - 1)"},
		{"DeleteWithNotExists", "DELETE FROM products WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.quantity > 100 AND products.category = 'Fruit' AND p.id < products.id)"},
		{"DeleteWithSubqueryMultiple", "DELETE FROM products WHERE id IN (SELECT id FROM (SELECT id FROM products WHERE category = 'Fruit' ORDER BY id LIMIT 2) AS fruits)"},
		{"DeleteNoRows", "DELETE FROM products WHERE id = 999"},
		{"DeleteLimit", "DELETE FROM products WHERE id IN (SELECT id FROM products ORDER BY id LIMIT 2)"},
		{"DeleteOrderBy", "DELETE FROM products WHERE id IN (SELECT id FROM products ORDER BY quantity ASC LIMIT 3)"},
		{"DeleteBetween", "DELETE FROM products WHERE price BETWEEN 1.00 AND 2.00"},
		{"DeleteWithLike", "DELETE FROM products WHERE name LIKE 'A%'"},
		{"DeleteWithExpression", "DELETE FROM products WHERE quantity * price < 100.00"},
		{"DeleteWithComplexWhere", "DELETE FROM products WHERE (category = 'Dairy' AND quantity < 50) OR (category = 'Bakery' AND quantity < 40)"},
		{"DeleteWithAggregate", "DELETE FROM products WHERE id IN (SELECT id FROM products WHERE quantity * price < 150.00)"},
		{"DeleteAllRows", "DELETE FROM products WHERE category = 'Vegetable'"},
	}

	for _, tt := range deleteTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectAfterDelete := []struct {
		name string
		sql  string
	}{
		{"SelectAfterDelete", "SELECT * FROM products ORDER BY id"},
		{"SelectCountByCategory", "SELECT category, COUNT(*) FROM products GROUP BY category"},
		{"SelectSumByCategory", "SELECT category, SUM(quantity) FROM products GROUP BY category"},
		{"SelectDeletedCheck", "SELECT * FROM products WHERE id = 8"},
		{"SelectRemainingFruit", "SELECT * FROM products WHERE category = 'Fruit'"},
		{"SelectTotalValue", "SELECT SUM(quantity * price) FROM products"},
		{"SelectMinQuantity", "SELECT MIN(quantity) FROM products"},
		{"SelectMaxPrice", "SELECT MAX(price) FROM products"},
		{"SelectDistinctCategories", "SELECT DISTINCT category FROM products"},
		{"SelectWithWhere", "SELECT * FROM products WHERE quantity > 50"},
		{"SelectCountTotal", "SELECT COUNT(*) FROM products"},
	}

	for _, tt := range selectAfterDelete {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
