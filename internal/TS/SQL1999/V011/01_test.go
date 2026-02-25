package V011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_V011_CreateViewAndSelect_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)")
	sqlvibeDB.Exec("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000)")
	sqlvibeDB.Exec("INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 70000)")
	sqlvibeDB.Exec("INSERT INTO employees VALUES (3, 'Carol', 'Engineering', 95000)")
	sqlvibeDB.Exec("INSERT INTO employees VALUES (4, 'Dave', 'HR', 60000)")
	sqlvibeDB.Exec("CREATE VIEW emp_view AS SELECT id, name, dept FROM employees")
	sqliteDB.Exec("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)")
	sqliteDB.Exec("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000)")
	sqliteDB.Exec("INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 70000)")
	sqliteDB.Exec("INSERT INTO employees VALUES (3, 'Carol', 'Engineering', 95000)")
	sqliteDB.Exec("INSERT INTO employees VALUES (4, 'Dave', 'HR', 60000)")
	sqliteDB.Exec("CREATE VIEW emp_view AS SELECT id, name, dept FROM employees")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectAllFromView", "SELECT * FROM emp_view ORDER BY id"},
		{"SelectNameFromView", "SELECT name FROM emp_view ORDER BY name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V011_ViewWithWhere_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, active INTEGER)")
	sqlvibeDB.Exec("INSERT INTO products VALUES (1, 'Apple', 1.50, 1)")
	sqlvibeDB.Exec("INSERT INTO products VALUES (2, 'Banana', 0.75, 1)")
	sqlvibeDB.Exec("INSERT INTO products VALUES (3, 'Cherry', 3.00, 0)")
	sqlvibeDB.Exec("INSERT INTO products VALUES (4, 'Date', 5.00, 1)")
	sqlvibeDB.Exec("CREATE VIEW active_products AS SELECT id, name, price FROM products WHERE active = 1")
	sqliteDB.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, active INTEGER)")
	sqliteDB.Exec("INSERT INTO products VALUES (1, 'Apple', 1.50, 1)")
	sqliteDB.Exec("INSERT INTO products VALUES (2, 'Banana', 0.75, 1)")
	sqliteDB.Exec("INSERT INTO products VALUES (3, 'Cherry', 3.00, 0)")
	sqliteDB.Exec("INSERT INTO products VALUES (4, 'Date', 5.00, 1)")
	sqliteDB.Exec("CREATE VIEW active_products AS SELECT id, name, price FROM products WHERE active = 1")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectActiveProducts", "SELECT * FROM active_products ORDER BY id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V011_ViewWithJoin_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
	sqlvibeDB.Exec("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, customer_name TEXT)")
	sqlvibeDB.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	sqlvibeDB.Exec("INSERT INTO customers VALUES (2, 'Bob')")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (101, 1, 250.00)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (102, 2, 150.00)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (103, 1, 75.00)")
	sqlvibeDB.Exec("CREATE VIEW order_details AS SELECT o.order_id, c.customer_name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.customer_id")
	sqliteDB.Exec("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
	sqliteDB.Exec("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, customer_name TEXT)")
	sqliteDB.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	sqliteDB.Exec("INSERT INTO customers VALUES (2, 'Bob')")
	sqliteDB.Exec("INSERT INTO orders VALUES (101, 1, 250.00)")
	sqliteDB.Exec("INSERT INTO orders VALUES (102, 2, 150.00)")
	sqliteDB.Exec("INSERT INTO orders VALUES (103, 1, 75.00)")
	sqliteDB.Exec("CREATE VIEW order_details AS SELECT o.order_id, c.customer_name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.customer_id")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectOrderDetails", "SELECT * FROM order_details ORDER BY order_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V011_ViewWithExpressions_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, unit_price REAL)")
	sqlvibeDB.Exec("INSERT INTO items VALUES (1, 'Widget', 10, 2.50)")
	sqlvibeDB.Exec("INSERT INTO items VALUES (2, 'Gadget', 5, 15.00)")
	sqlvibeDB.Exec("INSERT INTO items VALUES (3, 'Doohickey', 20, 0.99)")
	sqlvibeDB.Exec("CREATE VIEW item_totals AS SELECT id, name, qty * unit_price AS total_price FROM items")
	sqliteDB.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, unit_price REAL)")
	sqliteDB.Exec("INSERT INTO items VALUES (1, 'Widget', 10, 2.50)")
	sqliteDB.Exec("INSERT INTO items VALUES (2, 'Gadget', 5, 15.00)")
	sqliteDB.Exec("INSERT INTO items VALUES (3, 'Doohickey', 20, 0.99)")
	sqliteDB.Exec("CREATE VIEW item_totals AS SELECT id, name, qty * unit_price AS total_price FROM items")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectComputedTotals", "SELECT * FROM item_totals ORDER BY id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
