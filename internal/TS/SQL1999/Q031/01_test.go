package Q031

import (
	"database/sql"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func setup(t *testing.T) (*sqlvibe.Database, *sql.DB) {
	t.Helper()
	sv, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	sl, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return sv, sl
}

// TestSQL1999_Q031_SubqueryIN_L1 tests subquery with IN clause.
func TestSQL1999_Q031_SubqueryIN_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE product (id INTEGER, category_id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE product (id INTEGER, category_id INTEGER, name TEXT)")
	sv.Exec("CREATE TABLE category (id INTEGER, label TEXT)")
	sl.Exec("CREATE TABLE category (id INTEGER, label TEXT)")

	sv.Exec("INSERT INTO category VALUES (1, 'electronics')")
	sl.Exec("INSERT INTO category VALUES (1, 'electronics')")
	sv.Exec("INSERT INTO category VALUES (2, 'books')")
	sl.Exec("INSERT INTO category VALUES (2, 'books')")

	sv.Exec("INSERT INTO product VALUES (1, 1, 'phone')")
	sl.Exec("INSERT INTO product VALUES (1, 1, 'phone')")
	sv.Exec("INSERT INTO product VALUES (2, 2, 'novel')")
	sl.Exec("INSERT INTO product VALUES (2, 2, 'novel')")
	sv.Exec("INSERT INTO product VALUES (3, 1, 'tablet')")
	sl.Exec("INSERT INTO product VALUES (3, 1, 'tablet')")

	tests := []struct{ name, sql string }{
		{"SubqueryIN", "SELECT name FROM product WHERE category_id IN (SELECT id FROM category WHERE label = 'electronics') ORDER BY name"},
		{"SubqueryNotIN", "SELECT name FROM product WHERE category_id NOT IN (SELECT id FROM category WHERE label = 'electronics') ORDER BY name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q031_DerivedTable_L1 tests subquery in FROM (derived table).
func TestSQL1999_Q031_DerivedTable_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE sales (id INTEGER, amount INTEGER)")
	sl.Exec("CREATE TABLE sales (id INTEGER, amount INTEGER)")
	sv.Exec("INSERT INTO sales VALUES (1, 100)")
	sl.Exec("INSERT INTO sales VALUES (1, 100)")
	sv.Exec("INSERT INTO sales VALUES (2, 200)")
	sl.Exec("INSERT INTO sales VALUES (2, 200)")
	sv.Exec("INSERT INTO sales VALUES (3, 150)")
	sl.Exec("INSERT INTO sales VALUES (3, 150)")

	tests := []struct{ name, sql string }{
		{"DerivedTable", "SELECT sub.total FROM (SELECT SUM(amount) AS total FROM sales) AS sub"},
		{"DerivedTableFilter", "SELECT sub.id, sub.amount FROM (SELECT id, amount FROM sales WHERE amount > 100) AS sub ORDER BY sub.id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q031_ScalarSubquery_L1 tests scalar subquery in SELECT.
func TestSQL1999_Q031_ScalarSubquery_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE items (id INTEGER, val INTEGER)")
	sl.Exec("CREATE TABLE items (id INTEGER, val INTEGER)")
	sv.Exec("INSERT INTO items VALUES (1, 10)")
	sl.Exec("INSERT INTO items VALUES (1, 10)")
	sv.Exec("INSERT INTO items VALUES (2, 20)")
	sl.Exec("INSERT INTO items VALUES (2, 20)")
	sv.Exec("INSERT INTO items VALUES (3, 30)")
	sl.Exec("INSERT INTO items VALUES (3, 30)")

	tests := []struct{ name, sql string }{
		{"ScalarSubqueryInSelect", "SELECT id, val, (SELECT MAX(val) FROM items) AS max_val FROM items ORDER BY id"},
		{"ScalarSubqueryInWhere", "SELECT id FROM items WHERE val = (SELECT MIN(val) FROM items)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q031_ExistsSubquery_L1 tests EXISTS subquery.
func TestSQL1999_Q031_ExistsSubquery_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE orders (id INTEGER, cust_id INTEGER)")
	sl.Exec("CREATE TABLE orders (id INTEGER, cust_id INTEGER)")
	sv.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")

	sv.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	sl.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	sv.Exec("INSERT INTO customers VALUES (2, 'Bob')")
	sl.Exec("INSERT INTO customers VALUES (2, 'Bob')")

	sv.Exec("INSERT INTO orders VALUES (1, 1)")
	sl.Exec("INSERT INTO orders VALUES (1, 1)")

	tests := []struct{ name, sql string }{
		{"ExistsSubquery", "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.cust_id = customers.id) ORDER BY name"},
		{"NotExistsSubquery", "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.cust_id = customers.id) ORDER BY name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
