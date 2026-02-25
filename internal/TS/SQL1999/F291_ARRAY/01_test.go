package F291_ARRAY

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// TestSQL1999_F291_ScalarSubquery_L1 tests scalar subqueries in SELECT and WHERE.
func TestSQL1999_F291_ScalarSubquery_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	inserts := []string{
		"INSERT INTO employees VALUES (1, 'Alice', 1, 90000)",
		"INSERT INTO employees VALUES (2, 'Bob', 2, 75000)",
		"INSERT INTO employees VALUES (3, 'Carol', 1, 85000)",
		"INSERT INTO employees VALUES (4, 'Dave', 2, 60000)",
	}
	for _, sql := range inserts {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// Scalar subquery: employee with max salary
	rows, err := db.Query("SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)")
	if err != nil {
		t.Fatalf("scalar subquery: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "Alice" {
		t.Errorf("expected Alice, got %v", rows.Data)
	}

	// Scalar subquery in SELECT list
	rows2, err := db.Query("SELECT name, salary - (SELECT AVG(salary) FROM employees) FROM employees WHERE id = 1")
	if err != nil {
		t.Fatalf("scalar subquery in select: %v", err)
	}
	if len(rows2.Data) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows2.Data))
	}
}

// TestSQL1999_F291_InSubquery_L1 tests IN / NOT IN with subqueries.
func TestSQL1999_F291_InSubquery_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE orders: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE vip_customers (id INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE vip_customers: %v", err)
	}

	for _, sql := range []string{
		"INSERT INTO orders VALUES (1, 10, 500)",
		"INSERT INTO orders VALUES (2, 20, 300)",
		"INSERT INTO orders VALUES (3, 30, 700)",
		"INSERT INTO vip_customers VALUES (10)",
		"INSERT INTO vip_customers VALUES (30)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// IN subquery
	rows, err := db.Query("SELECT id FROM orders WHERE customer_id IN (SELECT id FROM vip_customers) ORDER BY id")
	if err != nil {
		t.Fatalf("IN subquery: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows.Data))
	}

	// NOT IN subquery
	rows2, err := db.Query("SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM vip_customers) ORDER BY id")
	if err != nil {
		t.Fatalf("NOT IN subquery: %v", err)
	}
	if len(rows2.Data) != 1 || rows2.Data[0][0] != int64(2) {
		t.Errorf("expected order id=2, got %v", rows2.Data)
	}
}

// TestSQL1999_F291_ExistsSubquery_L1 tests EXISTS / NOT EXISTS subqueries.
func TestSQL1999_F291_ExistsSubquery_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE products (id INTEGER, name TEXT)",
		"CREATE TABLE inventory (product_id INTEGER, qty INTEGER)",
		"INSERT INTO products VALUES (1, 'Widget')",
		"INSERT INTO products VALUES (2, 'Gadget')",
		"INSERT INTO products VALUES (3, 'Doohickey')",
		"INSERT INTO inventory VALUES (1, 100)",
		"INSERT INTO inventory VALUES (3, 0)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// EXISTS: products that have inventory records
	rows, err := db.Query("SELECT name FROM products WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.product_id = products.id) ORDER BY name")
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows, got %d: %v", len(rows.Data), rows.Data)
	}

	// NOT EXISTS: products with no inventory
	rows2, err := db.Query("SELECT name FROM products WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.product_id = products.id)")
	if err != nil {
		t.Fatalf("NOT EXISTS: %v", err)
	}
	if len(rows2.Data) != 1 || rows2.Data[0][0] != "Gadget" {
		t.Errorf("expected Gadget, got %v", rows2.Data)
	}
}

// TestSQL1999_F291_CorrelatedSubquery_L1 tests correlated subqueries.
func TestSQL1999_F291_CorrelatedSubquery_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE sales (id INTEGER, region TEXT, amount INTEGER)",
		"INSERT INTO sales VALUES (1, 'east', 200)",
		"INSERT INTO sales VALUES (2, 'east', 400)",
		"INSERT INTO sales VALUES (3, 'west', 300)",
		"INSERT INTO sales VALUES (4, 'west', 100)",
		"INSERT INTO sales VALUES (5, 'east', 600)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Correlated: rows whose amount exceeds the regional average
	rows, err := db.Query(`
		SELECT id, region, amount
		FROM sales s1
		WHERE amount > (SELECT AVG(amount) FROM sales s2 WHERE s2.region = s1.region)
		ORDER BY id`)
	if err != nil {
		t.Fatalf("correlated subquery: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows above regional avg, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_F291_NestedSubquery_L1 tests nested (multi-level) subqueries.
func TestSQL1999_F291_NestedSubquery_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE nums (n INTEGER)",
		"INSERT INTO nums VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Nested: values greater than the average of values less than 8
	rows, err := db.Query(`
		SELECT n FROM nums
		WHERE n > (SELECT AVG(n) FROM nums WHERE n < (SELECT MAX(n) FROM nums WHERE n < 8))
		ORDER BY n`)
	if err != nil {
		t.Fatalf("nested subquery: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Error("expected at least one row from nested subquery")
	}
	// MAX(n) where n<8 = 7; AVG(n) where n<7 = AVG(1..6) = 3.5; n>3.5 => 4,5,6,7,8,9,10 = 7 rows
	if len(rows.Data) != 7 {
		t.Errorf("expected 7 rows, got %d: %v", len(rows.Data), rows.Data)
	}
}
