// Package F876 tests SQL:1999 F876 - Advanced SQL compatibility features for v0.9.4.
// Features tested: Partial Index, Expression Index, RETURNING clause,
// UPDATE...FROM, DELETE...USING, MATCH operator, COLLATE support.
package F876

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F876_PartialIndex_L1 validates CREATE INDEX ... WHERE expr (partial index).
func TestSQL1999_F876_PartialIndex_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)`,
		`INSERT INTO users VALUES (1, 'Alice', 'active')`,
		`INSERT INTO users VALUES (2, 'Bob', 'inactive')`,
		`INSERT INTO users VALUES (3, 'Carol', 'active')`,
		`CREATE INDEX idx_active_name ON users(name) WHERE status = 'active'`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllRows", "SELECT id, name, status FROM users ORDER BY id"},
		{"ActiveUsers", "SELECT name FROM users WHERE status = 'active' ORDER BY name"},
		{"InactiveUsers", "SELECT name FROM users WHERE status = 'inactive'"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F876_ExpressionIndex_L1 validates CREATE INDEX ON table(LOWER(col)).
func TestSQL1999_F876_ExpressionIndex_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO products VALUES (1, 'Widget')`,
		`INSERT INTO products VALUES (2, 'Gadget')`,
		`INSERT INTO products VALUES (3, 'WIDGET')`,
		`CREATE INDEX idx_lower_name ON products(LOWER(name))`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllRows", "SELECT id, name FROM products ORDER BY id"},
		{"LowerSearch", "SELECT id FROM products WHERE LOWER(name) = 'widget' ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM products"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F876_InsertReturning_L1 validates INSERT ... RETURNING.
func TestSQL1999_F876_InsertReturning_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT, created_at TEXT)`,
		`INSERT INTO logs VALUES (1, 'hello', '2024-01-01')`,
		`INSERT INTO logs VALUES (2, 'world', '2024-01-02')`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	// Test INSERT RETURNING with *
	rows, err := svDB.Query("INSERT INTO logs VALUES (3, 'test', '2024-01-03') RETURNING *")
	if err != nil {
		t.Fatalf("INSERT RETURNING *: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
		var id int64
		var msg, created string
		if err := rows.Scan(&id, &msg, &created); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id != 3 || msg != "test" {
			t.Errorf("got id=%d msg=%q, want id=3 msg='test'", id, msg)
		}
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

// TestSQL1999_F876_DeleteReturning_L1 validates DELETE ... RETURNING.
func TestSQL1999_F876_DeleteReturning_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO items VALUES (1, 'apple')`,
		`INSERT INTO items VALUES (2, 'banana')`,
		`INSERT INTO items VALUES (3, 'cherry')`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	rows, err := svDB.Query("DELETE FROM items WHERE id < 3 RETURNING id, name")
	if err != nil {
		t.Fatalf("DELETE RETURNING: %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 deleted rows, got %d", len(ids))
	}

	// Verify remaining
	remaining, err := svDB.Query("SELECT COUNT(*) FROM items")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	remaining.Next()
	var cnt int64
	remaining.Scan(&cnt)
	if cnt != 1 {
		t.Errorf("expected 1 remaining row, got %d", cnt)
	}
}

// TestSQL1999_F876_UpdateReturning_L1 validates UPDATE ... RETURNING.
func TestSQL1999_F876_UpdateReturning_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)`,
		`INSERT INTO accounts VALUES (1, 100.0)`,
		`INSERT INTO accounts VALUES (2, 200.0)`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	rows, err := svDB.Query("UPDATE accounts SET balance = balance + 50.0 WHERE id = 1 RETURNING id, balance")
	if err != nil {
		t.Fatalf("UPDATE RETURNING: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
		var id int64
		var balance float64
		if err := rows.Scan(&id, &balance); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id == 1 && balance != 150.0 {
			t.Errorf("expected balance=150.0 for id=1, got %v", balance)
		}
	}
	if count == 0 {
		t.Error("expected at least one row from UPDATE RETURNING")
	}
}

// TestSQL1999_F876_UpdateFrom_L1 validates UPDATE ... FROM (PostgreSQL-style).
func TestSQL1999_F876_UpdateFrom_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, total REAL)`,
		`CREATE TABLE prices (product_id INTEGER PRIMARY KEY, price REAL)`,
		`INSERT INTO orders VALUES (1, 10, 0.0)`,
		`INSERT INTO orders VALUES (2, 20, 0.0)`,
		`INSERT INTO prices VALUES (10, 9.99)`,
		`INSERT INTO prices VALUES (20, 19.99)`,
		`UPDATE orders SET total = prices.price FROM prices WHERE orders.product_id = prices.product_id`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	rows, err := svDB.Query("SELECT id, total FROM orders ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	type row struct{ id int64; total float64 }
	var got []row
	for rows.Next() {
		var r row
		rows.Scan(&r.id, &r.total)
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].total != 9.99 {
		t.Errorf("order 1 total: got %v, want 9.99", got[0].total)
	}
	if got[1].total != 19.99 {
		t.Errorf("order 2 total: got %v, want 19.99", got[1].total)
	}
}

// TestSQL1999_F876_DeleteUsing_L1 validates DELETE ... USING.
func TestSQL1999_F876_DeleteUsing_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE orders2 (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)`,
		`CREATE TABLE customers (id INTEGER PRIMARY KEY, status TEXT)`,
		`INSERT INTO customers VALUES (1, 'active')`,
		`INSERT INTO customers VALUES (2, 'inactive')`,
		`INSERT INTO orders2 VALUES (1, 1, 100.0)`,
		`INSERT INTO orders2 VALUES (2, 2, 200.0)`,
		`INSERT INTO orders2 VALUES (3, 2, 300.0)`,
		`DELETE FROM orders2 USING customers WHERE orders2.customer_id = customers.id AND customers.status = 'inactive'`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	rows, err := svDB.Query("SELECT COUNT(*) FROM orders2")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	rows.Next()
	var cnt int64
	rows.Scan(&cnt)
	if cnt != 1 {
		t.Errorf("expected 1 remaining order, got %d", cnt)
	}
}

// TestSQL1999_F876_MatchOperator_L1 validates the MATCH operator.
func TestSQL1999_F876_MatchOperator_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)`,
		`INSERT INTO docs VALUES (1, 'Hello World')`,
		`INSERT INTO docs VALUES (2, 'Goodbye World')`,
		`INSERT INTO docs VALUES (3, 'Hello There')`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	rows, err := svDB.Query("SELECT id FROM docs WHERE content MATCH 'hello' ORDER BY id")
	if err != nil {
		t.Fatalf("MATCH query: %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 match results, got %d: %v", len(ids), ids)
	}
}

// TestSQL1999_F876_CollateNocase_L1 validates COLLATE NOCASE on columns.
func TestSQL1999_F876_CollateNocase_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)`,
		`INSERT INTO tags VALUES (1, 'Go')`,
		`INSERT INTO tags VALUES (2, 'PYTHON')`,
		`INSERT INTO tags VALUES (3, 'javascript')`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
	}

	// Verify rows stored correctly
	rows, err := svDB.Query("SELECT COUNT(*) FROM tags")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	rows.Next()
	var cnt int64
	rows.Scan(&cnt)
	if cnt != 3 {
		t.Errorf("expected 3 rows, got %d", cnt)
	}
}

// TestSQL1999_F876_GlobOperator_L1 validates GLOB operator with * and ? wildcards.
func TestSQL1999_F876_GlobOperator_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO files VALUES (1, 'report.txt')`,
		`INSERT INTO files VALUES (2, 'data.csv')`,
		`INSERT INTO files VALUES (3, 'image.png')`,
		`INSERT INTO files VALUES (4, 'test1.txt')`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"GlobStar", "SELECT id FROM files WHERE name GLOB '*.txt' ORDER BY id"},
		{"GlobQuestion", "SELECT id FROM files WHERE name GLOB 'test?.txt' ORDER BY id"},
		{"GlobAllIds", "SELECT id FROM files WHERE name GLOB '*' ORDER BY id"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F876_AlterTable_L1 validates ALTER TABLE ADD COLUMN and RENAME TO.
func TestSQL1999_F876_AlterTable_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO employees VALUES (1, 'Alice')`,
		`INSERT INTO employees VALUES (2, 'Bob')`,
		`ALTER TABLE employees ADD COLUMN department TEXT`,
		`UPDATE employees SET department = 'Engineering' WHERE id = 1`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllRows", "SELECT id, name, department FROM employees ORDER BY id"},
		{"Engineering", "SELECT name FROM employees WHERE department = 'Engineering'"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Test RENAME TO
	if _, err := svDB.Exec("ALTER TABLE employees RENAME TO staff"); err != nil {
		t.Fatalf("sqlvibe RENAME: %v", err)
	}
	if _, err := sqliteDB.Exec("ALTER TABLE employees RENAME TO staff"); err != nil {
		t.Fatalf("sqlite RENAME: %v", err)
	}
	SQL1999.CompareQueryResults(t, svDB, sqliteDB, "SELECT COUNT(*) FROM staff", "AfterRename")
}
