// Package F877 tests SQL:1999 F877 - SQL compatibility & maintenance for v0.9.5.
// Features tested: REINDEX (all / by table / by index), SELECT INTO.
package F877

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F877_ReindexAll_L1 validates REINDEX (all indexes) executes without error
// and subsequent queries still return correct results.
func TestSQL1999_F877_ReindexAll_L1(t *testing.T) {
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
		`CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT)`,
		`CREATE INDEX idx_dept ON employees(dept)`,
		`INSERT INTO employees VALUES (1, 'Alice', 'Engineering')`,
		`INSERT INTO employees VALUES (2, 'Bob', 'Marketing')`,
		`INSERT INTO employees VALUES (3, 'Carol', 'Engineering')`,
		`REINDEX`,
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
		{"AllRows", "SELECT id, name, dept FROM employees ORDER BY id"},
		{"EngDept", "SELECT name FROM employees WHERE dept = 'Engineering' ORDER BY name"},
		{"Count", "SELECT COUNT(*) FROM employees"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F877_ReindexByTable_L1 validates REINDEX <tablename> rebuilds all
// indexes on that table.
func TestSQL1999_F877_ReindexByTable_L1(t *testing.T) {
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
		`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`,
		`CREATE INDEX idx_price ON products(price)`,
		`INSERT INTO products VALUES (1, 'Widget', 9.99)`,
		`INSERT INTO products VALUES (2, 'Gadget', 19.99)`,
		`INSERT INTO products VALUES (3, 'Doohickey', 4.99)`,
		`REINDEX products`,
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
		{"AllRows", "SELECT id, name, price FROM products ORDER BY id"},
		{"CheapProducts", "SELECT name FROM products WHERE price < 10.0 ORDER BY name"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F877_ReindexByIndex_L1 validates REINDEX <indexname> rebuilds a specific index.
func TestSQL1999_F877_ReindexByIndex_L1(t *testing.T) {
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
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount REAL)`,
		`CREATE INDEX idx_status ON orders(status)`,
		`CREATE INDEX idx_amount ON orders(amount)`,
		`INSERT INTO orders VALUES (1, 'pending', 100.0)`,
		`INSERT INTO orders VALUES (2, 'shipped', 250.0)`,
		`INSERT INTO orders VALUES (3, 'pending', 75.0)`,
		`REINDEX idx_status`,
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
		{"AllOrders", "SELECT id, status, amount FROM orders ORDER BY id"},
		{"PendingOrders", "SELECT id FROM orders WHERE status = 'pending' ORDER BY id"},
		{"LargeOrders", "SELECT id FROM orders WHERE amount > 100.0"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F877_SelectInto_L1 validates SELECT ... INTO newtable FROM src.
// SELECT INTO is not supported by SQLite so this is a sqlvibe-only test.
func TestSQL1999_F877_SelectInto_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`,
		`INSERT INTO source VALUES (1, 'Alice', 95)`,
		`INSERT INTO source VALUES (2, 'Bob', 80)`,
		`INSERT INTO source VALUES (3, 'Carol', 90)`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	// SELECT INTO creates a new table
	if _, err := svDB.Exec(`SELECT id, name, score INTO highscorers FROM source WHERE score >= 90`); err != nil {
		t.Fatalf("SELECT INTO failed: %v", err)
	}

	// Verify the new table exists and has correct data
	rows, err := svDB.Query(`SELECT id, name, score FROM highscorers ORDER BY id`)
	if err != nil {
		t.Fatalf("query highscorers: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows in highscorers, got %d", len(rows.Data))
	}
}

// TestSQL1999_F877_SelectIntoAll_L1 validates SELECT * INTO newtable FROM src copies all columns.
func TestSQL1999_F877_SelectIntoAll_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	setup := []string{
		`CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, qty INTEGER)`,
		`INSERT INTO inventory VALUES (1, 'Apple', 50)`,
		`INSERT INTO inventory VALUES (2, 'Banana', 30)`,
		`INSERT INTO inventory VALUES (3, 'Cherry', 100)`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	// Copy entire table via SELECT *
	if _, err := svDB.Exec(`SELECT * INTO inventory_backup FROM inventory`); err != nil {
		t.Fatalf("SELECT INTO failed: %v", err)
	}

	// Both tables must have the same data
	orig, err := svDB.Query(`SELECT id, item, qty FROM inventory ORDER BY id`)
	if err != nil {
		t.Fatalf("query original: %v", err)
	}
	bkp, err := svDB.Query(`SELECT id, item, qty FROM inventory_backup ORDER BY id`)
	if err != nil {
		t.Fatalf("query backup: %v", err)
	}
	if len(orig.Data) != len(bkp.Data) {
		t.Errorf("row count mismatch: original=%d, backup=%d", len(orig.Data), len(bkp.Data))
	}
	for i := range orig.Data {
		for j := range orig.Data[i] {
			ov := orig.Data[i][j]
			bv := bkp.Data[i][j]
			if ov != bv {
				t.Errorf("row %d col %d mismatch: original=%v backup=%v", i, j, ov, bv)
			}
		}
	}
}
