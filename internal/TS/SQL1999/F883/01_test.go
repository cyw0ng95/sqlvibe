package F883

import (
	"database/sql"
	"testing"

	_ "github.com/cyw0ng95/sqlvibe/driver"
)

// TestSQL1999_F883_OpenMemory_L1 tests that sql.Open("sqlvibe", ":memory:") succeeds.
func TestSQL1999_F883_OpenMemory_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestSQL1999_F883_ExecDDL_L1 tests db.Exec with CREATE TABLE and INSERT.
func TestSQL1999_F883_ExecDDL_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	res, err := db.Exec("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row affected, got %d", n)
	}
}

// TestSQL1999_F883_QueryRowWithParams_L1 tests db.QueryRow with ? params.
func TestSQL1999_F883_QueryRowWithParams_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'hello'), (2, 'world')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var val string
	if err := db.QueryRow("SELECT val FROM t WHERE id = ?", int64(2)).Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

// TestSQL1999_F883_QueryWithScan_L1 tests db.Query + scan into Go types.
func TestSQL1999_F883_QueryWithScan_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE nums (n INTEGER, v REAL)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec("INSERT INTO nums VALUES (?, ?)", int64(i), float64(i)*1.5); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	rows, err := db.Query("SELECT n, v FROM nums ORDER BY n")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int64
		var v float64
		if err := rows.Scan(&n, &v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
		expected := float64(count) * 1.5
		if n != int64(count) || v != expected {
			t.Fatalf("row %d: n=%d v=%f, expected n=%d v=%f", count, n, v, count, expected)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

// TestSQL1999_F883_BeginCommit_L1 tests db.Begin / tx.Exec / tx.Commit.
func TestSQL1999_F883_BeginCommit_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (42)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var id int64
	if err := db.QueryRow("SELECT id FROM t").Scan(&id); err != nil {
		t.Fatalf("QueryRow after commit: %v", err)
	}
	if id != 42 {
		t.Fatalf("expected 42, got %d", id)
	}
}

// TestSQL1999_F883_BeginRollback_L1 tests db.Begin / tx.Rollback.
func TestSQL1999_F883_BeginRollback_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	// Use a single connection so the rollback and verification query share the
	// same in-memory database and the snapshot restore is visible.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (99)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	// After rollback the inserted row should not be visible.
	if count != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", count)
	}
}

// TestSQL1999_F883_PrepareStmtQuery_L1 tests db.Prepare / stmt.Query / stmt.Close.
func TestSQL1999_F883_PrepareStmtQuery_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price REAL)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	data := []struct {
		id    int64
		name  string
		price float64
	}{
		{1, "apple", 1.20},
		{2, "banana", 0.50},
		{3, "cherry", 2.00},
	}
	for _, d := range data {
		if _, err := db.Exec("INSERT INTO products VALUES (?, ?, ?)", d.id, d.name, d.price); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	stmt, err := db.Prepare("SELECT name, price FROM products WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	var name string
	var price float64
	if err := stmt.QueryRow(int64(2)).Scan(&name, &price); err != nil {
		t.Fatalf("stmt.QueryRow: %v", err)
	}
	if name != "banana" || price != 0.50 {
		t.Fatalf("expected banana/0.50, got %s/%f", name, price)
	}
}
