// Package F888 provides end-to-end tests for the v0.10.0 bytecode execution engine.
// Bytecode is now the always-on execution path.
package F888

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// TestF888_SelectLiteral tests SELECT of literal expressions via bytecode path.
func TestF888_SelectLiteral(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT 1+1")
	if err != nil {
		t.Fatalf("SELECT 1+1: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got != "2" {
		t.Errorf("SELECT 1+1 = %v, want 2", got)
	}
}

// TestF888_SelectStringConcat tests string concatenation.
func TestF888_SelectStringConcat(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT 'hello' || ' ' || 'world'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got != "hello world" {
		t.Errorf("concat = %v, want 'hello world'", got)
	}
}

// TestF888_SelectFromTable tests SELECT * FROM table via bytecode path.
func TestF888_SelectFromTable(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t(n INTEGER, s TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT n, s FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
}

// TestF888_SelectWhereFilter tests WHERE filtering via bytecode path.
func TestF888_SelectWhereFilter(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE nums(n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO nums VALUES (%d)", i)); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.Query("SELECT n FROM nums WHERE n > 3")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows (n>3), got %d", len(rows.Data))
	}
}

// TestF888_SelectNullLiteral tests NULL literal handling.
func TestF888_SelectNullLiteral(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT NULL")
	if err != nil {
		t.Fatalf("SELECT NULL: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != nil {
		t.Errorf("SELECT NULL = %v, want nil", rows.Data[0][0])
	}
}

// TestF888_SelectArithmetic tests arithmetic operations.
func TestF888_SelectArithmetic(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	cases := []struct {
		sql  string
		want interface{}
	}{
		{"SELECT 10 - 3", int64(7)},
		{"SELECT 6 * 7", int64(42)},
		{"SELECT 10 / 3", int64(3)},
		{"SELECT 10 % 3", int64(1)},
	}

	for _, tc := range cases {
		rows, err := db.Query(tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		if len(rows.Data) != 1 {
			t.Errorf("%s: expected 1 row", tc.sql)
			continue
		}
		got := fmt.Sprintf("%v", rows.Data[0][0])
		want := fmt.Sprintf("%v", tc.want)
		if got != want {
			t.Errorf("%s = %v, want %v", tc.sql, got, want)
		}
	}
}

// TestF888_BytecodeAlwaysOn verifies that the bytecode engine is always active
// (PRAGMA use_bytecode has been removed; bytecode is the only execution path).
func TestF888_BytecodeAlwaysOn(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Simple literal via bytecode path.
	rows, err := db.Query("SELECT 6 * 7")
	if err != nil {
		t.Fatalf("SELECT 6*7: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got != "42" {
		t.Errorf("SELECT 6*7 = %v, want 42", got)
	}
}

// TestF888_LegacyFallback verifies that unsupported constructs fall back to the legacy path.
func TestF888_LegacyFallback(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE a(x INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE b(x INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO a VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO b VALUES (1), (3)"); err != nil {
		t.Fatal(err)
	}

	// JOIN is not supported in bytecode path; should fall back to legacy.
	rows, err := db.Query("SELECT a.x FROM a JOIN b ON a.x = b.x")
	if err != nil {
		t.Fatalf("JOIN fallback: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("expected 1 matching row, got %d", len(rows.Data))
	}
}
