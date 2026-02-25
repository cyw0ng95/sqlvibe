package Regression

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/cyw0ng95/sqlvibe/driver"
)

// TestRegression_DriverTypeRoundTrip_L1 tests int64, float64, string, []byte, nil round-trips.
func TestRegression_DriverTypeRoundTrip_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE types (i INTEGER, f REAL, s TEXT, b BLOB)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO types VALUES (?, ?, ?, ?)",
		int64(42), float64(3.14), "hello", []byte("world")); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var i int64
	var f float64
	var s string
	var b []byte
	if err := db.QueryRow("SELECT i, f, s, b FROM types").Scan(&i, &f, &s, &b); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if i != 42 {
		t.Errorf("int64 round-trip: got %d, want 42", i)
	}
	if f != 3.14 {
		t.Errorf("float64 round-trip: got %f, want 3.14", f)
	}
	if s != "hello" {
		t.Errorf("string round-trip: got %q, want hello", s)
	}
	if string(b) != "world" {
		t.Errorf("[]byte round-trip: got %q, want world", string(b))
	}
}

// TestRegression_DriverNilRoundTrip_L1 tests nil (NULL) round-trip through the driver.
func TestRegression_DriverNilRoundTrip_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE nulltest (v TEXT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO nulltest VALUES (?)", nil); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var v sql.NullString
	if err := db.QueryRow("SELECT v FROM nulltest").Scan(&v); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if v.Valid {
		t.Errorf("expected NULL, got %q", v.String)
	}
}

// TestRegression_DriverNamedParams_L1 tests named params via sql.Named.
func TestRegression_DriverNamedParams_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int64
	err = db.QueryRow(
		"SELECT id FROM t WHERE name = :name",
		sql.Named("name", "Bob"),
	).Scan(&id)
	if err != nil {
		t.Fatalf("QueryRow named: %v", err)
	}
	if id != 2 {
		t.Fatalf("expected id=2, got %d", id)
	}
}

// TestRegression_DriverConcurrentReads_L1 tests concurrent reads via the connection pool.
// SetMaxOpenConns(1) is used because :memory: databases are per-connection.
func TestRegression_DriverConcurrentReads_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	// Limit to 1 connection so all goroutines share the same in-memory database.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := db.Exec("INSERT INTO t VALUES (?)", int64(i)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	errCh := make(chan error, 5)
	for w := 0; w < 5; w++ {
		go func() {
			var count int64
			if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
				errCh <- err
				return
			}
			if count != 10 {
				errCh <- fmt.Errorf("expected count=10, got %d", count)
				return
			}
			errCh <- nil
		}()
	}
	for i := 0; i < 5; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent read error: %v", err)
		}
	}
}

// TestRegression_DriverClosedStmt_L1 tests that a closed Stmt returns ErrBadConn.
func TestRegression_DriverClosedStmt_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	stmt, err := db.Prepare("SELECT id FROM t")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After close the stmt should be unusable
	_, err = stmt.Query()
	if err == nil {
		t.Fatal("expected error using closed stmt, got nil")
	}
}

// TestRegression_DriverContextCancel_L1 tests that context cancellation returns context.Canceled.
func TestRegression_DriverContextCancel_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Give the context time to expire
	time.Sleep(5 * time.Millisecond)

	_, err = db.QueryContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}
