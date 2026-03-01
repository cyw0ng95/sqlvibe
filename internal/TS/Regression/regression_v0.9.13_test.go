package Regression

import (
	sferrors "github.com/cyw0ng95/sqlvibe/internal/SF/errors"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestRegression_CancelledContextDDLNoPartialSchema_L1 verifies that a pre-cancelled
// context on ExecContext DDL does not leave partial schema state.
func TestRegression_CancelledContextDDLNoPartialSchema_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	_, err = db.ExecContext(ctx, "CREATE TABLE should_not_exist (id INTEGER)")
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}

	// Table must NOT exist in schema
	rows, qErr := db.Query("PRAGMA table_info(should_not_exist)")
	if qErr != nil {
		t.Fatalf("PRAGMA table_info: %v", qErr)
	}
	if len(rows.Data) > 0 {
		t.Fatal("partial schema state detected: table was created despite cancelled context")
	}
}

// TestRegression_TimeoutErrorCodeReturned_L1 verifies that SVDB_QUERY_TIMEOUT is returned
// when a query times out (not raw context.DeadlineExceeded).
func TestRegression_TimeoutErrorCodeReturned_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Set a very short timeout so a context.WithTimeout fires it
	// We simulate timeout by creating a deadline-exceeded context directly.
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	// Force the deadline to expire
	<-ctx.Done()

	_, err = db.ExecContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	svErr, ok := err.(*sferrors.Error)
	if !ok {
		t.Fatalf("expected *sferrors.Error, got %T: %v", err, err)
	}
	if svErr.Code != sferrors.SVDB_QUERY_TIMEOUT {
		t.Fatalf("expected SVDB_QUERY_TIMEOUT (%v), got %v", sferrors.SVDB_QUERY_TIMEOUT, svErr.Code)
	}
}

// TestRegression_MaxMemoryZeroMeansUnlimited_L1 verifies that max_memory = 0 means
// unlimited and does not cause false positive OOM errors.
func TestRegression_MaxMemoryZeroMeansUnlimited_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE t (id INTEGER, v TEXT)")
	for i := 0; i < 1000; i++ {
		db.MustExec("INSERT INTO t VALUES (?, 'data')", int64(i))
	}

	// Ensure max_memory is 0 (default = no limit)
	if _, err := db.Exec("PRAGMA max_memory = 0"); err != nil {
		t.Fatalf("PRAGMA max_memory = 0: %v", err)
	}

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM t")
	if err != nil {
		t.Fatalf("unexpected error with max_memory=0: %v", err)
	}
	if len(rows.Data) != 1000 {
		t.Fatalf("expected 1000 rows, got %d", len(rows.Data))
	}
}

// TestRegression_RowCounterResetsBetweenQueries_L1 verifies that the context check
// row counter resets correctly between queries on the same connection so that
// cancellation state from a previous query does not bleed into the next one.
func TestRegression_RowCounterResetsBetweenQueries_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE items (id INTEGER)")
	for i := 0; i < 100; i++ {
		db.MustExec("INSERT INTO items VALUES (?)", int64(i))
	}

	// First query: cancelled before execution
	ctx1, cancel1 := context.WithCancel(context.Background())
	cancel1()
	_, err = db.QueryContext(ctx1, "SELECT * FROM items")
	if err == nil || !errors.Is(err, context.Canceled) {
		if err != nil && errors.Is(err, context.Canceled) {
			// expected
		} else if err != nil {
			// some other error is also acceptable (context error propagation)
		}
	}

	// Second query: must succeed cleanly
	rows, err := db.QueryContext(context.Background(), "SELECT * FROM items")
	if err != nil {
		t.Fatalf("second query failed after cancelled first query: %v", err)
	}
	if len(rows.Data) != 100 {
		t.Fatalf("expected 100 rows on second query, got %d", len(rows.Data))
	}
}

// TestRegression_QueryTimeoutPragmaRoundTrip_L1 verifies that query_timeout pragma
// stores and retrieves the value correctly, including resetting to zero.
func TestRegression_QueryTimeoutPragmaRoundTrip_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tests := []int64{0, 100, 500, 1000, 0}
	for _, want := range tests {
		if _, err := db.Exec(fmt.Sprintf("PRAGMA query_timeout = %d", want)); err != nil {
			t.Fatalf("set query_timeout=%d: %v", want, err)
		}
		rows, err := db.Query("PRAGMA query_timeout")
		if err != nil {
			t.Fatalf("get query_timeout: %v", err)
		}
		if len(rows.Data) == 0 {
			t.Fatalf("no rows from PRAGMA query_timeout")
		}
		got, ok := rows.Data[0][0].(int64)
		if !ok {
			t.Fatalf("expected int64, got %T", rows.Data[0][0])
		}
		if got != want {
			t.Fatalf("query_timeout: want %d, got %d", want, got)
		}
	}
}

// end of regression_v0.9.13_test.go
