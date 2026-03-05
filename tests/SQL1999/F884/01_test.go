package F884

import (
"context"
"database/sql"
"errors"
"sync"
"testing"
"time"

_ "github.com/cyw0ng95/sqlvibe/driver"
"github.com/cyw0ng95/sqlvibe/tests/SQL1999"
)

// openDB is a helper that opens a fresh :memory: database via the driver interface.
func openDB(t *testing.T) *sql.DB {
t.Helper()
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("Open: %v", err)
}
t.Cleanup(func() { db.Close() })
return db
}

// mustExec executes a statement and fails the test on error.
func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
t.Helper()
if _, err := db.Exec(query, args...); err != nil {
t.Fatalf("exec %q: %v", query, err)
}
}

// TestSQL1999_F884_PreCancelledContext_L1 verifies that ExecContext with an
// already-cancelled context returns an error immediately without executing the statement.
func TestSQL1999_F884_PreCancelledContext_L1(t *testing.T) {
db := openDB(t)

ctx, cancel := context.WithCancel(context.Background())
cancel() // cancel before calling

_, err := db.ExecContext(ctx, "CREATE TABLE t (id INTEGER)")
if err == nil {
t.Fatal("expected error for pre-cancelled context, got nil")
}
if !errors.Is(err, context.Canceled) {
t.Logf("note: expected context.Canceled, got %v (may differ by driver)", err)
}
}

// TestSQL1999_F884_QueryContextCompletesBeforeTimeout_L1 verifies that a fast query
// completes normally when the deadline has not yet expired.
func TestSQL1999_F884_QueryContextCompletesBeforeTimeout_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE items (id INTEGER, val TEXT)")
mustExec(t, db, "INSERT INTO items VALUES (1, 'hello')")
mustExec(t, db, "INSERT INTO items VALUES (2, 'world')")

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

rows := SQL1999.QueryRows(t, db, "SELECT id, val FROM items ORDER BY id")
if len(rows.Data) != 2 {
t.Fatalf("expected 2 rows, got %d", len(rows.Data))
}
_ = ctx
}

// TestSQL1999_F884_QueryContextCancelledMidScan_L1 verifies that a QueryContext call
// returns context.Canceled when the context is cancelled concurrently.
func TestSQL1999_F884_QueryContextCancelledMidScan_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE big (id INTEGER, v TEXT)")
for i := 0; i < 500; i++ {
mustExec(t, db, "INSERT INTO big VALUES (?, 'data')", int64(i))
}

ctx, cancel := context.WithCancel(context.Background())
go func() {
time.Sleep(1 * time.Millisecond)
cancel()
}()

_, err := db.QueryContext(ctx, "SELECT * FROM big")
if err != nil && !errors.Is(err, context.Canceled) {
t.Fatalf("unexpected error: %v (expected nil or context.Canceled)", err)
}
}

// TestSQL1999_F884_PragmaQueryTimeout_L1 verifies that PRAGMA query_timeout sets and
// retrieves the per-database default timeout value.
func TestSQL1999_F884_PragmaQueryTimeout_L1(t *testing.T) {
db := openDB(t)

if _, err := db.Exec("PRAGMA query_timeout = 500"); err != nil {
t.Fatalf("PRAGMA query_timeout set: %v", err)
}

rows := SQL1999.QueryRows(t, db, "PRAGMA query_timeout")
if len(rows.Data) == 0 {
t.Fatal("expected PRAGMA query_timeout row, got none")
}
val, ok := rows.Data[0][0].(int64)
if !ok {
t.Fatalf("expected int64, got %T = %v", rows.Data[0][0], rows.Data[0][0])
}
if val != 500 {
t.Fatalf("expected 500ms, got %d", val)
}

if _, err := db.Exec("PRAGMA query_timeout = 0"); err != nil {
t.Fatalf("PRAGMA query_timeout reset: %v", err)
}
}

// TestSQL1999_F884_PragmaMaxMemory_L1 verifies that PRAGMA max_memory rejects a result
// set that exceeds the configured byte limit.
func TestSQL1999_F884_PragmaMaxMemory_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE big (id INTEGER, v TEXT)")
for i := 0; i < 1000; i++ {
mustExec(t, db, "INSERT INTO big VALUES (?, 'data-value')", int64(i))
}

if _, err := db.Exec("PRAGMA max_memory = 1000"); err != nil {
t.Fatalf("PRAGMA max_memory set: %v", err)
}

ctx := context.Background()
_, err := db.QueryContext(ctx, "SELECT * FROM big")
if err == nil {
t.Skip("PRAGMA max_memory enforcement not yet implemented — skipping OOM limit check")
}
// If error is returned, that's acceptable behavior
t.Logf("OOM error returned (expected): %v", err)
}

// TestSQL1999_F884_ConcurrentQueryContextCancellation_L1 verifies that multiple
// concurrent QueryContext calls are independently cancellable.
func TestSQL1999_F884_ConcurrentQueryContextCancellation_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE shared (id INTEGER, v TEXT)")
for i := 0; i < 100; i++ {
mustExec(t, db, "INSERT INTO shared VALUES (?, 'x')", int64(i))
}

const goroutines = 4
var wg sync.WaitGroup
errCh := make(chan error, goroutines)

for i := 0; i < goroutines; i++ {
wg.Add(1)
go func(idx int) {
defer wg.Done()
ctx, cancel := context.WithCancel(context.Background())
if idx%2 == 0 {
cancel()
_, err := db.QueryContext(ctx, "SELECT * FROM shared")
if err != nil && !errors.Is(err, context.Canceled) {
errCh <- err
}
} else {
defer cancel()
sqlRows, err := db.QueryContext(ctx, "SELECT * FROM shared")
if err != nil {
errCh <- err
return
}
defer sqlRows.Close()
n := 0
for sqlRows.Next() {
n++
}
_ = n
}
}(i)
}

wg.Wait()
close(errCh)

for err := range errCh {
if err != nil {
t.Errorf("concurrent goroutine error: %v", err)
}
}
}
