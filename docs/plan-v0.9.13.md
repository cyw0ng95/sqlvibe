# Plan v0.9.13 - Context API & Query Timeouts

## Summary

This version adds first-class `context.Context` support to sqlvibe's native API
(`ExecContext`, `QueryContext`) and implements query-level timeout enforcement inside the
execution engine. Production embedded databases must never let runaway queries block the
calling goroutine indefinitely; context cancellation is the standard Go mechanism for
this. This version also adds per-query and per-database memory limits to protect against
OOM in constrained environments.

---

## Background

v0.9.12 added context propagation at the `database/sql` driver boundary via a goroutine
wrapper. That approach is correct for the driver layer but is not true cancellation —
the underlying query keeps running until it finishes. This version wires cancellation
deeper: into the VM execution loop so that long-running scans and joins can be
interrupted mid-flight.

---

## Track A: Native Context API

### A1. `ExecContext` / `QueryContext` on `*Database`

Add context-aware variants to `pkg/sqlvibe/database.go`:

```go
func (db *Database) ExecContext(ctx context.Context, sql string) (Result, error)
func (db *Database) QueryContext(ctx context.Context, sql string) (*Rows, error)
func (db *Database) ExecContextWithParams(ctx context.Context, sql string, params []interface{}) (Result, error)
func (db *Database) QueryContextWithParams(ctx context.Context, sql string, params []interface{}) (*Rows, error)
```

The context is stored on a per-call execution state struct that is threaded through the
call stack.

### A2. Execution State Struct

Introduce a lightweight `execState` struct (not exported) that carries per-query
context and options:

```go
// pkg/sqlvibe/exec_state.go (new file)
type execState struct {
    ctx       context.Context
    deadline  time.Time
    hasDeadline bool
    rowsChecked int64  // counter for periodic context checks
}

func newExecState(ctx context.Context) *execState
func (s *execState) check() error // returns ctx.Err() if cancelled
```

### A3. Context Check in Scan Loops

Insert `execState.check()` calls at natural checkpoints in the hot execution paths:

```
pkg/sqlvibe/database.go — execSelectStmt inner row loop:
  Every 256 rows: if s.check() != nil { return nil, s.check() }

pkg/sqlvibe/exec_columnar.go — ColumnarHashJoin build phase:
  Every 256 rows: same check.

pkg/sqlvibe/vm_exec.go — execVMQuery main result-row loop:
  After each OpResultRow: same check.
```

The 256-row granularity keeps overhead negligible (one atomic load per 256 iterations)
while providing ≤256-row response latency for cancellation.

---

## Track B: Query Timeout

### B1. `PRAGMA query_timeout = N` (milliseconds)

Add a database-level default timeout (0 = no limit):

```
pkg/sqlvibe/pragma.go:
- pragmaQueryTimeout(n int) → sets db.queryTimeoutMs
- pragmaGetQueryTimeout() → returns current value
```

### B2. Per-Call Timeout via `context.WithTimeout`

Document and test the idiomatic pattern:

```go
ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
defer cancel()
rows, err := db.QueryContext(ctx, "SELECT * FROM big_table")
```

### B3. Timeout Error Type

Wrap `context.DeadlineExceeded` in a sqlvibe-native error code:

```go
// pkg/sqlvibe/error_code.go — add:
SVDB_QUERY_TIMEOUT ErrorCode = ...
```

Return `SVDB_QUERY_TIMEOUT` when the query is cancelled due to deadline expiry so
callers can distinguish timeout from user cancellation.

---

## Track C: Memory Limit

### C1. `PRAGMA max_memory = N` (bytes, 0 = unlimited)

Add a soft memory limit to protect against unbounded result sets:

```
pkg/sqlvibe/pragma.go:
- pragmaMaxMemory(n int64) → sets db.maxMemoryBytes
- pragmaGetMaxMemory() → returns current value
```

### C2. Result Set Size Guard

In `execSelectStmt` after each row is appended to the result set, estimate the memory
used (row count × estimated row width) and return `SVDB_OOM_LIMIT` if the limit is
exceeded:

```go
if db.maxMemoryBytes > 0 && estimatedBytes > db.maxMemoryBytes {
    return nil, NewError(SVDB_OOM_LIMIT, "result set exceeds max_memory limit")
}
```

### C3. Streaming Cursor (Future-safe design)

Design the `execState` struct so it can later carry a row callback (for streaming
results one row at a time without materialising the full result set). This is a
structural placeholder — no implementation in v0.9.13, just the interface stub:

```go
// pkg/sqlvibe/exec_state.go
type RowCallback func(cols []string, row []interface{}) error
// (reserved for v0.9.14+)
```

---

## Track D: Testing

### D1. F884 SQL1999 Suite

Add `internal/TS/SQL1999/F884/01_test.go`:

- `ExecContext` with already-cancelled context returns error immediately
- `QueryContext` with deadline completes before short timeout fires
- `QueryContext` cancelled mid-scan returns `context.Canceled`
- `PRAGMA query_timeout` fires on a long-running scan
- `PRAGMA max_memory` rejects an oversized result set
- Multiple concurrent `QueryContext` calls are independently cancellable

### D2. Regression Suite v0.9.13

Add `internal/TS/Regression/regression_v0.9.13_test.go`:

- Cancelled context on `ExecContext` DDL does not leave partial schema state
- Deadline-expired query does not corrupt in-progress transaction
- `SVDB_QUERY_TIMEOUT` error code is returned (not raw `context.DeadlineExceeded`)
- `max_memory = 0` means unlimited (no false positives)
- Row counter resets correctly between queries on the same connection

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `pkg/sqlvibe/exec_state.go` | **NEW** — `execState` struct, context check helper |
| `pkg/sqlvibe/database.go` | Add `ExecContext`, `QueryContext`, `ExecContextWithParams`, `QueryContextWithParams`; thread `execState` into scan loops; add `queryTimeoutMs`, `maxMemoryBytes` fields |
| `pkg/sqlvibe/pragma.go` | Add `pragmaQueryTimeout`, `pragmaMaxMemory` |
| `pkg/sqlvibe/error_code.go` | Add `SVDB_QUERY_TIMEOUT`, `SVDB_OOM_LIMIT` |
| `pkg/sqlvibe/vm_exec.go` | Thread `execState` check into result-row loop |
| `pkg/sqlvibe/exec_columnar.go` | Thread `execState` check into join build loop |
| `driver/conn.go` | Replace goroutine wrapper with native `ExecContext` / `QueryContext` |
| `internal/TS/SQL1999/F884/01_test.go` | **NEW** — context + timeout feature tests |
| `internal/TS/Regression/regression_v0.9.13_test.go` | **NEW** — cancellation safety regressions |
| `docs/HISTORY.md` | Add v0.9.13 entry |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| `ExecContext` / `QueryContext` on `*Database` | Yes | [ ] |
| Pre-cancelled context rejected immediately | Yes | [ ] |
| Running query cancelled mid-scan | Yes | [ ] |
| `PRAGMA query_timeout` fires correctly | Yes | [ ] |
| `PRAGMA max_memory` rejects oversized results | Yes | [ ] |
| `SVDB_QUERY_TIMEOUT` error code returned | Yes | [ ] |
| driver/conn.go updated to use native API | Yes | [ ] |
| F884 suite passes | 100% | [ ] |
| Regression v0.9.13 passes | 100% | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F884 suite | Context/timeout feature tests (6+ tests) | [ ] |
| Regression v0.9.13 | Cancellation safety regressions (5+ tests) | [ ] |
