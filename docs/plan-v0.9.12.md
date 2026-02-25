# Plan v0.9.12 - database/sql Driver Interface

## Summary

This version implements a fully compliant Go `database/sql` driver for sqlvibe, enabling
the database to be used through the standard `database/sql` API that the entire Go
ecosystem expects. After this release, any Go program that uses `database/sql` can
switch to sqlvibe by changing only the import path and the driver name string.

---

## Background

Go's `database/sql` package defines the standard interface that all database drivers
must implement (`driver.Driver`, `driver.Conn`, `driver.Stmt`, `driver.Rows`,
`driver.Result`, `driver.Tx`, etc.). sqlvibe currently provides its own
`*Database`, `*Rows`, and `Result` types that are idiomatic but incompatible with
`database/sql`. Adding the driver layer is the single most important step toward
production-readiness, as it unlocks every ORM, migration tool, and query builder in
the Go ecosystem.

---

## Track A: Driver Implementation

### A1. Package Layout

Create a new `driver/` package alongside `pkg/sqlvibe/`:

```
driver/
├── driver.go        // sql.Register + Driver interface
├── conn.go          // driver.Conn + driver.ConnBeginTx + driver.ExecerContext + driver.QueryerContext
├── stmt.go          // driver.Stmt + driver.StmtExecContext + driver.StmtQueryContext
├── rows.go          // driver.Rows
├── result.go        // driver.Result
├── tx.go            // driver.Tx
└── value.go         // driver.Value conversion helpers
```

### A2. `driver.Driver`

```go
// driver/driver.go
package driver

import (
    "database/sql"
    "database/sql/driver"
    "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

const DriverName = "sqlvibe"

func init() {
    sql.Register(DriverName, &Driver{})
}

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
    db, err := sqlvibe.Open(name)
    if err != nil {
        return nil, err
    }
    return &Conn{db: db}, nil
}
```

### A3. `driver.Conn`

```go
// driver/conn.go
type Conn struct {
    db *sqlvibe.Database
}

func (c *Conn) Prepare(query string) (driver.Stmt, error)
func (c *Conn) Close() error
func (c *Conn) Begin() (driver.Tx, error)

// Optional but important for production:
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error)
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
```

### A4. `driver.Stmt`

```go
// driver/stmt.go
type Stmt struct {
    stmt  *sqlvibe.Statement
    query string
    conn  *Conn
}

func (s *Stmt) Close() error
func (s *Stmt) NumInput() int  // -1 = driver inspects dynamically
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error)
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error)

// Context variants:
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error)
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error)
```

### A5. `driver.Rows`

```go
// driver/rows.go
type Rows struct {
    rows    *sqlvibe.Rows
    pos     int
    columns []string
}

func (r *Rows) Columns() []string
func (r *Rows) Close() error
func (r *Rows) Next(dest []driver.Value) error
```

### A6. Value Conversion

`driver/value.go` — bidirectional conversion between `driver.Value` and `interface{}`:

```go
// toDriverValue converts a sqlvibe value to a driver.Value.
// Supported: nil, int64, float64, string, []byte, time.Time
func toDriverValue(v interface{}) (driver.Value, error)

// fromNamedValues converts []driver.NamedValue to positional []interface{}
// and named map[string]interface{} for use with ExecWithParams/QueryNamed.
func fromNamedValues(args []driver.NamedValue) ([]interface{}, map[string]interface{})
```

---

## Track B: Context & Cancellation in the Driver

### B1. Context Propagation

The driver's `ExecContext` / `QueryContext` must respect `ctx.Done()`. Since the current
execution engine is synchronous, implement a goroutine + channel wrapper:

```go
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
    type result struct {
        rows *sqlvibe.Rows
        err  error
    }
    ch := make(chan result, 1)
    pos, named := fromNamedValues(args)
    go func() {
        var rows *sqlvibe.Rows
        var err error
        if len(named) > 0 {
            rows, err = c.db.QueryNamed(query, named)
        } else {
            rows, err = c.db.QueryWithParams(query, pos)
        }
        ch <- result{rows, err}
    }()
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    case r := <-ch:
        if r.err != nil {
            return nil, r.err
        }
        return &Rows{rows: r.rows}, nil
    }
}
```

### B2. Deadline Support

Honour `ctx.Deadline()` by setting a query timeout before execution (uses the
busy_timeout PRAGMA mechanism already in place in `internal/TM/isolation.go`).

---

## Track C: Compatibility Tests

### C1. F883 SQL1999 Suite

Add `internal/TS/SQL1999/F883/01_test.go`:

- Open via `sql.Open("sqlvibe", ":memory:")`
- `db.Exec` DDL round trip
- `db.QueryRow` with `?` params
- `db.Query` with scan into Go types
- `db.Begin` / `tx.Exec` / `tx.Commit`
- `db.Begin` / `tx.Rollback`
- `db.Prepare` / `stmt.Query` / `stmt.Close`

### C2. Regression Suite v0.9.12

Add `internal/TS/Regression/regression_v0.9.12_test.go`:

- Type round-trip: int64, float64, string, `[]byte`, nil, `time.Time`
- Named value args via `sql.Named`
- Concurrent read queries through `database/sql` connection pool
- Row scan error on type mismatch
- Closed stmt returns `driver.ErrBadConn`
- Context cancellation returns `context.Canceled`

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `driver/driver.go` | **NEW** — `Driver`, `sql.Register` |
| `driver/conn.go` | **NEW** — `Conn` with full context interface |
| `driver/stmt.go` | **NEW** — `Stmt` |
| `driver/rows.go` | **NEW** — `Rows` |
| `driver/result.go` | **NEW** — `Result` |
| `driver/tx.go` | **NEW** — `Tx` |
| `driver/value.go` | **NEW** — value conversion helpers |
| `internal/TS/SQL1999/F883/01_test.go` | **NEW** — database/sql feature tests |
| `internal/TS/Regression/regression_v0.9.12_test.go` | **NEW** — driver compatibility regressions |
| `docs/HISTORY.md` | Add v0.9.12 entry |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| `sql.Open("sqlvibe", ...)` works | Yes | [x] |
| `db.Exec` DDL works | Yes | [x] |
| `db.Query` + scan works | Yes | [x] |
| `db.Prepare` + `stmt.Query` works | Yes | [x] |
| `db.Begin` / `tx.Commit` / `tx.Rollback` works | Yes | [x] |
| `?` positional params via `database/sql` | Yes | [x] |
| Named params via `sql.Named` | Yes | [x] |
| `context.Context` cancellation respected | Yes | [x] |
| Type round-trip: int64/float64/string/[]byte/nil | Yes | [x] |
| F883 suite passes | 100% | [x] |
| Regression v0.9.12 passes | 100% | [x] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F883 suite | database/sql driver end-to-end (7+ tests) | [x] |
| Regression v0.9.12 | Driver compatibility + context (6+ tests) | [x] |
