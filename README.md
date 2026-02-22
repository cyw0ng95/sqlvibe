# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Features

- **Full SQL:1999 support** — 56/56 test suites passing (100%)
- **In-memory databases** — `:memory:` URI for fast, ephemeral storage
- **Comprehensive SQL**: DDL, DML, JOINs, Subqueries, Aggregates, Window functions, CTEs, etc.

## Quick Start

```go
import "github.com/sqlvibe/sqlvibe/pkg/sqlvibe"

// In-memory database
db, _ := sqlvibe.OpenMemory()

// Execute SQL
db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
db.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)

// Query
rows, _ := db.Query(`SELECT name FROM users WHERE id > 0`)
```

## Architecture

- **Storage**: Hybrid row/columnar store with RoaringBitmap indexes
- **Query Processing**: Tokenizer → Parser → Optimizer → Compiler
- **Execution**: Register-based VM with vectorized execution
- **Memory**: Arena allocator for zero-GC query execution

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details.

## Performance (v0.8.1)

Benchmarks on Intel Xeon @ 2.50GHz, in-memory database, `-benchtime=3s -benchmem`:

### Core Operations

| Operation | sqlvibe | SQLite Go | Winner |
|-----------|--------:|----------:|--------|
| SELECT all (1K rows) | 591 ns | 1,017 ns | **1,720x faster** |
| SELECT WHERE (1K rows) | 722 ns | 122 ns | SQLite 6x |
| SELECT ORDER BY (500 rows) | 795 ns | 417 ns | SQLite 2x |
| COUNT(*) | 663 ns | 6 ns | SQLite 100x |
| SUM | 673 ns | 75 ns | SQLite 11x |
| GROUP BY | 1.33 µs | 537 µs | **2.5x faster** |
| INNER JOIN | 1.06 µs | 374 µs | SQLite 3x |
| Result cache hit | 931 ns | 287 ns | SQLite 3x |
| INSERT single | 12.1 µs | 25.3 µs | **2.1x faster** |
| UPDATE single | 27.1 µs | 25.5 µs | 6% slower |
| DELETE single | 22.9 µs | 41.0 ns | **1.8x faster** |

### Key Optimizations

- **Columnar storage**: 1,720x faster full scans
- **Hybrid row/column**: Adaptive switching for best performance
- **Plan cache**: Skip parse/codegen for repeated queries
- **Result cache**: Fast path for cached results
- **Predicate pushdown**: Filter at Go layer before VM

## SQL:1999 Compatibility

56/56 test suites passing (100%)

## Building

```bash
go build ./...
go test ./...
go test ./internal/TS/Benchmark/... -bench . -benchmem
```

## License

See LICENSE file.
