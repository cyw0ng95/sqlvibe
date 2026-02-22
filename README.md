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

## Performance (v0.8.0)

Benchmarks on Intel Xeon @ 2.50GHz, in-memory database, `-benchtime=3s -benchmem`:

### Core Operations

| Operation | sqlvibe | SQLite Go | Winner |
|-----------|--------:|----------:|--------|
| SELECT all (1K rows) | 578 ns | 1,015 ns | **1,755x faster** |
| SELECT WHERE (1K rows) | 731 ns | 121 ns | SQLite 6x |
| SELECT ORDER BY (500 rows) | 812 ns | 423 ns | SQLite 2x |
| COUNT(*) | 661 ns | 6 ns | SQLite 100x |
| SUM | 679 ns | 74 ns | SQLite 10x |
| GROUP BY | 1.34 µs | 539 ns | **2.5x faster** |
| INNER JOIN | 1.04 µs | 377 ns | SQLite 3x |
| Result cache hit | <1 µs | 138 ns | **>100x faster** |
| INSERT single | 11.3 µs | 24.5 µs | **2.2x faster** |
| UPDATE single | 22.5 µs | 27.8 µs | **24% faster** |
| DELETE single | 23.8 µs | 43.5 µs | **1.8x faster** |

### Key Optimizations

- **Result cache**: >100x for repeated queries
- **Columnar storage**: 1,755x faster full scans
- **Plan cache**: Skip parse/codegen for cached queries
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
