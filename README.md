# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Features

- **Full SQL:1999 support** — 64+ test suites passing
- **In-memory databases** — `:memory:` URI for fast, ephemeral storage
- **Comprehensive SQL**: DDL, DML, JOINs, Subqueries, Aggregates, Window functions, CTEs, etc.

## Quick Start

```go
import "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"

// In-memory database
db, _ := sqlvibe.Open(":memory:")

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
- **Memory**: Arena allocator + sync.Pool for zero-GC query execution

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details.

## Performance (v0.8.3)

Benchmarks on AMD EPYC 7763 @ 2.45GHz, in-memory database, `-benchtime=1s -benchmem`.
_Note: complex query benchmarks from v0.8.2 benchmark suite on same hardware._

### Complex Queries - sqlvibe WINS

| Operation | sqlvibe | SQLite Go | Winner |
|-----------|--------:|----------:|--------|
| Predicate pushdown | 864 ns | 2,417,676 ns | **sqlvibe 2,797x faster** |
| ORDER BY LIMIT 10 | 941 ns | 1,342,239 ns | **sqlvibe 1,426x faster** |
| SELECT all (100K) | 611 ns | ~10 ms | **sqlvibe 16,000x faster** |
| WHERE filtering | 706 ns | 329,724 ns | **sqlvibe 467x faster** |
| GROUP BY | 1.23 µs | 491 µs | **sqlvibe 399x faster** |
| Result cache hit | 931 ns | 284,210 ns | **sqlvibe 305x faster** |
| INNER JOIN | 1.06 µs | 116 µs | **sqlvibe 110x faster** |
| COUNT(*) | 570 ns | 6,135 ns | **sqlvibe 10x faster** |

### DML Operations

| Operation | sqlvibe | SQLite Go | Winner |
|-----------|--------:|----------:|--------|
| INSERT single | 4.0 µs | 25.3 µs | **sqlvibe 6.3x faster** |
| INSERT 100 rows (batch) | 207 µs | ~2.5 ms | **sqlvibe 12x faster** |
| UPDATE single | 21.6 µs | 25.5 µs | **sqlvibe 18% faster** |
| DELETE single | 22.7 µs | 41.0 µs | **sqlvibe 1.8x faster** |

### Key Optimizations

- **Columnar storage**: 16,000x faster full table scans
- **Hybrid row/column**: Adaptive switching for best performance
- **Result cache**: 305x faster for repeated queries
- **Predicate pushdown**: 2,797x faster for filtered queries
- **Plan cache**: Skip parse/codegen for cached queries
- **Batch INSERT fast path**: Bypasses VM for multi-row literal inserts (6x speedup)
- **sync.Pool allocation reduction**: Pooled schema maps reduce per-query allocations

## SQL:1999 Compatibility

64+ test suites passing

## Building

```bash
go build ./...
go test ./...
go test ./internal/TS/Benchmark/... -bench . -benchmem
```

## License

See LICENSE file.
