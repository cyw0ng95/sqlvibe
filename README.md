# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
| **v0.8.9** | 2026-02-22 | CLI tools (sv-cli, sv-check), Info APIs, Integrity check |

## Features

- **SQL:1999 compatibility** — 84+ test suites passing (added F870/F871/F872)
- **In-memory databases** — `:memory:` URI for fast, ephemeral storage
- **Comprehensive SQL**: DDL, DML, JOINs, Subqueries, Aggregates, Window functions (ROW_NUMBER/RANK/LAG/LEAD/NTILE/PERCENT_RANK/CUME_DIST), CTEs (recursive), VALUES derived tables, ANY/ALL subqueries, GROUP_CONCAT, etc.
- **VIEW Support** — `CREATE VIEW`, `DROP VIEW`, query views like tables, INSTEAD OF triggers for updatable views
- **VACUUM** — `VACUUM` (in-place compaction) and `VACUUM INTO 'path'` (snapshot to file)
- **ANALYZE** — `ANALYZE` collects table/index statistics accessible via `sqlite_stat1`
- **Extended PRAGMAs** — `page_size`, `mmap_size`, `locking_mode`, `synchronous`, `auto_vacuum`, `query_only`, `temp_store`, `read_uncommitted`, `cache_spill`
- **New Functions** — `UNHEX()`, `RANDOM()`, `RANDOMBLOB()`, `ZEROBLOB()`, `IIF()`
- **Foreign Key Enforcement** — `PRAGMA foreign_keys = ON`, inline REFERENCES, table-level FOREIGN KEY, ON DELETE CASCADE/RESTRICT/SET NULL, ON UPDATE CASCADE
- **Trigger Support** — `CREATE TRIGGER` / `DROP TRIGGER`, BEFORE/AFTER INSERT/UPDATE/DELETE, WHEN conditions, UPDATE OF column filters
- **AUTOINCREMENT** — Monotonically increasing INTEGER PRIMARY KEY with `sqlite_sequence` tracking
- **DateTime Functions** — `julianday()`, `unixepoch()`, extended `strftime()` with `%w`/`%W`/`%s`/`%J`
- **String Functions** — `printf()`/`format()`, `quote()`, `hex()`, `char()`, `unicode()`, `instr()`
- **Concurrency & Transactions** — WAL mode, MVCC snapshot isolation, configurable isolation levels (READ UNCOMMITTED / READ COMMITTED / SERIALIZABLE), deadlock detection, busy timeout
- **Advanced Compression** — Pluggable compression via `PRAGMA compression`: NONE, RLE, LZ4, ZSTD, GZIP
- **Incremental Backup** — `BACKUP DATABASE TO 'path'` and `BACKUP INCREMENTAL TO 'path'` SQL commands
- **Storage Metrics** — `PRAGMA storage_info` for page counts, WAL size, compression ratio
- **Extended PRAGMAs** — `foreign_keys`, `encoding`, `collation_list`, `sqlite_sequence`, `wal_mode`, `isolation_level`, `busy_timeout`, `compression`, `storage_info`

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

## Performance (v0.8.9)

Benchmarks on AMD EPYC 7763 (CI environment), in-memory database, `-benchtime=3x` (3 runs averaged).
**Methodology**: both sides iterate all result rows end-to-end for a fair comparison
(`go test ./internal/TS/Benchmark/... -bench=. -benchtime=3x`).
Results may vary on different hardware; relative ordering reflects the algorithmic advantages
of sqlvibe's in-memory columnar engine.

### Query Performance (1 000-row table)

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| SELECT all | 22 µs | 590 µs | **sqlvibe 27x faster** |
| SELECT WHERE | 101 µs | 120 µs | **sqlvibe 1.2x faster** |
| ORDER BY | 72 µs | 328 µs | **sqlvibe 4.5x faster** |
| COUNT(*) | 4.4 µs | 28 µs | **sqlvibe 6.4x faster** |
| SUM | 3.9 µs | 100 µs | **sqlvibe 26x faster** |
| GROUP BY | 50 µs | 564 µs | **sqlvibe 11x faster** |
| JOIN | 182 µs | 275 µs | **sqlvibe 1.5x faster** |

### DML Operations

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| INSERT single | 9.6 µs | 50 µs | **sqlvibe 5x faster** |
| INSERT 100 (batch) | 311 µs | 741 µs | **sqlvibe 2.4x faster** |

### Special-case Performance

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| Result cache hit (repeated query) | 2 µs | 190 µs | **sqlvibe 95x faster** |
| TOP-N LIMIT 10 (10K rows) | 571 µs | 1.23 ms | **sqlvibe 2.2x faster** |
| COUNT(*) via index (1K, PK table) | 84 µs | 30 µs | SQLite 2.8x faster |
| Full scan + filter (10K, no index) | 1.87 ms | 1.52 ms | SQLite 1.2x faster |

> **Note**: sqlvibe is an in-memory engine optimised for query throughput on small-to-medium
> tables. SQLite's B-Tree index scans remain faster for COUNT(*) on indexed columns, and raw
> sequential scans on very large tables benefit from SQLite's lower per-row overhead.

### Key Optimizations

- **Columnar storage**: Fast full table scans via vectorised SIMD-friendly layouts
- **Hybrid row/column**: Adaptive switching for best performance per workload
- **Result cache**: Near-zero latency for repeated identical queries (FNV-1a keyed)
- **Predicate pushdown**: WHERE conditions evaluated before VM for fast filtered scans
- **Plan cache**: Skip tokenise/parse/codegen for cached query plans
- **Batch INSERT fast path**: Literal multi-row INSERT bypasses VM entirely
- **sync.Pool allocation reduction**: Pooled schema maps reduce per-query allocations

## SQL:1999 Compatibility

84+ test suites passing

## Building

```bash
go build ./...
go test ./...
go test ./internal/TS/Benchmark/... -bench . -benchmem
```

## License

See LICENSE file.
