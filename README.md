# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

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

## Performance (v0.8.7)

Benchmarks on AMD EPYC 7763 @ 2.45GHz, in-memory database, `-benchtime=1s -benchmem`.

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

84+ test suites passing

## Building

```bash
go build ./...
go test ./...
go test ./internal/TS/Benchmark/... -bench . -benchmem
```

## License

See LICENSE file.
