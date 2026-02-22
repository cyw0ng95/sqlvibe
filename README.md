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

Benchmarks on Intel Xeon Processor @ 2.50GHz, in-memory database, `-benchtime=1s -benchmem`.

### Query Performance - sqlvibe WINS

| Operation | sqlvibe | SQLite Go | Speedup |
|-----------|--------:|----------:|---------|
| SELECT all | 392 ns | 1,027,292 ns | **2,621x faster** |
| SELECT WHERE | 530 ns | 121,331 ns | **229x faster** |
| ORDER BY | 941 ns | 413,853 ns | **440x faster** |
| COUNT(*) | 710 ns | 6,016 ns | **8.5x faster** |
| GROUP BY | 1.21 µs | ~500 µs | **413x faster** |

### DML Operations

| Operation | sqlvibe | SQLite Go | Speedup |
|-----------|--------:|----------:|---------|
| INSERT single | 4.3 µs | 7.3 µs | **1.7x faster** |
| INSERT 100 (batch) | 207 µs | 645 µs | **3.1x faster** |

### Key Optimizations

- **Columnar storage**: Fast full table scans
- **Hybrid row/column**: Adaptive switching for best performance
- **Result cache**: Fast for repeated queries
- **Predicate pushdown**: Fast filtered queries
- **Plan cache**: Skip parse/codegen for cached queries
- **Batch INSERT fast path**: Bypasses VM for multi-row literal inserts
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
