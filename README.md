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
- **Extension Framework** — Pluggable extensions via build tags (`SVDB_EXT_JSON`, `SVDB_EXT_MATH`); query via `sqlvibe_extensions` virtual table
- **JSON Extension** — Full SQLite JSON1-compatible functions: `json()`, `json_array()`, `json_extract()`, `json_object()`, `json_set()`, `json_type()`, `json_length()`, and more (requires `-tags SVDB_EXT_JSON`)
- **Math Extension** — Advanced math functions: `POWER()`, `SQRT()`, `MOD()`, trigonometric, exponential (requires `-tags SVDB_EXT_MATH`)
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

## Extension Framework (v0.9.0)

Extensions are compiled in via build tags:

```bash
# Default (no extensions)
go build ./...

# With JSON extension (SQLite JSON1-compatible)
go build -tags "SVDB_EXT_JSON" ./...

# With Math extension
go build -tags "SVDB_EXT_MATH" ./...

# Both
go build -tags "SVDB_EXT_JSON SVDB_EXT_MATH" ./...
```

Query which extensions are loaded:

```sql
SELECT * FROM sqlvibe_extensions;
-- name | description       | functions
-- json | JSON extension    | json,json_array,json_extract,...
-- math | Math extension    | POWER,SQRT,MOD,...
```

## Architecture

- **Storage**: Hybrid row/columnar store with RoaringBitmap indexes
- **Query Processing**: Tokenizer → Parser → Optimizer → Compiler
- **Execution**: Register-based VM with vectorized execution
- **Memory**: Arena allocator + sync.Pool for zero-GC query execution
- **Extensions**: Build-tag controlled, statically linked, auto-registered at startup

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details.

## Performance (v0.9.5)

Benchmarks on AMD EPYC 7763 (CI environment), in-memory database, `-benchtime=3s`.
**Methodology**: the result cache is cleared before each sqlvibe iteration via
`db.ClearResultCache()` so actual per-query execution cost is measured (not cache-hit
latency). SQLite's `database/sql` driver reuses prepared statements across iterations.
Both sides iterate all result rows end-to-end.
(`go test ./internal/TS/Benchmark/... -bench=BenchmarkFair_ -benchtime=3s`).
Results may vary on different hardware.

### Query Performance (1 000-row table)

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| SELECT all (3 cols) | 61 µs | 564 µs | **sqlvibe 9.3x faster** |
| SELECT WHERE | 284 µs | 91 µs | SQLite 3.1x faster |
| ORDER BY (500 rows) | 197 µs | 298 µs | **sqlvibe 1.5x faster** |
| COUNT(*) | 6.6 µs | 5.4 µs | roughly equal |
| SUM | 19.5 µs | 66 µs | **sqlvibe 3.4x faster** |
| GROUP BY | 140 µs | 497 µs | **sqlvibe 3.6x faster** |
| JOIN (100×500 rows) | 561 µs | 231 µs | SQLite 2.4x faster |
| BETWEEN filter | 497 µs | 184 µs | SQLite 2.7x faster |

### DML Operations

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| INSERT single | 6.5 µs | 6.2 µs | roughly equal |
| INSERT 100 (batch) | 564 µs | 572 µs | roughly equal |
| INSERT OR REPLACE | 40 µs | — | conflict + delete + re-insert |
| INSERT OR IGNORE | 9.7 µs | — | conflict silently skipped |

### Special-case Performance

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| Result cache hit (repeated query) | 1.46 µs | 564 µs | **sqlvibe 386x faster** |
| LIMIT 10 no ORDER BY (10K rows) | 20.2 µs | — | fast early-termination path |
| Expression bytecode eval | 99 ns | — | single-dispatch expression evaluation |

### SIMD Vectorization (v0.9.3)

| Operation | 256 elems | 1 024 elems | 4 096 elems |
|-----------|----------:|------------:|------------:|
| VectorSumInt64 | 70 ns | 251 ns | 988 ns |
| VectorSumFloat64 | 69 ns | 249 ns | 969 ns |
| VectorAddInt64 (1 024) | — | 567 ns | — |
| VectorMulFloat64 (1 024) | — | 503 ns | — |

4-way loop unrolling enables the Go compiler to auto-vectorize on amd64/arm64 (SSE2/NEON).

### Dispatch Table (v0.9.3)

| Opcode group | Before | After (v0.9.3) |
|--------------|-------:|---------------:|
| Opcodes in dispatch table | 10 | 22 |

v0.9.3 extends the dispatch table to 22 opcodes: comparison operators (Eq/Ne/Lt/Le/Gt/Ge)
and extended string ops (Trim/LTrim/RTrim/Replace/Instr) now bypass the large switch statement.

### v0.9.5 SQL Compatibility Features

| Feature | Description |
|---------|-------------|
| REINDEX | `REINDEX` / `REINDEX table` / `REINDEX index` — rebuild all or named indexes |
| SELECT INTO | `SELECT col1, col2 INTO newtable FROM src [WHERE ...]` — create table from query |
| Window Functions (verified) | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` with `OVER (PARTITION BY ... ORDER BY ...)` |
| CTE / WITH (verified) | Non-recursive and recursive `WITH ... AS (SELECT ...)` fully functional |
| UPSERT (verified) | `INSERT ... ON CONFLICT (col) DO NOTHING / DO UPDATE SET ...` |
| EXPLAIN QUERY PLAN (verified) | `EXPLAIN QUERY PLAN SELECT ...` shows index usage and scan strategy |
| Multi-VALUES INSERT (verified) | `INSERT INTO t VALUES (...), (...), (...)` batch literal insert |
| ANALYZE (verified) | `ANALYZE [table]` collects row-count statistics for the optimizer |
| VACUUM (verified) | `VACUUM` compact in-place; `VACUUM INTO 'path'` backup variant |
| AUTOINCREMENT (verified) | `INTEGER PRIMARY KEY AUTOINCREMENT` monotonically increasing IDs |
| LIKE ESCAPE (verified) | `expr LIKE pattern ESCAPE '\'` — custom escape character |

> **Analysis**: sqlvibe v0.9.5 adds REINDEX and SELECT INTO while verifying the full set of
> SQL compatibility features listed in the plan. Core query throughput is stable relative to
> v0.9.4 — SELECT all stays at 9.3× faster, SUM at 3.4× faster, GROUP BY at 3.6× faster,
> and result cache at 386× faster. SQLite retains its advantage for filtered WHERE scans and
> range queries (indexed lookup vs. full-table scan). The REINDEX command allows rebuilding
> stale or corrupted indexes without restarting the server.

### v0.9.4 SQL Compatibility Features

| Feature | Description |
|---------|-------------|
| Partial Index | `CREATE INDEX ... WHERE expr` — index filtered by a predicate |
| Expression Index | `CREATE INDEX ON t(LOWER(col))` — index on computed expression |
| RETURNING clause | `INSERT/UPDATE/DELETE ... RETURNING col1, col2, *` |
| UPDATE ... FROM | `UPDATE t SET ... FROM t2 WHERE ...` (PostgreSQL-style) |
| DELETE ... USING | `DELETE FROM t USING t2 WHERE ...` (PostgreSQL-style) |
| MATCH operator | `expr MATCH pattern` — case-insensitive substring matching |
| COLLATE support | `COLLATE NOCASE / RTRIM / BINARY` in column defs and expressions |
| CHECK (verified) | `CHECK(expr)` constraints enforced on INSERT and UPDATE |
| GLOB (verified) | `expr GLOB pattern` case-sensitive wildcard matching (`*`, `?`, `[...]`) |
| ALTER TABLE (verified) | `ADD COLUMN` and `RENAME TO` fully functional |

### Key Optimizations

- **Columnar storage**: Fast full table scans via vectorised SIMD-friendly layouts
- **Hybrid row/column**: Adaptive switching for best performance per workload
- **Result cache**: Near-zero latency for repeated identical queries (FNV-1a keyed, 381x vs SQLite)
- **Predicate pushdown**: WHERE/BETWEEN conditions evaluated before VM for fast filtered scans
- **Plan cache**: Skip tokenise/parse/codegen for cached query plans
- **Batch INSERT fast path**: Literal multi-row INSERT bypasses VM entirely
- **Fast Hash JOIN**: Integer/string join keys bypass `fmt.Sprintf` allocation (v0.9.0)
- **BETWEEN pushdown**: Range predicates pushed to Go layer before VM (v0.9.0)
- **Early termination for LIMIT**: VM halts after collecting N rows when no ORDER BY (v0.9.0)
- **AND index lookup**: Compound `WHERE col=val AND cond` uses secondary index (v0.9.0)
- **LIMIT-aware pre-allocation**: Flat result buffer capped at LIMIT rows to avoid over-allocation (v0.9.0)
- **Pre-sized result slices**: Column-name slices pre-allocated to reduce GC pressure (v0.9.0)
- **Covering Index**: `IndexMeta.CoversColumns` enables index-only scans with zero table lookup (v0.9.1)
- **Column Projection**: `ScanProjected`/`ScanProjectedWhere` materialise only required columns (v0.9.1)
- **Index Skip Scan**: `SkipScan` enables range scans on non-leading index columns (v0.9.1)
- **Slab Allocator**: Bump-pointer slab with `sync.Pool` for small objects reduces GC pressure (v0.9.1)
- **Prepared Statement Pool**: LRU-evicting `StatementPool` caches compiled plans for parameterized queries (v0.9.1)
- **Direct Threaded VM**: Dispatch table (22 opcodes: arith + comparison + string) reduces branch misprediction (v0.9.1–v0.9.3)
- **Expression Bytecode**: Compact `ExprBytecode` stack machine for single-call expression evaluation (v0.9.1)
- **Direct Compiler**: `DirectCompiler` with fast-path detection for simple SELECT patterns (v0.9.1)
- **bytes.Compare**: `[]byte` comparison uses stdlib `bytes.Compare` (v0.9.2)
- **SIMD Vectorization**: 4-way unrolled batch ops for int64/float64 (VectorSum/Add/Sub/Mul/Min/Max) (v0.9.3)
- **sync.Pool allocation reduction**: Pooled schema maps reduce per-query allocations
- **VM constant folding**: Arithmetic on compile-time constants folded at compile time
- **Strength reduction**: `x*1` → copy, `x*0` → 0, `x*2` → add (VM optimizer)

## SQL:1999 Compatibility

89+ test suites passing

## Building

```bash
go build ./...
go test ./...
go test ./internal/TS/Benchmark/... -bench . -benchmem
```

## Build Config Flags

Configure extensions at build time using `-tags`:

| Flag | Extensions | Description |
|------|------------|-------------|
| `SVDB_EXT_JSON` | JSON extension | SQLite JSON1 functions |
| `SVDB_EXT_MATH` | Math extension | ABS, CEIl, FLOOR, ROUND, POWER, SQRT, MOD, EXP, LN, LOG, etc. |

### Examples

```bash
# With JSON extension
go build -tags "SVDB_EXT_JSON" -o sqlvibe .

# With Math extension
go build -tags "SVDB_EXT_MATH" -o sqlvibe .

# With multiple extensions
go build -tags "SVDB_EXT_JSON SVDB_EXT_MATH" -o sqlvibe .
```

### Checking Extensions

```sql
-- Query loaded extensions
SELECT * FROM sqlvibe_extensions;
```

## License

See LICENSE file.

