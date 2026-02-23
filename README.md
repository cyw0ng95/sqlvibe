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

## Performance (v0.9.2)

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
| SELECT all (3 cols) | 60 µs | 576 µs | **sqlvibe 9.6x faster** |
| SELECT WHERE | 271 µs | 94 µs | SQLite 2.8x faster |
| ORDER BY (500 rows) | 190 µs | 301 µs | **sqlvibe 1.6x faster** |
| COUNT(*) | 6.3 µs | 5.3 µs | roughly equal |
| SUM | 18 µs | 68 µs | **sqlvibe 3.7x faster** |
| GROUP BY | 134 µs | 496 µs | **sqlvibe 3.7x faster** |
| JOIN (100×500 rows) | 564 µs | 240 µs | SQLite 2.3x faster |
| BETWEEN filter | 488 µs | 191 µs | SQLite 2.6x faster |

### DML Operations

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| INSERT single | 3.7 µs | 6.2 µs | **sqlvibe 1.7x faster** |
| INSERT 100 (batch) | 266 µs | 551 µs | **sqlvibe 2.1x faster** |

### Special-case Performance

| Operation | sqlvibe | SQLite Go | Result |
|-----------|--------:|----------:|--------|
| Result cache hit (repeated query) | 1.4 µs | 576 µs | **sqlvibe 397x faster** |
| LIMIT 10 no ORDER BY (10K rows) | 20 µs | 119 µs | **sqlvibe 6x faster** |
| LIMIT 100 no ORDER BY (10K rows) | 38 µs | 119 µs | **sqlvibe 3.1x faster** |
| Expression bytecode eval | 99 ns | — | single-dispatch expression evaluation |

### v0.9.2 Fixes & Improvements

| Fix / Improvement | Detail |
|-------------------|--------|
| Unknown function error | `no such function: <name>` returned instead of silent NULL (VM + QE paths) |
| JULIANDAY(NULL) → NULL | Explicit NULL input now returns NULL, matching SQLite semantics |
| ROUND returns float64 | `ROUND(x)` with 0 decimals returns `float64` (was `int64`), enabling `ROUND(julianday(...))` |
| Math in constant SELECT | ROUND, ABS, CEIL, FLOOR, SQRT, … work in `SELECT expr` without FROM |
| Dispatch table | OpUpper, OpLower, OpLength, OpConcat added to fast-path dispatch |
| bytes.Compare | `[]byte` comparison uses stdlib `bytes.Compare` |

> **Analysis**: sqlvibe's in-memory columnar engine excels at full-table scans and aggregates
> (SELECT *, SUM, GROUP BY) and delivers near-zero latency for repeated identical queries via its
> result cache (397x faster than SQLite for cache hits). v0.9.2 improves correctness: undefined
> functions now surface an error instead of silently returning NULL, JULIANDAY(NULL) correctly
> returns NULL, and ROUND returns float64 to match SQLite semantics. SQLite's B-Tree index
> implementation gives it an advantage for selective WHERE filters, range predicates, and JOIN on
> indexed keys. Overall, sqlvibe excels at analytical/BI workloads and repeated queries; SQLite is
> better for highly selective point lookups on indexed columns.

### Key Optimizations

- **Columnar storage**: Fast full table scans via vectorised SIMD-friendly layouts
- **Hybrid row/column**: Adaptive switching for best performance per workload
- **Result cache**: Near-zero latency for repeated identical queries (FNV-1a keyed, 397x vs SQLite)
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
- **Direct Threaded VM**: Dispatch table infrastructure for arithmetic and string opcodes reduces branch misprediction (v0.9.1/v0.9.2)
- **Expression Bytecode**: Compact `ExprBytecode` stack machine for single-call expression evaluation (v0.9.1)
- **Direct Compiler**: `DirectCompiler` with fast-path detection for simple SELECT patterns (v0.9.1)
- **bytes.Compare**: `[]byte` comparison uses stdlib `bytes.Compare` (v0.9.2)
- **sync.Pool allocation reduction**: Pooled schema maps reduce per-query allocations
- **VM constant folding**: Arithmetic on compile-time constants folded at compile time
- **Strength reduction**: `x*1` → copy, `x*0` → 0, `x*2` → add (VM optimizer)

## SQL:1999 Compatibility

87+ test suites passing

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

