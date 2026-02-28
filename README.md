# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
| **v0.10.3** | 2026-03-01 | Advanced SQL: WINDOW clause, window frames, JSON functions (JSON_KEYS, JSON_VALID, JSON_ARRAY_LENGTH) |
| **v0.10.2** | 2026-03-01 | FTS5 Full-Text Search Extension: inverted index, BM25 ranking, MATCH queries, tokenizers (ASCII/Porter/Unicode61) |
| **v0.10.1** | 2026-02-28 | Coverage Improvement: +25% test coverage in critical packages (QP, CG, sqlvibe) |
| **v0.9.17** | 2026-02-26 | JSON Extension Enhancement: Table-valued functions (json_each, json_tree), Aggregates (json_group_array, json_group_object), JSONB format |

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

// Parameterized queries (safe against SQL injection)
db.ExecWithParams(`INSERT INTO users VALUES (?, ?)`, []interface{}{int64(3), "Carol"})
rows, _ = db.QueryWithParams(`SELECT name FROM users WHERE id = ?`, []interface{}{int64(3)})

// Named parameters
db.ExecNamed(`INSERT INTO users VALUES (:id, :name)`, map[string]interface{}{"id": int64(4), "name": "Dave"})
rows, _ = db.QueryNamed(`SELECT * FROM users WHERE name = :name`, map[string]interface{}{"name": "Dave"})

// Prepared statements with parameter binding
stmt, _ := db.Prepare(`SELECT name FROM users WHERE id = ?`)
defer stmt.Close()
rows, _ = stmt.Query(int64(1)) // binds ? = 1
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

## Performance (v0.10.0)

Benchmarks on AMD EPYC 7763 (CI environment), in-memory database, `-benchtime=2s`.
**Methodology**: the result cache is cleared before each sqlvibe iteration via
`db.ClearResultCache()` so actual per-query execution cost is measured.
SQLite's `database/sql` driver reuses prepared statements across iterations.
Both sides iterate all result rows end-to-end.
(`go test ./internal/TS/Benchmark/... -bench=BenchmarkCompare_ -benchtime=2s`).
Results may vary on different hardware.

### v0.10.0 Bytecode Engine: SQLite vs sqlvibe across data scales

sqlvibe v0.10.0 ships a register-based **bytecode execution engine** that is always on.
The old AST-walking path has been removed; the bytecode VM is the sole execution path,
with a transparent fallback to the register VM for SQL constructs the bytecode compiler
does not yet cover (e.g. multi-table JOINs with ORDER BY).

#### SELECT all rows

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 292 µs | 182 µs | **sqlvibe 1.6× faster** |
| 10 K | 2.87 ms | 1.62 ms | **sqlvibe 1.8× faster** |
| 100 K | 28.8 ms | 20.4 ms | **sqlvibe 1.4× faster** |

#### WHERE filter (integer column, ~50% selectivity)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 188 µs | 104 µs | **sqlvibe 1.8× faster** |
| 10 K | 1.79 ms | 990 µs | **sqlvibe 1.8× faster** |
| 100 K | 18.1 ms | 8.71 ms | **sqlvibe 2.1× faster** |

#### SUM aggregate

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 66.7 µs | 20.7 µs | **sqlvibe 3.2× faster** |
| 10 K | 600 µs | 113 µs | **sqlvibe 5.3× faster** |
| 100 K | 6.11 ms | 1.41 ms | **sqlvibe 4.3× faster** |

#### GROUP BY (4 groups, SUM + COUNT)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 480 µs | 128 µs | **sqlvibe 3.8× faster** |
| 10 K | 4.93 ms | 1.03 ms | **sqlvibe 4.8× faster** |
| 100 K | 57.9 ms | 11.9 ms | **sqlvibe 4.9× faster** |

#### COUNT(*)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 5.4 µs | 8.1 µs | SQLite 1.5× faster |
| 10 K | 7.0 µs | 7.9 µs | roughly equal |
| 100 K | 25.2 µs | 7.8 µs | **sqlvibe 3.2× faster** |

> **v0.10.0 analysis**: The bytecode engine delivers consistent 1.4–5.3× speedups over
> SQLite for scan-heavy and aggregate workloads. GROUP BY and SUM show the largest gains
> (up to 5×) because the bytecode VM eliminates interface{} boxing on hot aggregate loops.
> COUNT(*) on small tables is slightly slower than SQLite's prepared-statement fast path
> but surpasses it at 100 K rows. ORDER BY + LIMIT (Top-N) still falls back to the
> register VM and is currently slower than SQLite; this will be addressed in v0.11.0.

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

