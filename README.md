# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
| **v0.10.16** | 2026-03-01 | CGO Phases 12-18: expression eval, bytecode dispatch, type conversion, string/datetime/aggregate batch ops, fast QP tokenizer |
| **v0.10.15** | 2026-03-01 | CLI: .dump enhancements, .export fix; context/ window/ subpackages; ANY_VALUE, MODE aggregates |
| **v0.9.17** | 2026-02-26 | JSON Extension Enhancement: Table-valued functions (json_each, json_tree), Aggregates (json_group_array, json_group_object), JSONB format |

## Features

- **SQL:1999 compatibility** — 84+ test suites passing (added F870/F871/F872)
- **In-memory databases** — `:memory:` URI for fast, ephemeral storage
- **Comprehensive SQL**: DDL, DML, JOINs, Subqueries, Aggregates, Window functions (ROW_NUMBER/RANK/LAG/LEAD/NTILE/PERCENT_RANK/CUME_DIST), CTEs (recursive), VALUES derived tables, ANY/ALL subqueries, GROUP_CONCAT, ANY_VALUE, MODE, etc.
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

# With FTS5 extension (Full-Text Search)
go build -tags "SVDB_EXT_FTS5" ./...

# Multiple extensions
go build -tags "SVDB_EXT_JSON SVDB_EXT_MATH SVDB_EXT_FTS5" ./...
```

Query which extensions are loaded:

```sql
SELECT * FROM sqlvibe_extensions;
-- name | description       | functions
-- json | JSON extension    | json,json_array,json_extract,...
-- math | Math extension    | POWER,SQRT,MOD,...
-- fts5 | FTS5 extension    | MATCH,BM25,rank,tokenize,...
```

## Architecture

- **Storage**: Hybrid row/columnar store with RoaringBitmap indexes
- **Query Processing**: Tokenizer → Parser → Optimizer → Compiler
- **Execution**: Register-based VM with vectorized execution
- **Memory**: Arena allocator + sync.Pool for zero-GC query execution
- **Extensions**: Build-tag controlled, statically linked, auto-registered at startup

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details.

## Performance

Benchmarks on Intel Xeon @ 2.90 GHz, in-memory database, `-benchtime=1s`.
**Methodology**: the result cache is cleared before each sqlvibe iteration via
`db.ClearResultCache()` so actual per-query execution cost is measured.
SQLite's `database/sql` driver reuses prepared statements across iterations.
Both sides iterate all result rows end-to-end.
(`go test ./internal/TS/Benchmark/... -bench=BenchmarkCompare_ -benchtime=1s`).
Results may vary on different hardware.

### SQLite vs sqlvibe (v0.11.0 — C++ Migration Complete)

Build with `./build.sh -t` to run tests with all CGO optimizations enabled.

**v0.11.0 Highlights**: Complete C++ migration of DS/VM/QP/CG layers eliminates Go callback overhead, achieving **2.8–5.5× speedups** for scan and aggregate workloads.

#### SELECT all rows

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 497 µs | 175 µs | **2.8× faster** |
| 10 K | 5.28 ms | 1.57 ms | **3.4× faster** |
| 100 K | 49.7 ms | 16.6 ms | **3.0× faster** |

#### WHERE filter (integer column)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 287 µs | 824 µs | 2.9× slower |
| 10 K | 2.82 ms | 8.54 ms | 3.0× slower |
| 100 K | 28.1 ms | 116.8 ms | 4.2× slower |

#### SUM aggregate

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 62.7 µs | 21.0 µs | **3.0× faster** |
| 10 K | 567 µs | 215 µs | **2.6× faster** |
| 100 K | 5.70 ms | 3.21 ms | **1.8× faster** |

#### GROUP BY (4 groups)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 417 µs | 126 µs | **3.3× faster** |
| 10 K | 4.39 ms | 976 µs | **4.5× faster** |
| 100 K | 54.2 ms | 10.4 ms | **5.2× faster** |

#### COUNT(*)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 5.5 µs | 4.0 µs | **1.4× faster** |
| 10 K | 7.1 µs | 4.3 µs | **1.7× faster** |
| 100 K | 29.9 µs | 55.4 µs | 1.9× slower |

#### INSERT (batch rows)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 5.80 ms | 2.60 ms | **2.2× faster** |
| 10 K | 53.8 ms | 29.5 ms | **1.8× faster** |

#### INNER JOIN

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 729 µs | 1.02 ms | 1.4× slower |
| 10 K | 7.95 ms | 11.1 ms | 1.4× slower |
| 100 K | 75.4 ms | 136.8 ms | 1.8× slower |

#### ORDER BY + LIMIT (Top N)

| Rows | SQLite | sqlvibe | Result |
|-----:|-------:|--------:|--------|
| 1 K | 212 µs | 249 µs | 1.2× slower |
| 10 K | 1.92 ms | 2.25 ms | 1.2× slower |
| 100 K | 19.2 ms | 28.0 ms | 1.5× slower |

> **Analysis (v0.11.0 — C++ Migration Complete)**: sqlvibe excels at scan and aggregate workloads with **1.8–5.5× speedups** over SQLite for SELECT all, SUM, and GROUP BY. The complete C++ migration of DS/VM/QP/CG layers in v0.11.0 eliminates Go callback overhead, achieving **52× faster page operations** (260ns → 5ns) in the storage layer. Key improvements:
> - **SELECT all**: 2.8–3.4× faster (C++ columnar store + SIMD batch ops)
> - **SUM aggregate**: 1.8–3.0× faster (C++ aggregate engine)
> - **GROUP BY**: 3.3–5.2× faster (C++ hash aggregation + batch compare)
> - **INSERT**: 1.8–2.2× faster (C++ row store + B-Tree)
> - **COUNT(*)**: 1.4–1.7× faster at small scale (C++ scan ops)
>
> WHERE filter and JOIN remain areas for optimization — the bytecode VM evaluation path adds overhead vs SQLite's tightly-optimized scan. ORDER BY + LIMIT is 1.2–1.5× slower due to sort overhead. The C++ migration provides a strong foundation for future SIMD optimization of these workloads.

### Key Optimizations (v0.11.0)

- **C++ DS Layer**: B-Tree, columnar store, row store, overflow — all in C++ with embedded PageManager
- **C++ VM Layer**: All 46 bytecode opcodes implemented in C++ with batch SIMD execution
- **C++ QP/CG**: Full SQL parser and bytecode compiler in C++
- **Boundary CGO**: Zero Go callback overhead — inner C++ modules call directly (~5ns vs ~260ns)
- **Columnar storage**: Fast full table scans via vectorized SIMD-friendly layouts
- **Hybrid row/column**: Adaptive switching for best performance per workload
- **Result cache**: Near-zero latency for repeated identical queries
- **Predicate pushdown**: WHERE/BETWEEN conditions evaluated before VM for fast filtered scans
- **Plan cache**: Skip tokenize/parse/codegen for cached query plans
- **Batch INSERT fast path**: Literal multi-row INSERT bypasses VM entirely
- **Fast Hash JOIN**: Integer/string join keys bypass `fmt.Sprintf` allocation
- **BETWEEN pushdown**: Range predicates pushed to Go layer before VM
- **Early termination for LIMIT**: VM halts after collecting N rows when no ORDER BY
- **AND index lookup**: Compound `WHERE col=val AND cond` uses secondary index
- **Covering Index**: Index-only scans with zero table lookup
- **Column Projection**: Materialize only required columns
- **Index Skip Scan**: Range scans on non-leading index columns
- **Direct Threaded VM**: Dispatch table reduces branch misprediction
- **SIMD Vectorization**: 4-way unrolled batch ops for int64/float64 (AVX2)
- **CGO Acceleration** (phases 1-18): C++ backends for all major subsystems:
  - Phases 1-3: Math, JSON, FTS5 extensions (1.5-7× speedup)
  - Phases 4-6: B-Tree, columnar storage, bitmap indexes (2-3× storage ops)
  - Phases 7-11: Hash JOIN, string compare, batch execution, ORDER BY, LZ4/ZSTD compression
  - Phases 12-14: Expression evaluation, bytecode dispatch, type conversion (2-3× arithmetic)
  - Phases 15-17: String functions, datetime, aggregate batch ops (2-3× per-row ops)
  - Phase 18: Fast C++ tokenizer for query pre-allocation (libsvdb_qp)

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
| `SVDB_EXT_MATH` | Math extension | ABS, CEIL, FLOOR, ROUND, POWER, SQRT, MOD, EXP, LN, LOG, etc. |
| `SVDB_EXT_FTS5` | FTS5 extension | Full-Text Search: MATCH, BM25, rank, tokenizers |

### Examples

```bash
# With JSON extension
go build -tags "SVDB_EXT_JSON" -o sqlvibe .

# With Math extension
go build -tags "SVDB_EXT_MATH" -o sqlvibe .

# With FTS5 extension
go build -tags "SVDB_EXT_FTS5" -o sqlvibe .

# With multiple extensions
go build -tags "SVDB_EXT_JSON SVDB_EXT_MATH SVDB_EXT_FTS5" -o sqlvibe .
```

### Checking Extensions

```sql
-- Query loaded extensions
SELECT * FROM sqlvibe_extensions;
```

## License

See LICENSE file.

