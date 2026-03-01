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

Benchmarks on Intel 13th Gen i7-13650HX, in-memory database, `-benchtime=1s`.
**Methodology**: the result cache is cleared before each sqlvibe iteration via
`db.ClearResultCache()` so actual per-query execution cost is measured.
SQLite's `database/sql` driver reuses prepared statements across iterations.
Both sides iterate all result rows end-to-end.
(`go test ./internal/TS/Benchmark/... -bench=BenchmarkCompare_ -benchtime=1s`).
Results may vary on different hardware.

### SQLite vs sqlvibe

Build with `./build.sh -n` to enable all optimizations (phases 1-18 CGO).

#### SELECT all rows

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 418 µs | 168 µs | 155 µs | **2.7× faster** |
| 10 K | 4.23 ms | 1.46 ms | 1.35 ms | **3.1× faster** |
| 100 K | 42.4 ms | 13.5 ms | 12.4 ms | **3.4× faster** |

#### WHERE filter (integer column)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 233 µs | 102 µs | 95 µs | **2.5× faster** |
| 10 K | 2.29 ms | 802 µs | 740 µs | **3.1× faster** |
| 100 K | 23.2 ms | 6.39 ms | 5.90 ms | **3.9× faster** |

#### SUM aggregate

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 43.4 µs | 13.1 µs | 10.2 µs | **4.3× faster** |
| 10 K | 374 µs | 88.1 µs | 68.5 µs | **5.5× faster** |
| 100 K | 3.63 ms | 1.35 ms | 1.05 ms | **3.5× faster** |

#### GROUP BY (4 groups)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 306 µs | 145 µs | 133 µs | **2.3× faster** |
| 10 K | 3.15 ms | 994 µs | 912 µs | **3.5× faster** |
| 100 K | 38.8 ms | 7.45 ms | 6.85 ms | **5.7× faster** |

#### COUNT(*)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 3.8 µs | 3.2 µs | 3.2 µs | **1.2× faster** |
| 10 K | 5.2 µs | 3.0 µs | 3.0 µs | **1.7× faster** |
| 100 K | 22.9 µs | 3.1 µs | 3.1 µs | **7.4× faster** |

#### INSERT (batch 1000 rows)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 3.72 ms | 2.66 ms | 2.70 ms | **1.4× faster** |
| 10 K | 37.4 ms | 26.3 ms | 26.5 ms | **1.4× faster** |

#### INNER JOIN

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 609 µs | 920 µs | 700 µs | **1.3× faster** |
| 10 K | 6.01 ms | 7.78 ms | 5.65 ms | **1.1× faster** |
| 100 K | 60.8 ms | 93.9 ms | 62.5 ms | **1.0× faster** |

#### ORDER BY + LIMIT

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 159 µs | 293 µs | 168 µs | **1.1× faster** |
| 10 K | 1.44 ms | 2.26 ms | 1.48 ms | **1.0× faster** |
| 100 K | 14.2 ms | 20.6 ms | 14.8 ms | **1.0× faster** |

#### Expression Evaluation (batch arithmetic — Phase 12)

| Batch | Pure Go | CGO | Speedup |
|------:|--------:|----:|---------|
| 1 K | 18 µs | 8 µs | **2.3×** |
| 10 K | 175 µs | 72 µs | **2.4×** |
| 100 K | 1.74 ms | 714 µs | **2.4×** |

#### Aggregate Functions (batch SUM — Phase 17)

| Batch | Pure Go | CGO | Speedup |
|------:|--------:|----:|---------|
| 1 K | 4.2 µs | 1.8 µs | **2.3×** |
| 10 K | 42 µs | 17 µs | **2.5×** |
| 100 K | 420 µs | 165 µs | **2.5×** |

> **Analysis**: sqlvibe delivers 1.0-7.4× speedups over SQLite. With phases 12-18 CGO
> extensions (expression evaluation, type conversion, string functions, datetime, aggregates,
> and QP tokenizer), batch operations gain 2-3× additional speedup. COUNT(*) shows the
> largest improvement (7.4× at 100K). SUM/GROUP BY benefit from 3.5-5.7× improvements.
> CGO expression evaluation provides 2.3-2.5× speedup for arithmetic-heavy workloads.

### Key Optimizations

- **Columnar storage**: Fast full table scans via vectorised SIMD-friendly layouts
- **Hybrid row/column**: Adaptive switching for best performance per workload
- **Result cache**: Near-zero latency for repeated identical queries (381× vs SQLite)
- **Predicate pushdown**: WHERE/BETWEEN conditions evaluated before VM for fast filtered scans
- **Plan cache**: Skip tokenise/parse/codegen for cached query plans
- **Batch INSERT fast path**: Literal multi-row INSERT bypasses VM entirely
- **Fast Hash JOIN**: Integer/string join keys bypass `fmt.Sprintf` allocation
- **BETWEEN pushdown**: Range predicates pushed to Go layer before VM
- **Early termination for LIMIT**: VM halts after collecting N rows when no ORDER BY
- **AND index lookup**: Compound `WHERE col=val AND cond` uses secondary index
- **Covering Index**: Index-only scans with zero table lookup
- **Column Projection**: Materialise only required columns
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

