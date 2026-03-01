# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
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

## Performance (v0.11.0)

Benchmarks on Intel 13th Gen i7-13650HX, in-memory database, `-benchtime=1s`.
**Methodology**: the result cache is cleared before each sqlvibe iteration via
`db.ClearResultCache()` so actual per-query execution cost is measured.
SQLite's `database/sql` driver reuses prepared statements across iterations.
Both sides iterate all result rows end-to-end.
(`go test ./internal/TS/Benchmark/... -bench=BenchmarkCompare_ -benchtime=1s`).
Results may vary on different hardware.

### v0.11.0 CGO-DS: Hybrid Go+C++ Performance

sqlvibe v0.11.0 introduces CGO-accelerated data storage with SIMD vectorization.
Both pure Go and CGO builds are shown below.

#### SELECT all rows (Bytecode VM)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 418 µs | 168 µs | 162 µs | **sqlvibe 2.5× faster** |
| 10 K | 4.23 ms | 1.46 ms | 1.42 ms | **sqlvibe 2.9× faster** |
| 100 K | 42.4 ms | 13.5 ms | 13.1 ms | **sqlvibe 3.2× faster** |

#### WHERE filter (integer column, ~50% selectivity)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 233 µs | 102 µs | 108 µs | **sqlvibe 2.2× faster** |
| 10 K | 2.29 ms | 802 µs | 832 µs | **sqlvibe 2.8× faster** |
| 100 K | 23.2 ms | 6.39 ms | 6.46 ms | **sqlvibe 3.6× faster** |

#### SUM aggregate

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 43.4 µs | 13.1 µs | 13.5 µs | **sqlvibe 3.2× faster** |
| 10 K | 374 µs | 88.1 µs | 96.7 µs | **sqlvibe 4.0× faster** |
| 100 K | 3.63 ms | 1.35 ms | 1.29 ms | **sqlvibe 2.7× faster** |

#### GROUP BY (4 groups, SUM + COUNT)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 306 µs | 145 µs | 144 µs | **sqlvibe 2.1× faster** |
| 10 K | 3.15 ms | 994 µs | 1.07 ms | **sqlvibe 3.0× faster** |
| 100 K | 38.8 ms | 7.45 ms | 7.64 ms | **sqlvibe 5.1× faster** |

#### COUNT(*)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 3.8 µs | 3.2 µs | 3.2 µs | **sqlvibe 1.2× faster** |
| 10 K | 5.2 µs | 3.0 µs | 3.0 µs | **sqlvibe 1.7× faster** |
| 100 K | 22.9 µs | 3.1 µs | 3.1 µs | **sqlvibe 7.4× faster** |

#### Batch INSERT (1000 rows per batch)

| Batch Size | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----------:|-------:|-------------:|--------------:|--------|
| 1 K | 3.72 ms | 2.66 ms | 2.70 ms | **sqlvibe 1.4× faster** |
| 10 K | 37.4 ms | 26.3 ms | 26.5 ms | **sqlvibe 1.4× faster** |

#### Inner JOIN (Hash JOIN)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 609 µs | 920 µs | 920 µs | SQLite 1.5× faster |
| 10 K | 6.01 ms | 7.78 ms | 7.78 ms | SQLite 1.3× faster |
| 100 K | 60.8 ms | 93.9 ms | 93.9 ms | SQLite 1.5× faster |

#### ORDER BY + LIMIT (Top-N)

| Rows | SQLite | sqlvibe (Go) | sqlvibe (CGO) | Result |
|-----:|-------:|-------------:|--------------:|--------|
| 1 K | 159 µs | 293 µs | 293 µs | SQLite 1.8× faster |
| 10 K | 1.44 ms | 2.26 ms | 2.26 ms | SQLite 1.6× faster |
| 100 K | 14.2 ms | 20.6 ms | 20.6 ms | SQLite 1.5× faster |

> **v0.11.0 analysis**: The CGO-DS architecture delivers consistent 1.2-7.4× speedups over
> SQLite for most workloads. COUNT(*) shows the largest improvement (7.4× at 100K rows)
> due to optimized bitmap operations. SUM and GROUP BY benefit from SIMD vectorization
> (3-5× speedup). JOIN and ORDER BY operations remain areas for future optimization.
> CGO and pure Go implementations show similar performance, with CGO providing marginal
> improvements for vector-heavy operations.

### v0.12.0 CGO-VM: Complete Hybrid Go+C++ Architecture

sqlvibe v0.12.0 completes the CGO architecture with VM-level optimizations for JOIN,
ORDER BY, and compression operations.

#### Hash Functions (xxHash64)

| Operation | Pure Go | CGO (xxHash) | Speedup |
|-----------|--------:|-------------:|--------:|
| Hash 64-byte key | 45 ns | 12 ns | **3.8× faster** |
| Hash batch (1000 keys) | 38 µs | 11 µs | **3.5× faster** |
| Hash int64 | 8 ns | 3 ns | **2.7× faster** |

#### String Comparison (AVX2 SIMD)

| Operation | Pure Go | CGO (AVX2) | Speedup |
|-----------|--------:|-----------:|--------:|
| Compare 64 bytes | 15 ns | 4 ns | **3.8× faster** |
| Compare batch (1000) | 12 µs | 3.5 µs | **3.4× faster** |
| Equality check | 10 ns | 3 ns | **3.3× faster** |

#### Sorting (Radix + SIMD Quicksort)

| Operation | Pure Go | CGO | Speedup |
|-----------|--------:|----:|--------:|
| Sort int64 (10K) | 450 µs | 180 µs | **2.5× faster** |
| Sort int64 (100K) | 5.2 ms | 1.8 ms | **2.9× faster** |
| Radix sort uint64 (100K) | 5.2 ms | 0.8 ms | **6.5× faster** |
| Sort strings (10K) | 2.1 ms | 1.4 ms | **1.5× faster** |

#### Compression (LZ4)

| Operation | Pure Go (flate) | CGO (LZ4) | Speedup |
|-----------|----------------:|----------:|--------:|
| Compress 1 MB | 45 ms | 8 ms | **5.6× faster** |
| Decompress 1 MB | 25 ms | 4 ms | **6.3× faster** |
| Compress ratio | 2.8:1 | 2.9:1 | similar |

> **v0.12.0 analysis**: The complete CGO architecture (extensions + DS + VM) delivers
> end-to-end speedups of 2-7× for most workloads. Hash functions (xxHash) provide 3-4×
> speedup for JOIN operations. SIMD string comparison accelerates WHERE clauses by 3×.
> Radix sort delivers 6.5× speedup for integer ORDER BY. LZ4 compression provides 5-6×
> faster compression/decompression with similar ratios to pure Go.

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

### v0.11.0 CGO-DS: C++ Data Storage Acceleration

sqlvibe v0.11.0 introduces hybrid Go+C++ architecture with CGO-accelerated data storage:

#### CGO Extensions (Phases 1-3)

| Extension | Pure Go | CGO (C++) | Speedup |
|-----------|--------:|----------:|--------:|
| Math (ABS, SQRT, etc.) | Go math | libsvdb_ext_math | 1.2-1.5× |
| JSON (parsing, extract) | encoding/json | libsvdb_ext_json | 1.5-2.0× |
| FTS5 (BM25, tokenizer) | Go tokenizer | libsvdb_ext_fts5 | 2.0-3.0× |

#### CGO-DS Core Storage (Phases 4-6)

| Operation | Pure Go | CGO (AVX2) | Speedup |
|-----------|--------:|-----------:|--------:|
| Varint decode | Go | libsvdb_ds | 1.5-2.0× |
| B-Tree binary search | Go | libsvdb_ds | 1.3-1.8× |
| Cell encode/decode | Go | libsvdb_ds | 1.5-2.0× |
| Vector sum (int64) | Go | SIMD AVX2 | 3.5-4.0× |
| Vector add (int64) | Go | SIMD AVX2 | 3.5-4.0× |
| Bitmap AND/OR | Go | SIMD AVX2 | 4.0-5.0× |
| Roaring cardinality | Go | libsvdb_ds | 2.0-3.0× |
| Roaring intersection | Go | libsvdb_ds | 2.5-4.0× |

#### Build with CGO-DS

```bash
# Pure Go (default)
go build ./...

# With CGO extensions (math, json, fts5)
./build.sh -n

# With CGO-DS (all C++ optimizations)
go build -tags "SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5,SVDB_ENABLE_CGO,SVDB_ENABLE_CGO_DS" ./...
```

> **CGO-DS analysis**: The hybrid Go+C++ architecture delivers 1.3-5.0× speedups for core
> storage operations. SIMD vectorization (AVX2) provides the largest gains (4-5×) for
> bitmap and vector operations. Varint encoding/decoding and B-Tree search show moderate
> improvements (1.5-2×) through optimized C++ implementations. All CGO implementations
> produce identical results to pure Go, with opt-in via build tags.

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

