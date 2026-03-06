# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
| **v0.11.5** | 2026-03-06 | C++ module structure optimization: SC/ (System Composer), unified libsvdb build, removed redundant CMakeLists.txt |
| **v0.11.4** | 2026-03-05 | C++ module consolidation: All core code under src/core/[SubSystem] |
| **v0.11.3** | 2026-03-04 | Foundation classes v2: ArenaV2, CacheV2, PageManagerV2, C++ owned memory |
| **v0.11.2** | 2026-03-03 | C++ Native Engine: Unified C public API (svdb.h), thin CGO binding (~400 LOC) |
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

Benchmarks on 13th Gen Intel(R) Core(TM) i7-13650HX, in-memory database, `-benchtime=300ms`.
**Methodology**: the result cache is cleared before each sqlvibe iteration via
`db.ClearResultCache()` so actual per-query execution cost is measured.
SQLite's `database/sql` driver reuses prepared statements across iterations.
Both sides iterate all result rows end-to-end.
(`go test ./tests/Benchmark/... -bench=BenchmarkCompare_ -benchtime=300ms`).
Results may vary on different hardware.

### SQLite vs sqlvibe (v0.11.5 — Module Structure + Performance Optimization)

Build with `./build.sh -t` to run tests with all CGO optimizations enabled.

**v0.11.5 Highlights**: Optimized C++ module structure with `SC/` (System Composer) subsystem,
unified `libsvdb.so` build, and identified optimization opportunities for memory management,
SIMD expansion, and batch query execution.

**Test Hardware**: 13th Gen Intel(R) Core(TM) i7-13650HX (20 cores), Linux, Go 1.21+

#### 1K Rows Comparison

| Workload | SQLite | sqlvibe | Result |
|----------|--------|---------|--------|
| SELECT all | 428 µs | 4.23 ms | 9.9× slower |
| SUM aggregate | 45 µs | 2.02 ms | 44.9× slower |
| GROUP BY (4 groups) | 317 µs | 4.81 ms | 15.2× slower |
| COUNT(*) | 4.0 µs | 487 µs | 121.8× slower |
| INSERT batch | 3.64 ms | 2.47 ms | **1.5× faster** |

> **Analysis (v0.11.5 — Current Generation Hardware)**: On modern Intel CPUs, SQLite's
> highly-optimized C implementation demonstrates significant performance advantages for
> read-heavy workloads. sqlvibe shows competitive performance only for batch INSERT operations.
>
> **Key Observations**:
> - **INSERT batch**: 1.5× faster (C++ direct insert fast path bypasses VM)
> - **SELECT all**: 9.9× slower (bytecode VM dispatch overhead vs native C)
> - **Aggregates (SUM, GROUP BY)**: 15-45× slower (VM execution vs SQLite's optimized B-tree scans)
> - **COUNT(*)**: 122× slower (SQLite's index-only scan vs full VM iteration)
>
> **v0.11.5 Architecture Improvements**:
> - **SC/ (System Composer)**: Unified C public API, invoke chain, orchestration
> - **Simplified build**: Single `libsvdb.so` from `src/CMakeLists.txt`
> - **Reduced overhead**: 6 subsystem CMakeLists.txt files eliminated
> - **Cleaner structure**: All core subsystems (CG, DS, IS, PB, QP, SC, SF, TM, VM) source-only
>
> **Identified Optimization Opportunities** (see `docs/plan-v0.11.5.md`):
> - **P0 Memory Management**: Arena allocator integration (40% GC reduction target)
> - **P0 Batch Query Engine**: Vectorized execution (5-10x analytical target)
> - **P1 SIMD Expansion**: Batch compare/aggregate (4x speedup target)
> - **P1 Bytecode Optimizer**: Predicate pushdown, loop-invariant code motion
> - **P2 LTO/PGO**: Link-time and profile-guided optimization (10-15% target)

### Historical Benchmarks (v0.11.2 — AMD EPYC 7763)

**Note**: Preserved for reference. Results from AMD EPYC 7763 64-Core @ 2.45 GHz with v0.11.2.
These results demonstrate sqlvibe's potential with optimized aggregate workloads on server hardware.

| Workload | SQLite | sqlvibe | Result |
|----------|--------|---------|--------|
| SUM aggregate (100K) | 6.08 ms | 2.33 ms | **2.6× faster** |
| GROUP BY (100K) | 57.7 ms | 11.7 ms | **4.9× faster** |
| SELECT all (100K) | 28.6 ms | 27.4 ms | **1.0× faster** |
| INSERT batch (10K) | 56.1 ms | 32.3 ms | **1.7× faster** |
| WHERE filter (10K) | 1.81 ms | 8.39 ms | 4.6× slower |
| INNER JOIN (10K) | 4.60 ms | 11.5 ms | 2.5× slower |

> **Historical Context**: The v0.11.2 AMD EPYC benchmarks showed sqlvibe excelling at aggregate
> workloads (2.6–4.9× faster for SUM and GROUP BY). The difference in results between AMD EPYC
> and Intel i7 highlights the sensitivity of database performance to:
> - CPU architecture (server vs consumer)
> - Memory bandwidth and cache hierarchy
> - Go runtime GC behavior on different platforms
> - CGO call overhead variations

### Key Optimizations (v0.11.5)

- **Module Structure**: `SC/` (System Composer) subsystem for C API and orchestration
- **Unified Build**: Single `libsvdb.so` from `src/CMakeLists.txt` — no redundant subsystem libraries
- **Clean Architecture**: All core code under `src/core/[SubSystem]` (CG, DS, IS, PB, QP, SC, SF, TM, VM)
- **C++ Native Engine**: `src/core/svdb/` → `SC/` with unified C public API (`svdb.h`)
- **Phase 11 C smoke test**: `build.sh` verifies the C API via `svdb_open`/`svdb_exec`/`svdb_query`
- **Thin CGO binding**: `pkg/sqlvibe/cgo/` package (~600 LOC) — pure type-mapping, no business logic
- **C++ Bytecode Optimizer**: Dead-code elimination + peephole passes via `svdb_cg_optimize_bc_instrs`
- **C++ Query Engine Module**: 14 engine operations (FilterRows, InnerJoin, LeftOuterJoin, GroupRows, SortRows, ExistsRows, etc.)
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
- **SIMD Vectorization**: 4-way unrolled batch ops for int64/float64 (AVX2)

### Identified Optimization Opportunities (v0.11.5 → v0.11.6)

See `docs/plan-v0.11.5.md` for detailed optimization plans:

- **P0 Memory Management**: Arena allocator integration — 40% GC reduction target
- **P0 Batch Query Engine**: Vectorized execution — 5-10x analytical speedup target
- **P1 Tiered Cache**: Hot/cold separation — 20-30% hit rate improvement target
- **P1 SIMD Expansion**: Batch compare/aggregate — 4x speedup target
- **P1 Bytecode Optimizer**: Predicate pushdown, loop-invariant code motion — 2-3x complex queries
- **P2 Index Statistics**: Cost-based optimization — better query plans
- **P2 Parser string_view**: Zero-copy parsing — 30% faster parse
- **P2 LTO/PGO**: Link-time and profile-guided optimization — 10-15% overall

## SQL:1999 Compatibility

89+ test suites passing

## Building

```bash
go build ./...
go test ./...
go test ./tests/Benchmark/... -bench . -benchmem
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

