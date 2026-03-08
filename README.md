# sqlvibe

**sqlvibe** is a high-performance SQL:1999-compatible database engine with a C++ core.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
| **v0.11.4** | 2026-03-05 | C++ module consolidation: All core code under src/core/[SubSystem] |
| **v0.10.16** | 2026-03-01 | CGO Phases 12-18: expression eval, bytecode dispatch, type conversion, string/datetime/aggregate batch ops, fast QP tokenizer |
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

## Architecture

- **C++ Core**: All core subsystems (DS, VM, QP, CG, TM, PB, IS, SC) written in C++
- **Go API**: Thin CGO wrapper (~600 LOC) for Go public API
- **Storage**: Hybrid row/columnar store with RoaringBitmap indexes
- **Query Processing**: Tokenizer → Parser → Optimizer → Compiler
- **Execution**: Register-based VM with vectorized execution
- **Memory**: Arena allocator + object pools for high-performance query execution
- **Extensions**: Build-tag controlled, statically linked, auto-registered at startup

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details.

## Performance

**Test Hardware**: 13th Gen Intel(R) Core(TM) i7-13650HX (8 cores), Linux
**Benchmarks**: C++ Google Benchmark, 1000 rows per test, Release build
**Methodology**: Each benchmark creates fresh database, executes SQL, iterates all result rows.
Results may vary on different hardware.

### SQLite vs sqlvibe (v0.11.5 — C++ Benchmark)

Build with `./build.sh -b` to run C++ benchmarks with Release build.

**v0.11.5**: Pure C++ benchmark using Google Benchmark framework with svdb.h C API
direct comparison against SQLite3 C API.

#### Benchmark Results (1K rows, 1000 iterations)

| Workload | SQLite (ns/op) | sqlvibe (ns/op) | Ratio | Status |
|----------|----------------|-----------------|-------|--------|
| **INSERT single** | 18,497 | 511,351 | 27.6× | 🔴 slower |
| **INSERT batch** | 447,024 | 530,251 | 1.2× | 🔴 slower |
| **SELECT all** | 726 | 672,782 | 926× | 🔴 slower |
| **SELECT WHERE** | 603 | 800,304 | 1,327× | 🔴 slower |
| **SELECT ORDER BY** | 1,001 | 723,374 | 722× | 🔴 slower |
| **SELECT aggregate** | 645 | 828,865 | 1,285× | 🔴 slower |
| **SELECT JOIN** | 12,392 | 31,782,000 | 2,565× | 🔴 slower |
| **SELECT subquery** | 1,589 | 5,353,550,000 | 3,369,636× | 🔴 slower |
| **UPDATE** | 580 | 1,136,800 | 1,959× | 🔴 slower |
| **DELETE** | 525 | 808,853 | 1,541× | 🔴 slower |

#### Scalability Analysis

| Workload | sqlite (ms/1K) | sqlvibe (ms/1K) | Gap |
|----------|----------------|-----------------|-----|
| SELECT all | 0.73 | 673 | 922× |
| Insert batch | 447 | 530 | 1.2× |
| Join | 12 | 31,782 | 2,565× |

> **Key Insights**:
> - **INSERT batch**: Closest performance (1.2× slower) — C++ direct insert path
> - **SELECT**: Full table scan overhead significant — needs optimization
> - **JOIN**: Major bottleneck (2,565×) — hash join implementation needs work
> - **Subquery**: Critical issue (3.3M×) — correlated subquery handling needs fix

### Root Cause Analysis

| Bottleneck | Component | Root Cause | Solution |
|------------|-----------|------------|----------|
| VM dispatch | VM/exec.cpp | Per-instruction function call overhead | Batch execution, inline dispatch |
| Hash join | VM/hash_join.cpp | Naive hash table, no SIMD | Hash table tuning, SIMD probes |
| COUNT(*) | DS/btree.cpp | Full table scan | Index-only scan, metadata cache |
| Memory alloc | Multiple | malloc/new per-row | Arena allocator (ArenaV2) |

### Architecture Improvements (v0.11.5)

- **SC/ (System Composer)**: Unified C public API, invoke chain, orchestration
- **Simplified build**: Single `libsvdb.so` from `src/CMakeLists.txt`
- **Reduced overhead**: 6 subsystem CMakeLists.txt files eliminated
- **Cleaner structure**: All core subsystems (CG, DS, IS, PB, QP, SC, SF, TM, VM) source-only

### Detailed Optimization Plan

See `docs/plan-v0.11.5.md` for complete implementation details:

- **P0 Memory Management**: Arena allocator integration (40% GC reduction target)
- **P0 Batch Query Engine**: Vectorized execution (5-10x analytical target)
- **P1 SIMD Expansion**: Batch compare/aggregate (4x speedup target)
- **P1 Hash Join**: Hash table optimization, SIMD probes (60x target)
- **P1 Bytecode Optimizer**: Predicate pushdown, loop-invariant code motion
- **P2 Index Statistics**: Cost-based optimization, index-only scans
- **P2 LTO/PGO**: Link-time and profile-guided optimization (10-15% target)

## License

See LICENSE file.

