# sqlvibe

**sqlvibe** is a high-performance in-memory database engine written in Go with SQL compatibility.

## Stable Releases

| Version | Date | Description |
|---------|------|-------------|
| **v0.11.5** | 2026-03-06 | Comprehensive performance baseline: 1K/10K benchmarks, optimization priorities, root cause analysis |
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

- **Storage**: Hybrid row/columnar store with RoaringBitmap indexes
- **Query Processing**: Tokenizer → Parser → Optimizer → Compiler
- **Execution**: Register-based VM with vectorized execution
- **Memory**: Arena allocator + sync.Pool for zero-GC query execution
- **Extensions**: Build-tag controlled, statically linked, auto-registered at startup

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details.

## Performance

**Test Hardware**: 13th Gen Intel(R) Core(TM) i7-13650HX (20 cores), Linux, Go 1.21+  
**Benchmarks**: In-memory database, `-benchtime=500ms` for 1K/10K tests.  
**Methodology**: Result cache cleared before each sqlvibe iteration via `db.ClearResultCache()`.  
SQLite's `database/sql` driver reuses prepared statements. Both sides iterate all result rows.  
(`go test ./tests/Benchmark/... -bench=BenchmarkCompare_ -benchmem`).  
Results may vary on different hardware.

### SQLite vs sqlvibe (v0.11.5 — Comprehensive Baseline)

Build with `./build.sh -t` to run tests with all CGO optimizations enabled.

**v0.11.5 Highlights**: Optimized C++ module structure with `SC/` (System Composer) subsystem,
unified `libsvdb.so` build. This baseline establishes detailed metrics for optimization tracking.

#### Full Benchmark Results (1K, 10K rows)

| Workload | Scale | SQLite | sqlvibe | Ratio | Status |
|----------|-------|--------|---------|-------|--------|
| **SELECT all** | 1K | 442 µs | 4.17 ms | 9.4× | 🔴 slower |
| **SELECT all** | 10K | 4.22 ms | 47.8 ms | 11.3× | 🔴 slower |
| **COUNT(*)** | 1K | 3.9 µs | 510 µs | 130.8× | 🔴 slower |
| **COUNT(*)** | 10K | 5.2 µs | 4.89 ms | 940.4× | 🔴 slower |
| **SUM aggregate** | 1K | 43 µs | 1.90 ms | 44.2× | 🔴 slower |
| **SUM aggregate** | 10K | 375 µs | 18.5 ms | 49.3× | 🔴 slower |
| **GROUP BY** (4 groups) | 1K | 312 µs | 4.75 ms | 15.2× | 🔴 slower |
| **GROUP BY** (4 groups) | 10K | 3.27 ms | 49.0 ms | 15.0× | 🔴 slower |
| **INSERT batch** | 1K | 3.98 ms | 2.28 ms | **1.7×** | 🟢 faster |
| **INNER JOIN** | 1K | 619 µs | 6034 ms | 9746.4× | 🔴 slower |

#### Scalability Analysis (Slowdown Factor vs SQLite)

| Workload | 1K Slowdown | 10K Slowdown | Scaling |
|----------|-------------|--------------|---------|
| SELECT all | 9.4× | 11.3× | +20% |
| COUNT(*) | 130.8× | 940.4× | +619% ⚠️ |
| SUM aggregate | 44.2× | 49.3× | +12% |
| GROUP BY | 15.2× | 15.0× | -1% ✓ |
| INSERT batch | 0.6× | N/A | N/A |

> **Key Insights**:
> - **INSERT batch**: 1.7× faster — only workload where sqlvibe leads (C++ direct insert fast path)
> - **GROUP BY**: Best scaling behavior (+12% slowdown at 10K, stable ratio)
> - **COUNT(*)**: Worst scaling (940× slower at 10K) — needs index-only scan optimization
> - **INNER JOIN**: Critical bottleneck (9746× slower) — hash join needs urgent optimization
> - **SELECT all**: Moderate overhead (9-11×) — bytecode VM dispatch cost

### Optimization Priorities (v0.11.5 → v0.11.6)

Based on benchmark analysis, here are the prioritized optimization targets:

| Priority | Area | Current | Target | Impact |
|----------|------|---------|--------|--------|
| **P0** | INNER JOIN | 6034 ms | 100 ms | 60× speedup |
| **P0** | COUNT(*) 10K | 4.89 ms | 50 µs | 98× speedup |
| **P1** | SELECT all 10K | 47.8 ms | 5 ms | 10× speedup |
| **P1** | SUM aggregate 10K | 18.5 ms | 500 µs | 37× speedup |
| **P2** | GROUP BY 10K | 49.0 ms | 3 ms | 16× speedup |

### Root Cause Analysis

| Bottleneck | Component | Root Cause | Solution |
|------------|-----------|------------|----------|
| VM dispatch | VM/exec.cpp | Per-instruction function call overhead | Batch execution, inline dispatch |
| Hash join | VM/hash_join.cpp | Naive hash table, no SIMD | Hash table tuning, SIMD probes |
| COUNT(*) | DS/btree.cpp | Full table scan | Index-only scan, metadata cache |
| Memory alloc | Multiple | malloc/new per-row | Arena allocator (ArenaV2) |
| GC pressure | Go runtime | CGO → Go boundary allocations | Reduce crossing, batch results |

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

---

### Historical Reference (v0.11.2 — AMD EPYC 7763)

Preserved for comparison. Results from AMD EPYC 7763 64-Core @ 2.45 GHz with v0.11.2.

| Workload | SQLite | sqlvibe | Result |
|----------|--------|---------|--------|
| SUM aggregate (100K) | 6.08 ms | 2.33 ms | **2.6× faster** |
| GROUP BY (100K) | 57.7 ms | 11.7 ms | **4.9× faster** |
| SELECT all (100K) | 28.6 ms | 27.4 ms | **1.0× faster** |
| INSERT batch (10K) | 56.1 ms | 32.3 ms | **1.7× faster** |

> **Note**: The performance delta between AMD EPYC (v0.11.2) and Intel i7 (v0.11.5) highlights
> architecture sensitivity. Server CPUs (EPYC) show better sqlvibe performance due to:
> - Higher memory bandwidth (8-channel vs dual-channel)
> - Larger L3 cache (256MB vs 24MB)
> - Different CGO scheduling behavior
> - Go runtime GC tuning differences

## License

See LICENSE file.

