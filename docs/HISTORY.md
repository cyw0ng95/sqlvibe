# sqlvibe Release History

## **v0.7.8** (2026-02-22)

### Performance Improvements

- **VM: 2-bit saturating branch predictor** (`internal/VM/engine.go`, `exec.go`) — Added `BranchPredictor` struct with a 1024-slot 2-bit saturating counter table. Integrated into `OpNext` handler: when the predictor says "loop continues", the fast path increments the cursor index and checks bounds once; on correct prediction the counter is updated and execution continues immediately. Incorrect predictions fall through to the standard path. The predictor warms up to "strongly taken" after a few loop iterations, reducing branch mis-predictions in long table scans.
- **VM: Result cache** (`internal/VM/result_cache.go`) — New thread-safe TTL-based cache (`ResultCache`) for VM-level query rows. Keyed by `uint64`. Supports `Get`, `Set`, `Invalidate`, and LRU-style eviction when the entry limit is reached.
- **VM: String interning pool** (`internal/VM/string_pool.go`) — Added `InternString(s string) string` backed by a `sync.Map`. Returns the canonical pooled copy of a string, so that all identical string values share a single backing allocation. Reduces allocations and enables pointer-equality comparisons for deduplicated column names and constant strings.
- **DS: Standalone Prefetcher** (`internal/DS/prefetch.go`) — New `Prefetcher` struct wrapping the shared `prefetchWorkerPool`. Exposes a `Prefetch(pageNum uint32)` method for use outside of the BTree internals, allowing external callers to warm pages into the OS cache before sequential access.
- **CG: Plan cache** (`internal/CG/plan_cache.go`) — New thread-safe `PlanCache` that maps SQL strings to compiled `*VM.Program` instances. Integrated into `ExecVM`: the plan is compiled once and then served from cache on subsequent identical calls, bypassing the tokenise+parse+code-generation pipeline entirely.
- **DB: Full query result cache** (`pkg/sqlvibe/database.go`) — Added `queryResultCache` (columns + rows) keyed by FNV-1a hash of the SQL string. Pure SELECT queries are served from the cache after the first execution. The cache is invalidated atomically on any write operation (INSERT, UPDATE, DELETE, DDL). Cache is skipped during active transactions to maintain isolation.
- **QP: Top-N heap accumulator** (`internal/QP/topn.go`) — New `TopN` struct implementing a bounded max-heap for ORDER BY … LIMIT N. Streams rows in and retains only the N best using `container/heap`, giving O(N log K) time and O(K) peak memory vs. O(N log N) / O(N) for a full sort. Used by callers that know the limit at planning time.
- **QP: Predicate pushdown** (`internal/QP/optimizer.go`) — New `SplitPushdownPredicates`, `IsPushableExpr`, `EvalPushdown`, and `ApplyPushdownFilter` functions. Simple `col OP constant` conditions in a WHERE clause are now evaluated at the Go layer (in `execSelectStmtWithContext`) before rows are handed to the VM, reducing the number of rows the VM must process. AND predicates are split recursively so complex conditions have their pushable leaves extracted. Non-pushable predicates (subqueries, column OP column, function calls) remain in the WHERE clause for the VM. `stmt.Where` is restored after execution to avoid mutating the shared AST.

### New Benchmarks

- `internal/TS/Benchmark/benchmark_v0.7.8_test.go` — v0.7.8 benchmarks (12 tests):
  - `BenchmarkBranchPrediction_WarmLoop`, `BenchmarkBranchPrediction_ShortLoop` — branch prediction paths
  - `BenchmarkPlanCache_Hit` — plan-cache hit throughput
  - `BenchmarkResultCache_Hit`, `BenchmarkResultCache_Miss` — result-cache hit/miss
  - `BenchmarkTopN_Limit10`, `BenchmarkTopN_Limit100` — ORDER BY + LIMIT
  - `BenchmarkStringInterning_Repeated` — DISTINCT with repeated values
  - `BenchmarkWhereFiltering_1K`, `BenchmarkCountStar_1K`, `BenchmarkCountStarWhere_1K`, `BenchmarkJoinTwoTables`, `BenchmarkSubqueryIN`
- `internal/TS/Benchmark/benchmark_v0.7.8_sqlite_compare_test.go` — v0.7.8 SQLite comparison benchmarks (14 tests):
  - `BenchmarkSQLite78_WhereFiltering` / `BenchmarkSqlvibe78_WhereFiltering` — predicate pushdown comparison
  - `BenchmarkSQLite78_CountStar` / `BenchmarkSqlvibe78_CountStar`
  - `BenchmarkSQLite78_TopN_Limit10` / `BenchmarkSqlvibe78_TopN_Limit10` — ORDER BY LIMIT 10
  - `BenchmarkSQLite78_ResultCache_Hit` / `BenchmarkSqlvibe78_ResultCache_Hit`
  - `BenchmarkSQLite78_InnerJoin` / `BenchmarkSqlvibe78_InnerJoin`
  - `BenchmarkSQLite78_GroupBy` / `BenchmarkSqlvibe78_GroupBy`
  - `BenchmarkSQLite78_PredicatePushdown` / `BenchmarkSqlvibe78_PredicatePushdown` — 10K row AND pushdown

---

## **v0.7.7** (2026-02-22)

### Performance Improvements

- **QP: Switch-based keyword lookup** (`internal/QP/tokenizer.go`) — Replaced the `keywords` map lookup in `readIdentifier` with a two-level `switch` on `len(s)` + `s`. The switch handles all keywords up to length 7 directly, avoiding map hashing for the common case. Falls back to the existing `keywords` map for longer/less-common keywords only.
- **QP: Hex string lookup table** (`internal/QP/tokenizer.go`) — Replaced `fmt.Sscanf("%2x", ...)` in `parseHexString` with a 256-byte `hexValTable` lookup array initialised once at startup. Each hex character decodes with a single array index operation instead of format-string parsing, eliminating all allocations in the hot path.
- **QP: Token slice pre-allocation** (`internal/QP/tokenizer.go`) — `NewTokenizer` now pre-allocates the token slice with `cap = max(len(input)/8, 16)`, reducing the number of slice growth reallocations during tokenisation of typical queries.
- **DS: VarintLen with math/bits** (`internal/DS/encoding.go`) — `VarintLen` now uses `math/bits.Len64` to compute the number of significant bits in a single CPU instruction (`BSR`/`LZCNT`), replacing the eight sequential threshold comparisons. Result is capped at 9 (maximum SQLite varint size).
- **DS: sync.Pool for record encoding** (`internal/DS/encoding.go`) — Added `recordBufferPool` (a `sync.Pool` of `*bytes.Buffer`) and `EncodeRecordPooled`: a variant of `EncodeRecord` that obtains a scratch buffer from the pool, writes the encoded record into it, copies the result to a fresh caller-owned slice, and returns the buffer to the pool. This amortises the `bytes.Buffer` internal allocation cost across repeated calls.
- **DS: Worker pool for page prefetch** (`internal/DS/btree.go`) — `prefetchChildren` no longer spawns a bare goroutine per child page. Instead a single shared `prefetchWorkerPool` with 4 fixed worker goroutines and a 64-slot task channel is initialised lazily on first use. Each child page read is submitted as a closure; if the channel is full, the prefetch is silently skipped, preventing goroutine explosion under high concurrency.
- **DS: Cell key caching in findCell** (`internal/DS/btree.go`) — `findCell` now pre-decodes all cell keys from a page into a `[]cachedKey` slice before entering the binary search loop. Previously each comparison could decode the same mid-point key multiple times due to binary search revisits. With N cells the binary search visits O(log N) keys total but may visit the same key multiple times when the range narrows; pre-decoding eliminates all redundant decoding work.

### New Benchmarks

- `internal/QP/bench_tokenizer_test.go` — Tokenizer/parser benchmarks: `BenchmarkTokenizer_Identifiers`, `BenchmarkTokenizer_Numbers`, `BenchmarkTokenizer_Strings`, `BenchmarkTokenizer_HexStrings`, `BenchmarkTokenizer_FullQuery`, `BenchmarkParser_Select`, `BenchmarkParser_ComplexExpr`
- `internal/DS/bench_encoding_test.go` — Encoding benchmarks: `BenchmarkVarint_Put`, `BenchmarkVarint_Get`, `BenchmarkVarint_Len`, `BenchmarkRecord_Encode`, `BenchmarkRecord_EncodePooled`, `BenchmarkRecord_Decode`
- `internal/DS/bench_btree_test.go` — BTree and cache benchmarks: `BenchmarkBTree_Insert`, `BenchmarkBTree_Search`, `BenchmarkBTree_Cursor`, `BenchmarkCache_Get`, `BenchmarkCache_Set`

---

## **v0.7.6** (2026-02-21)

### Performance Improvements

- **CG: Common Subexpression Elimination (CSE)** (`internal/CG/optimizer.go`) — Added `eliminateCommonSubexprs` pass. Within each basic-block segment, repeated arithmetic/concat expressions with the same source registers are replaced with a cheap `OpSCopy` from the first computed result, avoiding redundant recalculation.
- **CG: Strength Reduction** (`internal/CG/optimizer.go`) — Added `reduceStrength` pass. Detects multiply/add/subtract against known compile-time constants and replaces with cheaper operations: `x * int(2) → x + x`, `x * int(1) → SCopy x`, `x * 0 → LoadConst 0`, `x + int(0) → SCopy x`, `x - int(0) → SCopy x`. Float-zero and float-one constants are intentionally excluded to preserve type-coercion semantics (e.g. `col + 0.0` promotes an integer column to float64).
- **CG: Peephole Optimizations** (`internal/CG/optimizer.go`) — Added `peepholeOptimize` pass with two patterns: (1) `OpGoto` targeting the immediately following instruction → `OpNoop`; (2) `LoadConst(rx, v); Move/SCopy(rx→ry)` where `rx` is used only once → `LoadConst(ry, v)` + `OpNoop`, reducing register pressure.
- **VM: Type Assertion Reduction** (`internal/VM/instruction.go`, `program.go`, `exec.go`) — Added `DstReg int` and `HasDst bool` fields to `Instruction`. All `EmitAdd`, `EmitSubtract`, `EmitMultiply`, `EmitDivide`, `EmitConcat`, and `EmitOpWithDst` now pre-fill these fields. The VM's hot-path `Exec` loop uses `inst.DstReg` directly (branch on bool) instead of `inst.P4.(int)` (interface type assertion) for arithmetic and concat opcodes. **BenchmarkVM_ArithmeticOps: 2 494 → 1 142 ns/op (−54%); BenchmarkVM_TypeAssertion: 20 308 → 11 119 ns/op (−45%).**
- **VM: Reset() allocation reduction** (`internal/VM/engine.go`) — `Reset()` now reuses the `subReturn` slice (`[:0]` instead of `make`) and clears the `ephemeralTbls` map in-place (`delete` loop instead of `make`) to avoid per-execution heap allocations on the hot path.

### New Benchmarks

- `internal/VM/bench_cg_test.go` — CG compilation benchmarks: `BenchmarkCG_CSE`, `BenchmarkCG_Peephole`, `BenchmarkCG_CompileSelect`, `BenchmarkCG_CompileComplexExpr`, `BenchmarkCG_ConstFolding`, `BenchmarkCG_StrengthReduction`
- `internal/VM/bench_vm_test.go` — VM execution benchmarks: `BenchmarkVM_ArithmeticOps`, `BenchmarkVM_ResultRow`, `BenchmarkVM_ResultRowNoPrealloc`, `BenchmarkVM_RegisterPrealloc`, `BenchmarkVM_TypeAssertion`, `BenchmarkVM_StringLike`, `BenchmarkVM_CursorScan`, `BenchmarkVM_SubqueryCache`, `BenchmarkVM_Aggregate`
- `internal/VM/benchdata/testdata.go` — Reusable benchmark data generators (`GenerateArithProgram`, `GenerateResultRowProgram`, `MakeTableRows`, `MakeIntTableRows`)

---

## **v0.7.5** (2026-02-21)

### New Features
- **SQLLogicTest runner** (`internal/TS/SQLLogic/`) — Custom black-box test runner that parses the standard sqllogictest `.test` format used by SQLite, PostgreSQL, TiDB and CockroachDB. Implemented using only the Go standard library (no external dependencies). Supports `statement ok`, `statement error`, and `query TYPE [rowsort|valuesort|nosort]` records. Test files are loaded from `testdata/*.test`. Runner entry point: `TestSQLLogic` in `sql_logic_test.go`.
- **Test data files** — Three coverage areas added:
  - `basic.test` — DDL (CREATE/DROP), DML (INSERT/UPDATE/DELETE), basic SELECT, NULL handling, DISTINCT, LIKE, BETWEEN, IN, string functions (48 records, 100% pass)
  - `joins.test` — INNER JOIN, LEFT JOIN, self-join, 3-table JOIN, JOIN with WHERE and aggregate (27 records, 100% pass)
  - `aggregates.test` — COUNT/SUM/AVG/MIN/MAX, GROUP BY, HAVING, COUNT DISTINCT, NULL aggregation, scalar subquery in WHERE (35 records, 100% pass)

### Bug Fixes
- **Scalar subquery in WHERE/aggregate context** (`internal/VM/exec.go`) — `evaluateExprOnRow` now handles `*QP.SubqueryExpr`: when `vm.ctx` provides `ExecuteSubqueryWithContext` or `ExecuteSubquery`, the subquery is executed and the scalar value returned. Previously the default case returned `nil`, causing `column > (SELECT ...)` to always pass the filter.
- **JOIN + GROUP BY / aggregate** (`pkg/sqlvibe/vm_exec.go`, `internal/CG/compiler.go`) — Added `execJoinAggregate` path for SELECT queries that combine a 2-table equi-JOIN with aggregate functions or GROUP BY. `CompileAggregate` only scanned a single table cursor, silently ignoring the JOIN. The new path materialises the full join result via `execHashJoin` (with a temporary `SELECT *`) then pre-opens cursor 0 with the joined rows before running the aggregate VM; `OpOpenRead` detects the pre-opened cursor and skips the single-table reload. Also added `CG.HasAggregates()` as a new exported function.
- **Table-qualified column reference in aggregate evaluation** (`internal/VM/exec.go`) — `evaluateExprOnRow` for `*QP.ColumnRef` now tries the table-qualified key (`alias.column`) in the row map first when `e.Table` is set. This fixes GROUP BY and aggregate expressions like `d.name` when rows are stored with qualified keys (as built by `execJoinAggregate`).


### Performance Improvements
- **Page prefetching** (`internal/DS/btree.go`) — Added `prefetchEnabled bool` field and `prefetchChildren(page, count)` to `BTree`. When enabled, interior-page traversal fires goroutines to warm the OS page cache for sibling child pages, reducing sequential I/O wait. Enabled via `SetPrefetchEnabled(true)`.
- **EXISTS early exit** (`pkg/sqlvibe/database.go`, `vm_context.go`, `internal/VM/exec.go`) — `OpExistsSubquery` and `OpNotExistsSubquery` now check for the new `ExistsSubqueryExecutor` interface before falling back to the full `ExecuteSubqueryRowsWithContext` path. The implementation applies `LIMIT 1` to the inner query (shallow-copy of the AST to avoid mutation), short-circuiting after the first matching row. Eliminates materializing the full subquery result set for EXISTS tests.
- **Index range scan for BETWEEN** (`pkg/sqlvibe/database.go`) — `tryIndexLookup` now recognises `col BETWEEN lo AND hi` and routes it through `tryIndexRangeScan`, which iterates only the secondary-index hash map keys rather than the full table. Reduces rows processed from O(N) to O(K) where K = distinct indexed values.
- **Index IN-list lookup** (`pkg/sqlvibe/database.go`) — `tryIndexLookup` now recognises `col IN (a, b, c)` and routes it through `tryIndexInLookup`, performing one O(1) hash lookup per IN value and unioning the results. Replaces O(N) full table scan for each probe.
- **Index LIKE prefix scan** (`pkg/sqlvibe/database.go`) — `tryIndexLookup` now recognises `col LIKE 'prefix%'` (pure trailing wildcard, no `_` in prefix) and routes it through `tryIndexLikePrefix`, scanning index keys with `strings.HasPrefix`. Falls back to full table scan for complex patterns.
- **sync.Pool for hash join merged rows** (`pkg/sqlvibe/hash_join.go`) — `buildJoinMergedRow` now obtains its scratch `map[string]interface{}` from `mergedRowPool` (sync.Pool) and callers return it via `putMergedRow` after use. Eliminates one map allocation per matched row pair in hash joins with WHERE clauses.
- **VM flat result backing array** (`internal/VM/engine.go`, `exec.go`) — Added `flatBuf []interface{}` to the VM struct. `OpResultRow` now writes result values into a pre-allocated contiguous flat buffer and uses sub-slices as row values instead of calling `make([]interface{}, n)` per row. `PreallocResultsFlat(rows, cols)` pre-allocates both the header slice and the flat buffer. `Reset()` reuses existing capacities (`[:0]`) instead of re-allocating. Eliminates one allocation per result row. **SELECT * on 1K-row table: 1 060 allocs → 15 allocs (71×), 280 µs → 54 µs (5.2×).**
- **SELECT * fast path** (`pkg/sqlvibe/vm_exec.go`) — `isSimpleSelectStar` detects `SELECT * FROM table` queries with no WHERE, GROUP BY, ORDER BY, DISTINCT, LIMIT, JOINs, or subqueries. `execSelectStarFast` bypasses tokenize/parse/compile/VM entirely, materializing results from `db.data` directly into 2 allocations (flat backing array + row header slice) regardless of row count. **5 000-row scan: ~1.4 ms → 342 µs (4.1×); 15 000-row scan scales linearly at ~13 µs per 1 000 rows.**

### New Benchmarks
- `BenchmarkIndexBetween` — BETWEEN on secondary-indexed integer column (1 000 rows)
- `BenchmarkIndexInList` — IN list on secondary-indexed text column (1 000 rows)
- `BenchmarkIndexLikePrefix` — LIKE 'prefix%' on secondary-indexed text column (1 000 rows)
- `BenchmarkExistsSubquery` — EXISTS with correlated subquery (100 parent × 1 000 child rows)
- `BenchmarkHashJoinWithWhere` — Hash join with WHERE clause (20 dept × 500 emp rows)
- `BenchmarkSelectAll5K` — SELECT * on 5 000-row table (validates sub-400 µs target)

### New Tests
- `TestIndexBetweenScan` — Regression guard for BETWEEN index range scan
- `TestIndexInListScan` — Regression guard for IN-list index lookup
- `TestIndexLikePrefixScan` — Regression guard for LIKE prefix index scan

### Architecture Notes
- `compareIndexVals(a, b)` — New package-level helper in `database.go` for ordering index key values (int64, float64, string, mixed). Used by `tryIndexRangeScan`.
- `tryIndexRangeScan`, `tryIndexInLookup`, `tryIndexLikePrefix` — Three new sub-functions extracted from `tryIndexLookup` for each extended index-scan pattern.
- `execExistsSubquery(stmt, outerRow)` — New method on `Database` that shallow-copies the stmt, sets `Limit=1`, and delegates to `execSelectStmtWithContext`. Exposed as `ExecuteExistsSubquery` on all three VM context types.
- `isSimpleSelectStar(stmt)` + `execSelectStarFast(rows, cols)` — New helpers in `vm_exec.go`. `execSelectStarFast` pre-allocates a single `n×ncols` flat `[]interface{}` backing array; each result row is a sub-slice of that array.
- `VM.flatBuf []interface{}` + `PreallocResultsFlat(rows, cols)` — VM now maintains a contiguous flat backing array that grows with amortised doubling (2× + 64). Callers use `PreallocResultsFlat` to hint the expected result size.
- Wave 4 (AND/OR short-circuit) was already implemented: `evaluateBoolExprOnRow` in `exec.go` uses Go's native `&&` / `||` short-circuit operators.

### Breaking Changes
- None

---

## **v0.7.3** (2026-02-21)

### Performance Improvements
- **GROUP BY key: `strings.Builder` + type switch** — Replaced per-row `fmt.Sprintf` + `[]string` + `strings.Join` in `computeGroupKey` with a single `strings.Builder` write and a type switch (`int64`, `float64`, `string`, `bool`, `nil` fast paths). GROUP BY is ~11% faster.
- **SortRows pre-resolved column indices** — Pre-resolve `ORDER BY col_name` column indices once before sorting (was a linear scan per comparison pair). Skip per-row `rowMap` allocation for non-ColumnRef ORDER BY terms. **10–12% faster ORDER BY, 9% less memory.**
- **Top-K heap for `ORDER BY … LIMIT N`** — New `SortRowsTopK(data, orderBy, cols, topK)` using `container/heap`. Maintains a bounded max-heap of topK=offset+limit candidates. For ColumnRef ORDER BY (the common case), rows that don't enter the heap incur zero allocation. Stable sort semantics preserved via `origIdx` tiebreaker. Shared `cmpOrderByKey` helper centralises NULL/DESC comparison logic. **ORDER BY + LIMIT 10 on 1 000 rows: 22% faster, 28% less memory.**
- **Primary key O(1) uniqueness check** (`pkg/sqlvibe/database.go`, `vm_context.go`) — INSERT into a PRIMARY KEY table previously scanned all existing rows for uniqueness (O(N) per insert → O(N²) total for N inserts). Added `pkHashSet map[string]map[interface{}]struct{}` per table. The set is initialised on `CREATE TABLE`, maintained on INSERT/UPDATE/DELETE, and rebuilt on transaction rollback. INSERT uniqueness check is now O(1) amortised. **Batch insert of 1 000 PK rows is now constant-time (was O(N²)).**
- **In-memory secondary hash index** (`pkg/sqlvibe/database.go`, `vm_exec.go`) — `WHERE indexed_col = val` queries on indexed columns still did a full O(N) table scan because the index metadata was never applied at query time. Added `indexData map[string]map[interface{}][]int` (index name → column value → []row indices). Built immediately on `CREATE INDEX`, maintained on INSERT/UPDATE/DELETE, rebuilt on rollback. New `tryIndexLookup` pre-filter in `execSelectStmtWithContext` passes only matching rows to the VM. **~10× reduction in rows processed for selective equality lookups on indexed columns.**
- **`deduplicateRows` key** (`pkg/sqlvibe/vm_exec.go`) — `UNION`/`UNION ALL` used `fmt.Sprintf("%v", row)` per row for deduplication (1 allocation each). Replaced with a reusable `strings.Builder` + type switch (int64/float64/string/bool/nil fast paths). Eliminates per-row `fmt.Sprintf` allocation.
- **GROUP BY `interface{}` key for single-column GROUP BY** (`internal/VM/exec.go`) — `computeGroupKey` called `strings.Builder.String()` per row, allocating a new string for every row even when the group already exists. For single-expression GROUP BY, the raw column value is now used directly as the `map[interface{}]` key (int64/float64/string/bool: zero extra allocation; []byte: one conversion to string). **Eliminates ~1 alloc/row** for the dominant `GROUP BY col` pattern.
- **Hash join: `interface{}` key map** (`pkg/sqlvibe/hash_join.go`) — The hash join build and probe phases called `hashJoinKey()` (a `fmt.Sprintf`-based function) to produce a string key for every row. Replaced with a direct `interface{}` map (`map[interface{}][]...`) and `normalizeJoinKey()` that converts only `[]byte` to string; all other comparable types (int64, float64, string, bool) are used directly. **Eliminates one string allocation per join key lookup on both build and probe.**
- **Hash join: skip merged-row map for star-only no-WHERE queries** (`pkg/sqlvibe/hash_join.go`) — `buildJoinMergedRow` allocated a `map[string]interface{}` per match, even for the common `SELECT * FROM a JOIN b ON …` case where all output columns are stars and WHERE is absent. Added a fast path that skips the merged map entirely; output rows are built directly from source rows. **Eliminates one map allocation per matched row pair.**

### New Benchmarks
- `BenchmarkInsertBatchPK` — batch insert into PK table (validates O(1) hash set)
- `BenchmarkSecondaryIndexLookup` — equality WHERE on secondary index (100/1 000 rows)
- `BenchmarkSecondaryIndexLookupUnique` — unique index equality lookup (1/1 000 rows)
- `BenchmarkDeduplicateRows` — UNION deduplication throughput

### Architecture Notes
- Comparison logic extracted into `cmpOrderByKey(qe, keyA, keyB, ob)` — used by `SortRows`, `topKHeap.Less`, `topKHeap.lessEntry`, and `SortRowsTopK.compareRawToTop`. Single authoritative source for NULL handling and DESC order, eliminating four previous copies.
- `pkKey()` helper normalises single-col and composite PK values into a comparable map key (`interface{}` for single-col, `string` via `strings.Builder` for multi-col).
- `normalizeIndexKey(v)` converts `[]byte` to `string` for hashability; used by both `pkKey` and the secondary index.
- `indexShiftDown(fromIdx)` shifts entries `> fromIdx` down by 1 after DELETE, keeping row indices consistent without full rebuild.
- All index maintenance (`addToIndexes`, `removeFromIndexes`, `updateIndexes`, `rebuildAllIndexes`) flows through a single set of helpers in `database.go`.
- `normalizeJoinKey(v)` converts `[]byte` to `string`; other comparable types pass through for direct use as `map[interface{}]` keys in the hash join.

### Bug Fixes
- **LIMIT in IN subqueries now correctly applied**: Two related bugs caused `LIMIT` inside an `IN (SELECT …)` subquery to be silently ignored, matching all rows instead of only the top-K.
  - `compileBinaryExpr` (CG) called `compileExpr(Right)` eagerly for every binary operator, which caused a spurious `OpScalarSubquery` to be emitted for `TokenInSubquery`/`TokenNotIn`/`TokenExists`. When the VM executed `OpScalarSubquery`, it ran the inner query and mutated the shared `SelectStmt` (clearing `Limit` and `OrderBy`), so the subsequent `OpInSubquery` saw no LIMIT.
  - `execSelectStmt` (called from `ExecuteSubqueryRows`) delegated to `execVMQuery` but never applied `ORDER BY + LIMIT` when all `ORDER BY` columns were already in the `SELECT` list (the `extraOrderByCols` path was not taken).
  - Fixed by: (a) adding early-exit paths in `compileBinaryExpr` for `TokenInSubquery`, `TokenNotIn` (subquery), and `TokenExists` before the eager evaluation; (b) applying `ORDER BY + LIMIT` in `execSelectStmt` after `execVMQuery` returns, matching the same logic in `database.go`.

### Breaking Changes
- None

---

## **v0.7.2** (2026-02-21)

### Performance Improvements
- **SUM / AVG typed accumulators**: Replaced per-row `interface{}` boxing in the aggregate engine with typed `int64`/`float64` fields in `AggregateState`. Eliminates ~1 heap allocation per row for SUM and AVG: **94% fewer allocations** (1 032 → 58 allocs/op on 1 000-row table), and queries run ~25% faster.
- **Self-join / qualified-star hash join**: Queries of the form `SELECT a.*, b.* FROM t a JOIN t b ON …` were incorrectly falling back to an O(N²) VM nested-loop join because the hash join rejected qualified stars (`t.*`). Extended hash join column expansion to support qualified-star syntax, routing self-joins through the O(N+M) hash join path. **9× speedup** (1.57 ms → 169 µs for a 100-row self-join).
- **Benchmark suite expansion (v0.7.2)**: Added 49 new benchmark tests covering all DB engine layers (DS, VM, QP, TM), bringing total benchmark count to 70.

### Bottlenecks Identified (for future work)
- Secondary index queries do not yet use secondary indexes (full table scan always used).
- `SELECT … ORDER BY … LIMIT N` materializes the full result set before limiting.
- JOIN row materialization copies all rows into memory before joining.
- `GROUP BY` uses `fmt.Sprintf` string keys per row.

### Bug Fixes
- None

### Breaking Changes
- None

---

## **v0.7.1** (2026-02-21)

### Performance Improvements
- **Subquery Materialization (Wave 1)**: Non-correlated IN/NOT IN subqueries are now materialized into a hash set once per outer query execution, eliminating redundant full-table scans. InSubquery benchmark: ~101x faster (19.5ms → 0.19ms). ScalarSubquery benchmark: ~31x faster (2.6ms → 0.08ms).
- **Hash Join (Wave 2)**: Two-table INNER equi-joins now use a Go-level hash join (O(N+M)) instead of the previous O(N×M) nested-loop VM bytecode. Join benchmark: ~11x faster (7.9ms → 0.7ms). Correctly handles NULL join keys per SQL standard (NULLs never match).
- **Result Set Pre-allocation (Wave 3)**: VM result slices are pre-allocated based on estimated table size, reducing reallocations for large SELECT queries.
- **Object Pool Utility**: Added `internal/SF/util/pool.go` with reusable byte buffer and interface slice pools for frequently allocated objects.

### Bug Fixes
- Fixed correlated subquery detection when inner table has an alias (e.g., `SELECT 1 FROM t c WHERE c.id = t.id - 1`). Previously such queries were incorrectly treated as non-correlated.
- Fixed NULL key handling in hash join: NULL values in equi-join columns are now correctly excluded from matches.

### Breaking Changes
- None

---

## **v0.7.0** (2026-02-21)

### Bug Fixes
- None

### Features
- **CG Optimizer**: Constant folding and dead code elimination passes implemented
- **Page Cache (LRU)**: Full LRU cache with SQLite-compatible cache_size PRAGMA
- **WAL Mode**: Write-ahead logging with WAL header, frame format, checkpoint support
- **Remove QE Subsystem**: Architecture simplified, QE layer completely removed
- **SQL1999 Tests**: Expanded from 56 to 64+ test suites (224 → 340+ test cases)
- **Benchmark Suite**: Added 25+ benchmark tests

### Breaking Changes
- None

---

## **v0.6.0** (2026-02-20)

### Bug Fixes
- None

### Features
- **DS Subsystem (90% complete)**: Page type validation, cell bounds, cursor state assertions
- **VM Subsystem (30% complete)**: Cursor ID bounds validation
- **QP/QE Subsystems (20% complete)**: Token array and schema validation
- **TM Subsystem (10% complete)**: Transaction manager PageManager validation
- **PB Subsystem (60% complete)**: File offset and buffer validation
- **Public API (10% complete)**: Row scanning bounds validation

### Breaking Changes
- None

### Assertion Coverage
- Overall: ~35% of critical code paths
- Core data structure validation complete, preventing most B-Tree and page corruption bugs

### Testing
All existing tests pass with current assertions:
- internal/DS/... - All tests passing
- internal/VM/... - All tests passing
- internal/QP/... - All tests passing
- internal/QE/... - All tests passing
- internal/TM/... - All tests passing
- internal/PB/... - All tests passing

---

## **v0.5.2** (2026-02-18)

### Summary
Bug fix release addressing LIKE, GLOB, and SUBSTR issues from v0.5.1.

### Bug Fixes
- **LIKE**: Rewrote pattern matching algorithm, fixed % and _ wildcards
- **LIKE**: Added NOT LIKE support (TokenNotLike)
- **GLOB**: Added OpGlob and globMatch function (case-sensitive)
- **SUBSTR**: Fixed start=0 edge case
- **Numeric comparison**: Added toFloat64 helper for consistent int64/float64 comparison

### Known Issues (Deferred)
- DECIMAL/NUMERIC type ordering (requires DS layer type affinity fix)

---

## **v0.5.1** (2026-02-18)

### Summary
Bug fix release addressing critical issues from v0.5.0.

### Bug Fixes
- **DS Encoding**: Fixed serial type mapping (removed Int24, SQLite doesn't use it)
- **ORDER BY**: Fixed expression evaluation using EvalExpr for non-column references
- **IN/NOT IN**: Fixed NULL propagation in OpBitOr/OpBitAnd operators
- **BETWEEN**: Fixed NULL handling same as IN operators
- **TRIM**: Fixed default characters when P2=0 (now means space)
- **SUBSTR**: Fixed length parameter handling and negative/zero edge cases

### Known Issues (Remaining)
- LIKE/GLOB pattern matching edge cases
- DECIMAL/NUMERIC type handling
- SUBSTR(str, 0, n) edge case

---

## **v0.5.0** (2026-02-18)

### Summary
Major architectural release delivering three core infrastructure components: CG (Code Generator) subsystem, VFS (Virtual File System) architecture, and complete BTree implementation with SQLite-compatible encoding.

### Features
- **CG Subsystem**: Extracted compiler from VM into dedicated Code Generator package for clean separation of concerns (AST → bytecode → execution)
- **VFS Architecture**: Implemented pluggable storage abstraction layer with Unix VFS and Memory VFS implementations
- **Complete BTree**: Full SQLite-compatible BTree encoding (~2500 lines) including:
  - Varint & record encoding
  - Cell formats for all 4 page types (table/index leaf/interior)
  - Overflow page management
  - Page balancing algorithms
  - Freelist management
- **WHERE Operators**: Added OR, AND, IN, BETWEEN, LIKE, IS NULL operators

### Known Issues (Not Fixed in This Release)
- DS encoding tests: int32/int64 serial type mapping incorrect
- ORDER BY expression/ABS handling bugs
- IN/BETWEEN operator bugs
- Varchar TRIM and SUBSTR string operation issues
- LIKE operator 1 edge case (case sensitivity)

### Bug Fixes
- Cell boundary detection: Fixed payload size overflow in BTree
- WHERE operators: 13/14 tests passing (93%)

---

## **v0.4.5** (2026-02-16)

### Summary
Final verification release. Test failures reduced from 72 to 36 (50% improvement).

### Known Issues (Not Fixed)
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these SQL-standard functions
- OCTET_LENGTH: SQLite doesn't support this SQL-standard function
- POSITION: SQLite doesn't support this SQL-standard function
- Unicode case folding: Go and SQLite handle Unicode case conversion differently
- MinInt64 display: -9223372036854775808 displays as float64 (pre-existing)
- ABS on multiple columns: Pre-existing engine issue

### Fixed in Previous Versions
- v0.4.1: NOT IN, NOT BETWEEN, LIKE, GLOB, NULL handling
- v0.4.2: LENGTH (Unicode), INSTR, TRIM, SUBSTR
- v0.4.3: CAST expression
- v0.4.4: ROUND negative precision

---

## **v0.4.4** (2026-02-16)

### Bug Fixes
- ROUND: Fixed handling of negative precision (ROUND(x, -n))

### Known Issues
- ABS on columns: Pre-existing engine issue with multiple column evaluation
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these functions
- OCTET_LENGTH: SQLite doesn't support this function

---

## **v0.4.3** (2026-02-16)

### Bug Fixes
- CAST expression: Implemented CAST(expr AS type) syntax
- Support for CAST to INTEGER, REAL, TEXT, BLOB types
- Most CAST tests now pass (E02110)

### Known Issues
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these functions
- OCTET_LENGTH: SQLite doesn't support this function  
- Unicode case folding differs between Go and SQLite (UPPER/LOWER)

---

## **v0.4.2** (2026-02-16)

### Bug Fixes
- LENGTH: Fixed to count Unicode characters (runes) instead of bytes
- INSTR: Fixed argument order (haystack, needle) and use rune-based indexing
- TRIM/LTRIM/RTRIM: Added support for two-argument form TRIM(str, chars)
- SUBSTR/SUBSTRING: Fixed negative start index, zero start, and Unicode support

### Tests
- E02104: LENGTH_Unicode, LENGTH_Chinese, LENGTH_Emoji now pass
- E02106: Many SUBSTR tests now pass
- E02109: TRIM_Special, LTRIM_Special, RTRIM_Special now pass

### Known Issues
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these functions
- OCTET_LENGTH: SQLite doesn't support this function
- POSITION: SQLite doesn't support this function
- TRIM tabs/newlines: Test data encoding differs between sqlvibe and SQLite

---

## **v0.4.1** (2026-02-16)

### Bug Fixes
- NOT IN operator: Implemented in parser and engine
- NOT BETWEEN operator: Implemented in parser and engine
- LIKE operator: Fixed in SELECT expressions (added to evalValue)
- NOT LIKE operator: Implemented in parser and engine
- GLOB operator: Implemented with pattern matching
- NULL arithmetic: Fixed add, sub, mul, div, mod, concat to return NULL for NULL operands
- NULL comparisons: Fixed 3-valued logic for comparisons with NULL
- AND/OR operators: Fixed in SELECT expressions

### Tests
- E01105: All IN/BETWEEN/NULL comparison tests now pass
- E02112: All LIKE/GLOB/BETWEEN/IN tests now pass

---

## **v0.4.0** (2026-02-16)

### Features
- Index support: CREATE INDEX, DROP INDEX, B-Tree operations
- Set operations: UNION, EXCEPT, INTERSECT
- CASE expressions: Simple and Searched CASE
- Full E021 character data types support
  - CHAR, CHARACTER types
  - VARCHAR, TEXT types
  - Character functions: UPPER, LOWER, LENGTH, SUBSTRING, TRIM, INSTR
  - String concatenation (|| operator)
  - Implicit type casting
- Date/Time types: DATE, TIME, TIMESTAMP
- Date/Time functions: CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, STRFTIME, YEAR, MONTH, DAY
- Query planner optimizations using indexes
- PRAGMA statements: table_info, index_list, database_list
- PlainFuzzer: Go native fuzzing framework for SQL testing

### Known Issues
- Some edge cases in INSTR/POSITION functions may differ from SQLite
- BETWEEN with character types not fully implemented

### Fixed Bugs
- Float math functions (ABS, CEIL, FLOOR, ROUND) now return correct values
- DECIMAL/NUMERIC arithmetic operations fixed
- Unary minus on column references works correctly
- NULL IS NULL / IS NOT NULL returns 0/1 (not NULL)
- Implicit numeric casting between INTEGER/REAL/DECIMAL
- COALESCE returns first non-NULL argument correctly
- PlainFuzzer database reuse issue fixed (commit e51554d)

### Tests
- E011: Comprehensive numeric type tests (~290 test cases)
- E021: Complete character data types tests (251 test cases across 12 sections)
- PlainFuzzer: SQL fuzzing with mutation strategies

---

## **v0.3.0** (2026-02-15)

### Features
- JOIN support (INNER, LEFT, CROSS)
- sqlite_master table
- Subqueries (Scalar, EXISTS, IN, ALL/ANY, Correlated)
- :memory: database support
- TS Test Suites subsystem

### Known Issues
- ABS, CEIL, FLOOR, ROUND functions not implemented
- DECIMAL/NUMERIC type handling incomplete
- IS NULL/IS NOT NULL returns nil instead of 0/1

### Fixed Bugs
- Column ordering in SELECT * queries (commit 316b157)
- Constant expression evaluation (SELECT 10 + 5.0) (commit 316b157)
- Comparison operators return 0/1 instead of nil (commit 316b157)
- Integer division follows SQLite behavior (commit 316b157)

### Tests
- E011-01 through E011-06 numeric type tests added

---

## **v0.2.0** (2026-02-15)

### Features
- WHERE enhancements: AND, OR, NOT evaluation
- IS NULL / IS NOT NULL
- IN operator
- BETWEEN
- LIKE pattern matching

### Known Issues
- COALESCE function not implemented
- IFNULL function not implemented

### Fixed Bugs
- None

### Tests
- 21 passing (+8 from v0.1.0)

---

## **v0.1.0** (2026-02-15)

### Features
- Basic DML: INSERT, UPDATE, DELETE
- Basic Queries: SELECT, WHERE (simple), ORDER BY, LIMIT
- Aggregates: COUNT, SUM, AVG, MIN, MAX
- Transactions: BEGIN, COMMIT, ROLLBACK
- Prepared Statements

### Known Issues
- None

### Fixed Bugs
- None (initial release)

### Tests
- 13 passing (~47 subtests)
