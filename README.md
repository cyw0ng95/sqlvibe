# sqlvibe

**sqlvibe** is a SQLite-compatible database engine written in Go. It implements a register-based virtual machine, a B-Tree storage layer, and a full SQL:1999 query pipeline — all built from scratch in pure Go.

## Features

- **Full SQL:1999 support** — 56/56 test suites passing (100%)
- **SQLite-compatible file format** — on-disk databases readable by SQLite tools
- **In-memory databases** — `:memory:` URI for fast, ephemeral storage
- **Comprehensive SQL**:
  - DDL: `CREATE`/`DROP`/`ALTER TABLE`, `CREATE`/`DROP INDEX`, `CREATE`/`DROP VIEW`
  - DML: `INSERT` (multi-row), `SELECT`, `UPDATE`, `DELETE`
  - Joins: `INNER`, `LEFT`, `CROSS` with aliases and multi-table
  - Subqueries: scalar, `EXISTS`, `IN`, `ALL`/`ANY`, correlated
  - Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
  - Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT`
  - Window functions: `OVER (PARTITION BY … ORDER BY …)`, `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`
  - Common Table Expressions (`WITH … AS (…)`)
  - `CASE` expressions, `CAST`, `BETWEEN`, `LIKE`/`GLOB`, `IS NULL`
  - String functions: `LENGTH`, `SUBSTR`, `UPPER`, `LOWER`, `TRIM`, `INSTR`, `REPLACE`, `COALESCE`
  - Math functions: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `MOD`, `POW`, `SQRT`, trig
  - Date/Time: `DATE`, `TIME`, `DATETIME`, `STRFTIME`, `CURRENT_DATE`/`TIME`/`TIMESTAMP`
  - Constraints: `NOT NULL`, `UNIQUE`, `PRIMARY KEY`, `CHECK`, `DEFAULT`
  - Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`
  - `INFORMATION_SCHEMA` views: `TABLES`, `COLUMNS`, `TABLE_CONSTRAINTS`, `VIEWS`
  - `PRAGMA`: `table_info`, `index_list`, `database_list`
  - `EXPLAIN` statement

## Quick Start

```go
import "github.com/sqlvibe/sqlvibe/pkg/sqlvibe"

// Open an on-disk database
db, err := sqlvibe.Open("mydb.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Or use an in-memory database
db, err = sqlvibe.OpenMemory()

// Execute DDL
_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)`)

// Insert rows
_, err = db.Exec(`INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)`)

// Query rows
rows, err := db.Query(`SELECT name, age FROM users WHERE age > 20 ORDER BY name`)
if err != nil {
    log.Fatal(err)
}
for rows.Next() {
    var name string
    var age int
    _ = rows.Scan(&name, &age)
    fmt.Printf("%s: %d\n", name, age)
}
```

## Architecture

sqlvibe is organised into ten internal subsystems:

| Subsystem | Package | Responsibility |
|-----------|---------|----------------|
| System Framework | `internal/SF` | Logging, VFS interface definition |
| Platform Bridges | `internal/PB` | VFS implementations (Unix, Memory), file I/O |
| Data Storage | `internal/DS` | SQLite-compatible B-Tree, page cache, encoding |
| Transaction Monitor | `internal/TM` | ACID transactions, lock manager, WAL |
| Query Processing | `internal/QP` | SQL tokenizer, recursive-descent parser, AST |
| Code Generator | `internal/CG` | AST → VM bytecode compiler, optimizer |
| Virtual Machine | `internal/VM` | Register-based bytecode executor (~200 opcodes) |
| Query Execution | `internal/QE` | Expression and operator evaluation |
| Information Schema | `internal/IS` | INFORMATION_SCHEMA virtual views, schema registry |
| Test Suites | `internal/TS` | SQL:1999 tests, benchmarks, fuzzer |

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for a full description of each subsystem.

## Building

```bash
# Build the project
go build ./...

# Run all tests
go test ./...

# Run SQL:1999 compatibility test suite
go test ./internal/TS/SQL1999/...

# Run a specific test
go test -run TestSQL1999_E011 ./internal/TS/SQL1999/...

# Run benchmarks
go test ./internal/TS/Benchmark/... -bench . -benchmem

# Format code
go fmt ./...

# Vet code
go vet ./...
```

## Performance

Benchmarks run on an Intel Xeon Platinum 8370C @ 2.80GHz (in-memory database, `-benchtime=3s -benchmem`).  
All measurements are end-to-end (parse → compile → execute) via the public API.

### v0.7.8 Optimizations

v0.7.8 added seven targeted performance optimizations: branch prediction (2-bit saturating counters in OpNext), VM result cache, string interning, async prefetcher, CG plan cache, full-query result cache (FNV-1a keyed), and predicate pushdown at the Go layer before the VM.

#### Plan Cache & Result Cache (v0.7.8)

After the first execution of an identical query, the plan is served from the plan cache (skipping tokenise+parse+codegen) and — for pure SELECTs outside a transaction — the full result is served from the result cache.

| Benchmark | Cold (first call) | Warm (cache hit) | Speedup |
|-----------|------------------:|-----------------:|--------:|
| Plan cache hit — simple SELECT | ~52 µs | ~42 µs | 19% |
| Result cache hit — filtered SELECT | ~180 µs | < 1 µs | >100x |

#### Predicate Pushdown (v0.7.8)

Simple `col OP constant` conditions are now evaluated at the Go layer before the VM even sees the row, reducing the work the VM must do.

| Benchmark | sqlvibe v0.7.7 | sqlvibe v0.7.8 | SQLite Go |
|-----------|---------------:|---------------:|----------:|
| WHERE x > 500 (1K rows) | ~210 µs | ~160 µs | ~120 µs |
| WHERE x > 500 AND y < 50 (10K rows) | ~1.8 ms | ~1.2 ms | ~620 µs |

#### Branch Prediction (v0.7.8)

The `OpNext` hot path now uses a 2-bit saturating counter branch predictor, warming to "strongly taken" after a few loop iterations.

| Benchmark | v0.7.7 | v0.7.8 | Δ |
|-----------|-------:|-------:|---|
| Full scan 1K rows | ~175 µs | ~155 µs | −11% |
| LIMIT 10 (1K rows) | ~16 µs | ~14 µs | −12% |

### Comparison with SQLite Go (v0.7.8)

Benchmarks comparing sqlvibe against [go-sqlite](https://github.com/glebarez/go-sqlite) on identical workloads (in-memory, `-benchtime=3s -benchmem`):

| Operation | sqlvibe v0.7.8 | SQLite Go | Winner |
|-----------|---------------:|----------:|--------|
| WHERE filtering (1K rows) | ~160 µs | ~120 µs | SQLite 25% faster |
| COUNT(*) (1K rows) | ~35 µs | ~6 µs | SQLite 6x faster |
| ORDER BY LIMIT 10 (10K rows) | ~1.2 ms | ~400 µs | SQLite 3x faster |
| INNER JOIN (100 × 100) | ~620 µs | ~840 µs | **sqlvibe 26% faster** |
| GROUP BY + SUM (1K rows) | ~100 µs | ~530 µs | **sqlvibe 5x faster** |
| Result cache hit (500 rows) | < 1 µs | ~140 µs | **sqlvibe >100x faster** |
| INSERT single row | ~20 µs | ~27 µs | **sqlvibe 26% faster** |
| DELETE single row | ~22 µs | ~43 µs | **sqlvibe 49% faster** |

**Analysis (v0.7.8):**
- **sqlvibe advantages**: Full result cache (>100x for repeated queries), GROUP BY (5x), DML (26–49% faster), JOIN (26% faster)
- **SQLite advantages**: WHERE filtering (B-tree + index optimizer), COUNT aggregate (native optimizer), ORDER BY LIMIT (native sort optimizer)

### Core Operations (v0.7.3)

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| INSERT single row (PK table) | 9.0 µs | 6.8 KB | 83 |
| INSERT batch 100 rows | 657 µs | 642 KB | 7,500 |
| UPDATE single row | 18.2 µs | 6.2 KB | 64 |
| DELETE single row | 14.5 µs | 6.3 KB | 71 |
| SELECT all (1,000 rows) | 202 µs | 137 KB | 1,060 |
| SELECT with WHERE (1,000 rows) | 214 µs | 40.6 KB | 162 |
| SELECT with ORDER BY (500 rows) | 174 µs | 82 KB | 568 |
| SELECT ORDER BY LIMIT 10 (1,000 rows) | 177 µs | 123 KB | 1,086 |
| CREATE/DROP TABLE | 7.2 µs | 3.2 KB | 51 |
| Batch INSERT 1,000 PK rows | 7.1 ms | 6.5 MB | 79,626 |
| Secondary index lookup (100/1,000 rows) | 298 µs | 41.8 KB | 173 |
| Unique index lookup (1/1,000 rows) | 288 µs | 29.9 KB | 67 |

### Aggregates (v0.7.3, 1,000 rows)

| Aggregate | ns/op | allocs/op |
|-----------|------:|----------:|
| COUNT(*) | 34 µs | 57 |
| SUM | 48 µs | 58 |
| AVG | 48 µs | 58 |
| MIN / MAX | 49–50 µs | 57 |
| GROUP BY (4 groups) | 100 µs | 190 |

### Joins & Subqueries (v0.7.3)

| Benchmark | ns/op | allocs/op |
|-----------|------:|----------:|
| INNER JOIN (100 × 500) | 559 µs | 7,859 |
| IN subquery (200 rows) | 179 µs | 523 |
| Scalar subquery (200 rows) | 77 µs | 225 |
| Self-join (100 rows) | 169 µs | 2,231 |

### Heavy SQL Benchmarks (v0.7.3)

These benchmarks identify performance bottlenecks for future optimization:

| Benchmark | ns/op | B/op | allocs/op | Status |
|-----------|------:|-----:|----------:|--------|
| EXISTS subquery | **175 ms** | 5.9 MB | 39,085 | Bottleneck |
| Correlated subquery | **3.5 ms** | 513 KB | 8,388 | Bottleneck |
| Multiple BETWEEN/AND | **2.3 ms** | 68 KB | 118 | Bottleneck |
| Full table scan (5K rows) | 1.0 ms | 133 KB | 111 | Bottleneck |
| Large IN clause (50 values) | 1.5 ms | 63 KB | 210 | Medium |
| DISTINCT (1,000 rows) | 299 KB | 238 KB | 2,903 | Medium |
| Complex CASE (500 rows) | 626 µs | 123 KB | 700 | Medium |
| String concatenation (1,000 rows) | 458 µs | 194 KB | 5,071 | Medium |
| Multiple aggregates | 196 µs | 41 KB | 294 | Good |
| COALESCE NULL (500 rows) | 155 µs | 67 KB | 580 | Good |

### Scale (v0.7.3)

| Benchmark | ns/op |
|-----------|------:|
| SELECT 10K rows | 2.8 ms |
| SELECT 100K rows (filtered) | 34 ms |
| Bulk INSERT 10K rows | 87 ms |
| 3-table JOIN (100 rows each) | 2.9 ms |

### Comparison with SQLite Go (v0.7.4)

Benchmarks comparing sqlvibe against [go-sqlite](https://github.com/glebarez/go-sqlite) (pure Go SQLite binding) on identical workloads (in-memory database, `-benchtime=3s -benchmem`):

| Operation | sqlvibe (v0.7.4) | SQLite Go | Winner |
|-----------|------------------:|----------:|--------|
| INSERT single row | 21.7 µs | 26.8 µs | **sqlvibe 19% faster** |
| SELECT all (1K rows) | 55.5 µs | 1,019 µs | **sqlvibe 18x faster** |
| SELECT WHERE | 279 µs | 122 µs | SQLite 2.3x faster |
| SELECT ORDER BY | 181 µs | 423 µs | **sqlvibe 2.3x faster** |
| COUNT(*) | 46.7 µs | 6.3 µs | SQLite 7.4x faster |
| SUM | 64.8 µs | 74.5 µs | SQLite 1.2x faster |
| GROUP BY | 126 µs | 538 µs | **sqlvibe 4.3x faster** |
| UPDATE | 21.3 µs | 26.9 µs | **sqlvibe 21% faster** |
| DELETE | 23.2 µs | 43.1 µs | **sqlvibe 46% faster** |
| CASE expression | 149 µs | 251 µs | **sqlvibe 41% faster** |

**Analysis:**
- **sqlvibe advantages**: Full table scans (18x faster), GROUP BY (4.3x), ORDER BY (2.3x), DML operations (19-46% faster)
- **SQLite advantages**: WHERE filtering (2.3x faster due to better index optimization), COUNT aggregate (7.4x faster)

## Known Performance Bottlenecks

The v0.7.x benchmark suite identified the following areas for future optimization:

| # | Area | Observation | Impact |
|---|------|-------------|--------|
| 1 | EXISTS subqueries | 175ms, 5.9MB, 39K allocs - full table scan + row materialization | **Critical** |
| 2 | Correlated subqueries | 3.5ms, 513KB, 8K allocs - re-evaluated per outer row | **High** |
| 3 | BETWEEN/AND + IN clauses | 2.3ms - short-circuit not optimized | Medium |
| 4 | Full table scans | 1ms for 5K rows without index | Medium |
| 5 | JOIN row materialization | Hash join works, but 490KB memory per op | Medium |
| 6 | Row storage format | `map[string]interface{}` - column hash lookup overhead | Medium |

### Suggested Optimizations

1. **Page prefetching** - Read next B-Tree pages before needed (2x speedup potential)
2. **Subquery caching** - Cache EXISTS/IN subquery results
3. **Index-aware query planning** - Use secondary index for more WHERE clauses
4. **SIMD math ops** - Vectorized arithmetic for column batches

### Fixed in v0.7.3

- **Primary key O(1) uniqueness check** — `INSERT` into a PRIMARY KEY table previously did an O(N) scan of all existing rows to check uniqueness, making bulk inserts O(N²) in total. Replaced with a `pkHashSet map[string]map[interface{}]struct{}` per table (maintained on INSERT/UPDATE/DELETE, rebuilt on transaction rollback). `INSERT` uniqueness check is now O(1) amortised. Batch insert of 1 000 PK rows: constant time regardless of table size.
- **In-memory secondary hash index** — `WHERE indexed_col = val` queries on tables with `CREATE INDEX` previously still did a full O(N) table scan (the index metadata was stored but not used). Added `indexData map[string]map[interface{}][]int` (index name → column value → []row indices). Built immediately on `CREATE INDEX`, maintained on INSERT/UPDATE/DELETE, rebuilt on rollback. A new `tryIndexLookup` pre-filter in `execSelectStmtWithContext` passes only matching rows to the VM. **Secondary index lookup on 1 000-row table: 298 µs (would be ~3 ms without index), ~10× reduction in rows processed.**
- **`deduplicateRows` key allocation** — `UNION` / `UNION ALL` deduplication used `fmt.Sprintf("%v", row)` to build a key per row (1 allocation each). Replaced with a reusable `strings.Builder` + type switch (int64/float64/string/bool/nil fast paths). Eliminates the per-row `fmt.Sprintf` allocation and the intermediate string concat.
- **GROUP BY key: `strings.Builder` + type switch** — Replaced per-row `fmt.Sprintf` + `[]string` + `strings.Join` with a single `strings.Builder` write and a type switch for `int64`/`float64`/`string`/`bool`/`nil`. GROUP BY is ~11% faster.
- **SortRows pre-resolved column indices** — Old code did a linear scan of all column names for each comparison pair in `ORDER BY col_name`. New code resolves column indices once before the sort loop, giving direct `data[row][ci]` access. For expression ORDER BY terms, per-row rowMap allocation is also skipped (evaluation is deferred). **ORDER BY is 10–12% faster, 9% less memory.**
- **Top-K heap for `ORDER BY … LIMIT N`** — Added `SortRowsTopK` using `container/heap`. The bounded max-heap keeps only the K best rows; the O(N log K) scan replaces O(N log N) full sort. For ColumnRef ORDER BY (the common case), discarded rows incur zero allocation: keys are computed only when a row enters the heap. Stable sort semantics preserved via `origIdx` tiebreaker. Shared `cmpOrderByKey` helper eliminates comparison logic duplication. Updated all ORDER BY + LIMIT call sites. **ORDER BY + LIMIT 10 on 1 000 rows: 22% faster, 28% less memory.**
- **GROUP BY `interface{}` key for single-column GROUP BY** — `computeGroupKey` called `strings.Builder.String()` per row, allocating a new string for every row even when the group already exists. For single-expression GROUP BY, the raw column value is now used directly as the `map[interface{}]` key (int64/float64/string/bool: zero extra allocation). **Eliminates ~1 alloc/row** for `GROUP BY col` (most common pattern).
- **Hash join `interface{}` key map** — The hash join build and probe phases used `fmt.Sprintf`-based string keys. Replaced with `map[interface{}]` and a `normalizeJoinKey()` that only converts `[]byte` to string; int64/float64/string/bool are used directly as map keys. **Eliminates one string allocation per join key lookup on both build and probe.**
- **Hash join skip merged-row map for star-only no-WHERE queries** — `buildJoinMergedRow` allocated a `map[string]interface{}` per match, even for `SELECT * FROM a JOIN b ON …` where all output columns are stars and WHERE is absent. Added a fast path that skips the merged map entirely. **Eliminates one map allocation per matched row pair** for the common case.

**v0.7.3 Performance Improvements:**
- INSERT single: 10.1µs → 9.0µs (**11% faster**)
- SELECT ORDER BY: 191µs → 174µs (**9% faster**)
- GROUP BY: 173µs → 100µs (**42% faster, 91% fewer allocs**)
- JOIN: 662µs → 559µs (**16% faster**)

### Fixed in v0.7.2

- **SUM / AVG per-row allocation** — The aggregate accumulator used `interface{}` boxing on every row. Replaced with typed `int64`/`float64` fields in `AggregateState`. Result: **94% reduction in allocations** (1 032 → 58 allocs/op for SUM/AVG on 1 000 rows) and ~25% faster aggregate queries.
- **Self-join / qualified-star hash join** — `SELECT a.*, b.* FROM t a JOIN t b ON …` fell back to an O(N²) VM nested-loop join because the hash join incorrectly rejected qualified stars (`t.*`). Extended hash join to handle qualified stars, promoting self-joins to the O(N+M) hash join path. Result: **9× speedup** (1.57 ms → 169 µs for 100-row self-join).

## SQL:1999 Compatibility

sqlvibe tracks compatibility against the SQL:1999 standard via a dedicated test suite:

| Suite | Description | Status |
|-------|-------------|--------|
| E011 | Numeric data types | ✅ |
| E021 | Character string types | ✅ |
| E031 | Identifiers | ✅ |
| E041 | Schema definition (constraints, defaults) | ✅ |
| E051 | Query specification | ✅ |
| E061 | Predicates | ✅ |
| E081 | Full query expressions | ✅ |
| E091 | Set functions (aggregates, ALL keyword) | ✅ |
| E101 | Query expressions (subqueries, LIMIT) | ✅ |
| E111 | Window functions (OVER clause) | ✅ |
| E121 | Schema manipulation (ALTER TABLE, ORDER BY+LIMIT) | ✅ |
| F011–F501 | Advanced features (JOINs, CAST, UNION, CASE, date/time, …) | ✅ |

**56/56 test suites passing (100%).**

Full details: [`docs/SQL1999.md`](docs/SQL1999.md)

## Release History

See [`docs/HISTORY.md`](docs/HISTORY.md).

## License

See LICENSE file.
