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

### Core Operations

| Benchmark | ns/op | MB/op | allocs/op |
|-----------|------:|------:|----------:|
| INSERT single row (PK table) | 10.1 µs | 6.6 KB | 84 |
| UPDATE single row | 18.6 µs | 6.1 KB | 65 |
| DELETE single row | 17.4 µs | 6.2 KB | 71 |
| SELECT all (1 000 rows) | 215 µs | 133 KB | 1 060 |
| SELECT with WHERE (1 000 rows) | 224 µs | 39.6 KB | 162 |
| SELECT with ORDER BY (500 rows) | 191 µs | 80.3 KB | 570 |
| SELECT ORDER BY LIMIT 10 (1 000 rows) | 205 µs | 120 KB | 1 089 |
| CREATE/DROP TABLE | 6.4 µs | 3.2 KB | 51 |
| 1 000 INSERT batch (PRIMARY KEY table) | 8.1 ms | 6.4 MB | 79 632 |
| Secondary index lookup (100/1 000 rows) | 298 µs | 41.8 KB | 173 |
| Unique index lookup (1/1 000 rows) | 288 µs | 29.9 KB | 67 |

### Aggregates (1 000 rows)

| Aggregate | ns/op | allocs/op |
|-----------|------:|----------:|
| COUNT(*) | 26.7 µs | 57 |
| SUM | 41.0 µs | 58 |
| AVG | 40.8 µs | 58 |
| MIN / MAX | 41–42 µs | 57 |
| GROUP BY (4 groups, 1 000 rows) | 173 µs | 2 191 |

### Joins & Subqueries

| Benchmark | ns/op | allocs/op |
|-----------|------:|----------:|
| INNER JOIN (100 users × 500 orders) | 662 µs | 8 459 |
| IN subquery (200 rows) | 189 µs | 523 |
| Scalar subquery (200 rows) | 80 µs | 225 |
| 3-level nested subquery (100 rows) | 312 µs | 1 639 |
| Self-join (100 rows) | 169 µs | 2 231 |

### QP Layer (parser, no VM)

| Benchmark | ns/op | allocs/op |
|-----------|------:|----------:|
| Tokenize (10-token query) | 1.22 µs | 13 |
| Parse simple SELECT | 0.60 µs | 9 |
| Parse complex query (JOIN/GROUP/HAVING) | 2.01 µs | 30 |
| AST build (4-statement batch) | 6.82 µs | 80 |

### Scale

| Benchmark | ns/op |
|-----------|------:|
| SELECT 10 K rows | 2.78 ms |
| SELECT 100 K rows (filtered) | 34 ms |
| Bulk INSERT 10 K rows | 87 ms |
| 3-table JOIN (100 rows each) | 2.91 ms |

## Known Performance Bottlenecks

The v0.7.x benchmark suite identified the following areas for future optimization:

| # | Area | Observation | Impact |
|---|------|-------------|--------|
| 1 | JOIN row materialization | Hash join (equi-join) works well, but all rows are copied into memory before joining — streaming row evaluation would reduce peak allocation | Medium |
| 2 | Row storage format | Rows stored as `map[string]interface{}` (hash maps); switching to `[]interface{}` indexed by column position would eliminate per-column hash lookup overhead | High |

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
