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

# Format code
go fmt ./...

# Vet code
go vet ./...
```

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
