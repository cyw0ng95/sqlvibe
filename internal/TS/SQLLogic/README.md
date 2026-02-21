# SQLLogicTest Integration

This package provides a black-box test runner that validates sqlvibe query
results against the [SQLLogicTest](https://www.sqlite.org/sqllogictest/) format
used by SQLite, PostgreSQL, TiDB, CockroachDB and others.

## Test Format

Each `.test` file contains records separated by blank lines.

### Statement record

```
statement ok
CREATE TABLE t(a INTEGER, b TEXT)

statement error
INVALID SQL THAT SHOULD FAIL
```

### Query record

```
query IT rowsort
SELECT a, b FROM t ORDER BY a
----
1  hello
2  world
```

**Type characters** (one per result column):

| Char | Meaning |
|------|---------|
| `I`  | Integer |
| `T`  | Text    |
| `R`  | Real    |

**Sort modes** (optional, after type string):

| Mode        | Description                          |
|-------------|--------------------------------------|
| `rowsort`   | Sort rows before comparing           |
| `valuesort` | Sort all individual values           |
| *(omitted)* | Compare in result order (`nosort`)   |

## Running Tests

```bash
# Run all SQLLogicTest files
go test ./internal/TS/SQLLogic/... -v

# Run a specific file category
go test ./internal/TS/SQLLogic/... -run "TestSQLLogic/basic"
go test ./internal/TS/SQLLogic/... -run "TestSQLLogic/joins"
go test ./internal/TS/SQLLogic/... -run "TestSQLLogic/aggregates"
```

## Adding Test Files

Drop any `.test` file into `testdata/` and it will be picked up automatically
by `TestSQLLogic`.  The runner creates a fresh in-memory sqlvibe database per
file, so files are fully isolated.

## File Structure

```
internal/TS/SQLLogic/
├── runner.go            # Parser + runner (no external dependencies)
├── sql_logic_test.go    # Test entry point
├── testdata/
│   ├── basic.test       # DDL, DML, basic SELECT, NULL, DISTINCT, LIKE
│   ├── joins.test       # INNER/LEFT/self/3-table JOINs
│   └── aggregates.test  # COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING
└── README.md            # This file
```
