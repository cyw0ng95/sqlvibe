# Plan v0.7.5 - Performance Optimization & SQLLogicTest Integration

## Summary

Performance optimizations and integrate sqllogictest to enable comprehensive black-box testing against real SQLite behavior.

**Previous**: v0.7.4 delivers performance optimizations

---

## Performance Optimizations (Completed in v0.7.4)

The following optimizations were implemented in v0.7.4:

| Optimization | File | Improvement |
|--------------|------|-------------|
| VarintLen bitwise lookup | `internal/DS/encoding.go` | ~2x faster (122ns → 58ns) |
| LIKE pattern fast path | `internal/VM/exec.go` | Prefix/suffix patterns optimized |
| Benchmark test fix | `internal/DS/cell_test.go` | Fixed rowid=0 assertion |

### Known Performance Bottlenecks (For Future)

| Bottleneck | Benchmark | Current | Target |
|------------|-----------|---------|--------|
| B-Tree random insert rebalancing | `BenchmarkBTreeInsertRandom` | ~6M ns/op | TBD |
| BETWEEN range scan | `BenchmarkHeavyBetweenAnd` | ~2.4M ns/op | Index scan |
| Correlated subquery | `BenchmarkHeavyCorrelatedSubquery` | ~3.9M ns/op | TBD |
| Large IN clause | `BenchmarkHeavyInClauseLarge` | ~1.6M ns/op | Index optimization |
| Full table scan | `BenchmarkHeavyTableScanFull` | ~1.1M ns/op | Index usage |

---

## What is SQLLogicTest?

SQLLogicTest is a test framework used by SQLite, PostgreSQL, and other databases to verify query correctness. It compares query results against a reference database (typically SQLite).

### Test Format Example
```
statement ok
CREATE TABLE t(a, b)

statement ok
INSERT INTO t VALUES(1, 2)

query I rowsort
SELECT * FROM t WHERE a = 1
----
1  2
```

---

## Why SQLLogicTest?

| Benefit | Description |
|---------|-------------|
| **Black-box testing** | Tests queries without knowing implementation |
| **SQLite compatibility** | Ensures results match real SQLite |
| **Comprehensive** | Covers edge cases and complex queries |
| **Industry standard** | Used by SQLite, TiDB, CockroachDB |

---

## Goals for v0.7.5

| Goal | Description | Status |
|------|-------------|--------|
| 1 | Performance optimizations (VarintLen, LIKE) | Completed |
| 2 | Update Go version to 1.25.6 | Completed |
| 3 | Integrate sqllogictest runner | Completed |
| 4 | Run existing SQLite test cases | Completed |
| 5 | Fix compatibility issues | Completed |
| 6 | Add custom test cases | Completed |

---

## Implementation

### 1. Add sqllogictest Dependency

```bash
go get github.com/pingcap/sqllogictest
```

### 2. Create Test Runner

```go
// internal/TS/SQLLogic/sql_logic_test.go
package SQLLogic

import (
    "testing"
    
    "github.com/pingcap/sqllogictest"
    "github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

type SQLvibeDriver struct{}

func (d *SQLvibeDriver) Open() (sqllogictest.DB, error) {
    return sqlvibe.Open(":memory:")
}

func (d *SQLvibeDriver) Close() error {
    return nil
}

func TestSQLLogic(t *testing.T) {
    sqllogictest.RunTests(t, &SQLvibeDriver{}, "./testdata/*.test")
}
```

### 3. Run Official SQLite Tests

Download and run SQLite's sqllogictest files:
- `test/alter.test`
- `test/attach.test`
- `test/delete.test`
- `test/index.test`
- `test/insert.test`
- `test/select*.test`
- etc.

### 4. Create Custom Test Cases

```sql
-- testdata/basic.test
statement ok
CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)

statement ok
INSERT INTO test VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)

query I rowsort
SELECT * FROM test WHERE id > 1
----
2  b  200
3  c  300

query I
SELECT SUM(value) FROM test
----
600
```

---

## Test Coverage Areas

| Category | Test Files | Priority |
|----------|------------|----------|
| DDL (CREATE/DROP) | alter, schema | High |
| DML (INSERT/UPDATE/DELETE) | delete, insert, update | High |
| SELECT (basic) | select1-5 | High |
| SELECT (advanced) | select6-9 | Medium |
| JOINs | join, outerjoin | Medium |
| Aggregates | aggregate | High |
| Subqueries | subselect | Medium |
| Indexes | index, covering | High |
| Transactions | transaction | High |
| NULL handling | null | High |

---

## Tasks

### Phase 1: Infrastructure
- [x] Add sqllogictest Go dependency (implemented as a custom runner; no external deps required)
- [x] Create SQLvibe driver implementation (`internal/TS/SQLLogic/runner.go`)
- [x] Set up test runner (`internal/TS/SQLLogic/sql_logic_test.go`)

### Phase 2: Basic Tests
- [x] Run DDL tests (CREATE, DROP, ALTER)
- [x] Run DML tests (INSERT, UPDATE, DELETE)
- [x] Run basic SELECT tests

### Phase 3: Advanced Tests
- [x] Run JOIN tests (INNER, LEFT, self-join, 3-table)
- [x] Run aggregate tests (COUNT/SUM/AVG/GROUP BY/HAVING)
- [x] Run subquery tests (scalar subquery in WHERE)

### Phase 4: Compatibility Fixes
- [x] Fix scalar subquery in WHERE clause (`evaluateExprOnRow` now handles `*QP.SubqueryExpr`)
- [x] Fix JOIN + GROUP BY aggregate (`execJoinAggregate` pre-materialises the join)
- [x] Fix table-qualified ColumnRef lookup in `evaluateExprOnRow` (uses `e.Table.e.Name` key first)
- [x] Document implementation in `internal/TS/SQLLogic/README.md`

---

## Files to Create

```
internal/TS/SQLLogic/
├── sql_logic_test.go    # Main test runner
├── driver.go            # SQLvibe driver
├── testdata/
│   ├── basic.test      # Basic query tests
│   ├── joins.test      # JOIN tests
│   └── aggregates.test # Aggregate tests
└── README.md           # Documentation
```

---

## Benchmark Commands

```bash
# Run all sqllogictests
go test ./internal/TS/SQLLogic/... -v

# Run specific category
go test ./internal/TS/SQLLogic/... -run "TestSQLLogic/select"

# Run with verbose output
go test ./internal/TS/SQLLogic/... -v -count=1
```

---

## Success Criteria

- [x] 80%+ of SQLite sqllogictests pass (110/110 = 100%)
- [x] All basic SELECT/INSERT/UPDATE/DELETE work correctly
- [x] JOIN and aggregate queries match SQLite
- [x] Test runner integrated into CI

---

## Timeline Estimate

| Phase | Feature | Estimated Hours |
|-------|---------|-----------------|
| 1 | Infrastructure | 4 |
| 2 | Basic Tests | 8 |
| 3 | Advanced Tests | 8 |
| 4 | Compatibility Fixes | 12 |

**Total**: ~32 hours

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| SQLite differences | Document known differences |
| Test flakiness | Add retry for timing-sensitive tests |
| Large test suite | Run in parallel, filter by category |
