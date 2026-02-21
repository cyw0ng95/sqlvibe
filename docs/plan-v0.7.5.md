# Plan v0.7.5 - SQLLogicTest Integration

## Summary

Integrate sqllogictest to enable comprehensive black-box testing against real SQLite behavior.

**Previous**: v0.7.4 delivers performance optimizations

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
| 1 | Integrate sqllogictest runner | Pending |
| 2 | Run existing SQLite test cases | Pending |
| 3 | Fix compatibility issues | Pending |
| 4 | Add custom test cases | Pending |

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
- [ ] Add sqllogictest Go dependency
- [ ] Create SQLvibe driver implementation
- [ ] Set up test runner

### Phase 2: Basic Tests
- [ ] Run DDL tests (CREATE, DROP, ALTER)
- [ ] Run DML tests (INSERT, UPDATE, DELETE)
- [ ] Run basic SELECT tests

### Phase 3: Advanced Tests
- [ ] Run JOIN tests
- [ ] Run aggregate tests
- [ ] Run subquery tests

### Phase 4: Compatibility Fixes
- [ ] Fix failing tests
- [ ] Document known differences
- [ ] Add workarounds

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

- [ ] 80%+ of SQLite sqllogictests pass
- [ ] All basic SELECT/INSERT/UPDATE/DELETE work correctly
- [ ] JOIN and aggregate queries match SQLite
- [ ] Test runner integrated into CI

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
