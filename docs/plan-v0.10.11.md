# Plan v0.10.11 - Transaction + SetOps

## Summary

Implement transaction rollback and complete set operations, refactor database into subpackage.

## Background

### Existing TODOs
- Transaction Rollback: 1 item (actual rollback logic)
- SetOps Compilation: 1 item (full VM bytecode compilation)

### Current State
- database.go: 5063 lines (too large)
- ROLLBACK: Implemented but incomplete
- UNION/INTERSECT/EXCEPT: Partial support

---

## 1. Features

### 1.1 Transaction Rollback

| Feature | Description |
|---------|-------------|
| Actual Rollback | Implement real rollback logic |
| Savepoints | SAVEPOINT/RELEASE/ROLLBACK TO |
| Nested Transactions | Support nested transaction blocks |

### 1.2 Set Operations

| Operation | Description |
|-----------|-------------|
| UNION | Union of two queries |
| UNION ALL | Union with duplicates |
| INTERSECT | Intersection of queries |
| EXCEPT | Difference of queries |
| Full Compilation | Complete VM bytecode compilation |

---

## 2. Refactoring

### Goal
Refactor `pkg/sqlvibe/database.go` (5063 lines) into subpackage

### Structure

```
pkg/sqlvibe/
├── database.go            # Main entry, ~400 lines
└── database/              # Subpackage
    ├── ddl.go             # CREATE/DROP/ALTER TABLE
    ├── dml.go             # INSERT/UPDATE/DELETE
    ├── query.go           # SELECT queries
    ├── transaction.go     # Transaction management
    ├── prepare.go         # Statement preparation
    ├── meta.go            # Metadata operations
    └── constraint.go      # Constraint checking
```

---

## 3. Tests

### Target Coverage
Current: 23.6% → Target: 45%

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| database/ddl_test.go | DDL operations | ~10 |
| database/dml_test.go | DML operations | ~10 |
| database/query_test.go | Query execution | ~10 |
| database/transaction_test.go | Transactions | ~10 |
| database/constraint_test.go | Constraints | ~10 |

**Total New Tests**: ~50

---

## 4. Implementation Order

1. Create `pkg/sqlvibe/database/` subpackage
2. Move code to subpackage files
3. Add database/*_test.go files
4. Implement Transaction Rollback
5. Implement SetOps full compilation
6. Run all tests
7. Commit

---

## 5. Success Criteria

- [ ] Transaction rollback implemented
- [ ] Savepoints working
- [ ] SetOps (UNION/INTERSECT/EXCEPT) fully implemented
- [ ] database/ subpackage created
- [ ] database/*_test.go added (~50 tests)
- [ ] All tests pass
- [ ] Coverage: pkg/sqlvibe → 45%
