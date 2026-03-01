# Plan v0.10.11 - Transaction + SetOps + DDL Enhancements

## Summary

Implement transaction rollback, set operations, DDL enhancements, and refactor database into subpackage.

## Background

### Existing TODOs
- Transaction Rollback: 1 item (actual rollback logic)
- SetOps Compilation: 1 item (full VM bytecode compilation)

### Current State
- database.go: 5063 lines (too large)
- ROLLBACK: Implemented but incomplete
- UNION/INTERSECT/EXCEPT: Partial support
- ALTER TABLE: Limited support
- PRAGMA: Basic support

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

### 1.3 ALTER TABLE Enhancements

| Feature | Description |
|---------|-------------|
| RENAME COLUMN | ALTER TABLE t RENAME COLUMN old TO new |
| DROP COLUMN | ALTER TABLE t DROP COLUMN col |
| ADD CONSTRAINT | ALTER TABLE t ADD CONSTRAINT name CHECK/UNIQUE/FK |
| RENAME INDEX | ALTER TABLE t RENAME INDEX old TO new |

**Syntax:**
```sql
ALTER TABLE users RENAME COLUMN name TO full_name;
ALTER TABLE users DROP COLUMN middle_name;
ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);
ALTER TABLE t RENAME INDEX idx_old TO idx_new;
```

### 1.4 PRAGMA Enhancements

| PRAGMA | Description |
|--------|-------------|
| PRAGMA table_info(t) | Detailed table column info (type, nullable, default, pk) |
| PRAGMA table_list | List all tables with type, schema |
| PRAGMA index_xinfo(idx) | Detailed index column info (seq, cid, name, desc, coll) |
| PRAGMA foreign_key_check | Check for FK violations |

**Output Format:**
```sql
PRAGMA table_info(users);
-- cid, name, type, notnull, dflt_value, pk

PRAGMA table_list;
-- seq, name, type, ncol, root, wr

PRAGMA index_xinfo(idx);
-- seqno, cid, name, desc, coll, key
```

### 1.5 Index Management

| Feature | Description |
|---------|-------------|
| DROP INDEX IF EXISTS | Safe index deletion |
| INDEXED BY | Force index usage hint |
| INDEXES ON table | List all indexes for table |

**Syntax:**
```sql
DROP INDEX IF EXISTS idx_name;
SELECT * FROM t INDEXED BY idx_name WHERE x > 5;
PRAGMA index_list(t);
```

### 1.6 Constraint Validation

| Feature | Description |
|---------|-------------|
| PRAGMA foreign_key_check | Report all FK violations |
| PRAGMA quick_check | Fast table integrity check |
| ON CONFLICT | Conflict resolution strategies |

**Syntax:**
```sql
PRAGMA foreign_key_check;
PRAGMA quick_check;
INSERT INTO t VALUES(...) ON CONFLICT(x) DO NOTHING;
INSERT INTO t VALUES(...) ON CONFLICT(x) DO UPDATE SET y=excluded.y;
```

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
    ├── meta.go           # Metadata operations
    └── constraint.go     # Constraint checking
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
| database/pragma_test.go | PRAGMA enhancements | ~8 |
| database/alter_test.go | ALTER TABLE | ~8 |
| database/index_test.go | Index management | ~6 |

**Total New Tests**: ~62

---

## 4. Implementation Order

1. Create `pkg/sqlvibe/database/` subpackage
2. Move code to subpackage files
3. Add database/*_test.go files
4. Implement Transaction Rollback
5. Implement SetOps full compilation
6. Implement ALTER TABLE enhancements
7. Implement PRAGMA enhancements
8. Implement Index Management
9. Implement Constraint Validation
10. Run all tests
11. Commit

---

## 5. Success Criteria

- [ ] Transaction rollback implemented
- [ ] Savepoints working
- [ ] SetOps (UNION/INTERSECT/EXCEPT) fully implemented
- [ ] ALTER TABLE RENAME/DROP/ADD CONSTRAINT working
- [ ] PRAGMA table_info, table_list, index_xinfo, foreign_key_check working
- [ ] DROP INDEX IF EXISTS working
- [ ] ON CONFLICT clause working
- [ ] database/ subpackage created
- [ ] database/*_test.go added (~62 tests)
- [ ] All tests pass
- [ ] Coverage: pkg/sqlvibe → 45%
