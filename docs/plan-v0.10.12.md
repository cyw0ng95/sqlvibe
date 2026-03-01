# Plan v0.10.12 - Auto Vacuum + Query Execution Optimization

## Summary

Implement auto vacuum, query execution optimizations, and refactor VM execution into subpackage.

## Background

### Current State
- PRAGMA auto_vacuum: Implemented but basic
- PRAGMA incremental_vacuum: Not implemented
- vm_exec.go: 2270 lines (too large)
- Query execution: Basic implementation, room for optimization

---

## 1. Features

### 1.1 Auto Vacuum

| Feature | Description |
|---------|-------------|
| Full Auto Vacuum | Automatic vacuum on commit |
| Incremental Vacuum | PRAGMA incremental_vacuum(N) |
| Page Size Config | PRAGMA page_size on create |
| Vacuum Statistics | Detailed vacuum status |

### 1.2 Storage Enhancements

| Feature | Description |
|---------|-------------|
| PRAGMA freelist_count | Free pages count |
| PRAGMA page_count | Total pages |
| PRAGMA optimize | Run ANALYZE automatically |

### 1.3 Query Execution Optimization

| Optimization | Description |
|--------------|-------------|
| Predicate Pushdown | Push WHERE conditions to storage layer |
| Column Pruning | Only read required columns from storage |
| Short-circuit Eval | Stop evaluation when result is determined |
| Expression Caching | Cache computed expression results |

**Implementation:**
```sql
-- Before: Filter in VM layer
SELECT * FROM (SELECT * FROM t WHERE x > 10) WHERE x < 100;

-- After: Push both conditions to storage
SELECT * FROM t WHERE x > 10 AND x < 100;
```

**Benefits:**
- Reduce data transfer from storage to VM
- Minimize memory allocations
- Faster query execution

---

## 2. Refactoring

### Goal
Refactor `pkg/sqlvibe/vm_exec.go` (2270 lines) into subpackage

### Structure

```
pkg/sqlvibe/
├── vm_exec.go            # Main entry, ~250 lines
└── vm/                  # Subpackage
    ├── select.go        # SELECT execution
    ├── insert.go        # INSERT execution
    ├── update.go        # UPDATE execution
    ├── delete.go        # DELETE execution
    ├── aggregate.go     # Aggregate execution
    ├── cursor.go       # Cursor management
    └── optimize.go     # Query optimization
```

### Code-Level Optimizations

| Optimization | Target | Description |
|--------------|--------|-------------|
| Function Inlining | Hot paths | Inline frequently called functions |
| Reduce Allocations | vm_exec.go | Reuse buffers, reduce string conversions |
| Type Switching | expression eval | Optimize type assertions |
| Slice Pre-allocation | result sets | Pre-allocate result slices |
| String Interning | column names | Reduce string duplication |

---

## 3. Tests

### Target Coverage
Improve execution layer test coverage

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| vm/select_test.go | SELECT execution | ~8 |
| vm/insert_test.go | INSERT execution | ~8 |
| vm/update_test.go | UPDATE execution | ~8 |
| vm/delete_test.go | DELETE execution | ~8 |
| vm/aggregate_test.go | Aggregates | ~8 |
| vm/optimize_test.go | Query optimization | ~6 |

**Total New Tests**: ~46

---

## 4. Implementation Order

1. Create `pkg/sqlvibe/vm/` subpackage
2. Move code to subpackage files
3. Add vm/*_test.go files
4. Implement incremental vacuum
5. Add storage PRAGMAs
6. Implement predicate pushdown
7. Implement column pruning
8. Implement expression caching
9. Apply code-level optimizations
10. Run all tests
11. Commit

---

## 5. Success Criteria

- [ ] Incremental vacuum implemented
- [ ] Auto vacuum enhanced
- [ ] Storage PRAGMAs working
- [ ] Predicate pushdown working
- [ ] Column pruning working
- [ ] Expression caching working
- [ ] Code-level optimizations applied
- [ ] vm/ subpackage created
- [ ] vm/*_test.go added (~46 tests)
- [ ] All tests pass
