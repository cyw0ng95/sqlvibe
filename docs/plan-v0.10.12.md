# Plan v0.10.12 - Auto Vacuum + VM Refactoring

## Summary

Implement auto vacuum and incremental vacuum, refactor VM execution into subpackage.

## Background

### Current State
- PRAGMA auto_vacuum: Implemented but basic
- PRAGMA incremental_vacuum: Not implemented
- vm_exec.go: 2270 lines (too large)

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
    └── cursor.go        # Cursor management
```

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

**Total New Tests**: ~40

---

## 4. Implementation Order

1. Create `pkg/sqlvibe/vm/` subpackage
2. Move code to subpackage files
3. Add vm/*_test.go files
4. Implement incremental vacuum
5. Add storage PRAGMAs
6. Run all tests
7. Commit

---

## 5. Success Criteria

- [ ] Incremental vacuum implemented
- [ ] Auto vacuum enhanced
- [ ] Storage PRAGMAs working
- [ ] vm/ subpackage created
- [ ] vm/*_test.go added (~40 tests)
- [ ] All tests pass
