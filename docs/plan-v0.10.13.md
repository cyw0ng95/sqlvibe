# Plan v0.10.13 - Query Cache + Compiler Refactoring

## Summary

Implement query caching and refactor compiler into subpackage.

## Background

### Current State
- compiler.go: 1313 lines
- Prepared Statement: Not cached
- Query Plan: Not cached

---

## 1. Features

### 1.1 Prepared Statement Cache

| Feature | Description |
|---------|-------------|
| Statement Pool | Cache compiled statements |
| LRU Eviction | Least recently used eviction |
| Size Limit | Configurable cache size |

### 1.2 Query Plan Cache

| Feature | Description |
|---------|-------------|
| Plan Cache | Cache query execution plans |
| Plan Reuse | Reuse plans for similar queries |
| PRAGMA cache_plan | Enable/disable caching |

### 1.3 Cache Management

| PRAGMA | Description |
|--------|-------------|
| PRAGMA cache_size | Pages per database cache |
| PRAGMA cache_spill | Threshold to spill cache |
| PRAGMA shrink_memory | Release unused memory |

---

## 2. Refactoring

### Goal
Refactor `internal/CG/compiler.go` (1313 lines) into subpackage

### Structure

```
internal/CG/
├── compiler.go           # Main entry, ~250 lines
└── compiler/             # Subpackage
    ├── select.go         # SELECT compilation
    ├── dml.go            # INSERT/UPDATE/DELETE compilation
    ├── aggregate.go      # Aggregate compilation
    ├── window.go         # Window function compilation
    ├── subquery.go       # Subquery compilation
    └── cte.go            # CTE compilation
```

---

## 3. Tests

### Target Coverage
Current: 27.7% → Target: 50%

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| compiler/select_test.go | SELECT compilation | ~7 |
| compiler/dml_test.go | DML compilation | ~7 |
| compiler/aggregate_test.go | Aggregate plans | ~7 |
| compiler/window_test.go | Window plans | ~7 |
| compiler/subquery_test.go | Subquery plans | ~7 |

**Total New Tests**: ~35

---

## 4. Implementation Order

1. Create `internal/CG/compiler/` subpackage
2. Move code to subpackage files
3. Add compiler/*_test.go files
4. Implement statement cache
5. Implement plan cache
6. Run all tests
7. Commit

---

## 5. Success Criteria

- [x] Prepared statement cache implemented
- [x] Query plan cache implemented
- [x] Cache PRAGMAs working
- [x] compiler/ subpackage created
- [x] compiler/*_test.go added (~64 tests)
- [x] All tests pass
- [ ] Coverage: internal/CG → 50%
