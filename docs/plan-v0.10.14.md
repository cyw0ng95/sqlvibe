# Plan v0.10.14 - Memory Optimization + Engine Refactoring

## Summary

Implement memory optimizations and refactor query engine into subpackage.

## Background

### Current State
- query_engine.go: 2621 lines
- Arena: Basic implementation
- Memory mapping: Basic implementation

---

## 1. Features

### 1.1 Arena Enhancements

| Feature | Description |
|---------|-------------|
| Larger Pools | Increase arena pool sizes |
| GC Reduction | Minimize GC pressure |
| Zero-Allocation | Reduce allocations in hot paths |

### 1.2 Memory Mapping

| Feature | Description |
|---------|-------------|
| PRAGMA mmap_size | Memory-mapped I/O size |
| mmap Optimization | Optimize mmap thresholds |

### 1.3 Memory Statistics

| PRAGMA | Description |
|--------|-------------|
| PRAGMA memory_status | Detailed memory usage |
| PRAGMA heap_limit | Maximum heap size |
| PRAGMA temp_store | Temp storage location |

---

## 2. Refactoring

### Goal
Refactor `internal/VM/query_engine.go` (2621 lines) into subpackage

### Structure

```
internal/VM/
├── query_engine.go       # Main entry, ~300 lines
└── engine/               # Subpackage
    ├── select.go         # SELECT engine
    ├── aggregate.go      # Aggregate engine
    ├── sort.go           # Sort engine
    ├── join.go           # Join engine
    ├── window.go         # Window function engine
    └── subquery.go       # Subquery engine
```

---

## 3. Tests

### Target Coverage
Improve engine layer test coverage

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| engine/select_test.go | SELECT engine | ~7 |
| engine/aggregate_test.go | Aggregate engine | ~7 |
| engine/sort_test.go | Sort operations | ~7 |
| engine/join_test.go | Join operations | ~7 |
| engine/window_test.go | Window engine | ~7 |

**Total New Tests**: ~35

---

## 4. Implementation Order

1. Create `internal/VM/engine/` subpackage
2. Move code to subpackage files
3. Add engine/*_test.go files
4. Implement arena enhancements
5. Optimize memory mapping
6. Run all tests
7. Commit

---

## 5. Success Criteria

- [x] Arena memory optimizations implemented
- [x] Memory mapping optimized
- [x] Memory statistics enhanced
- [x] engine/ subpackage created
- [x] engine/*_test.go added (~40 tests)
- [x] All tests pass
