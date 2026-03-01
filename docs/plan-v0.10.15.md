# Plan v0.10.15 - CLI Tools + Context/Window Refactoring

## Summary

Enhance CLI tools and refactor context and window into subpackages.

## Background

### Current State
- vm_context.go: 940 lines
- window.go: 786 lines
- .dump: Basic implementation
- .import/.export: Basic implementation

---

## 1. Features

### 1.1 .dump Enhancements

| Feature | Description |
|---------|-------------|
| .dump --data-only | Dump data only |
| .dump --schema-only | Dump schema only |
| .dump --inserts | INSERT statements |

### 1.2 Import/Export Enhancements

| Feature | Description |
|---------|-------------|
| .import --csv | CSV import with options |
| .export --csv | CSV export |
| .export --json | JSON export |

### 1.3 Additional Functions

| Function | Description |
|----------|-------------|
| group_concat | Concatenate with group |
| any_value | Return any value in group |
| mode | Statistical mode |

---

## 2. Refactoring

### Goal
Refactor `vm_context.go` and `window.go` into subpackages

### Structure

**vm_context (940 lines)**
```
pkg/sqlvibe/
├── context.go            # Main entry, ~150 lines
└── context/              # Subpackage
    ├── eval.go           # Expression evaluation
    ├── aggregate.go      # Aggregate context
    └── check.go          # CHECK constraint
```

**window (786 lines)**
```
pkg/sqlvibe/
├── window.go             # Main entry, ~150 lines
└── window/              # Subpackage
    ├── func.go           # Window function impl
    ├── frame.go          # Window frame calc
    └── partition.go      # Partition handling
```

---

## 3. Tests

### Target Coverage
Improve context and window test coverage

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| context/eval_test.go | Expression eval | ~6 |
| context/aggregate_test.go | Aggregate ctx | ~6 |
| context/check_test.go | CHECK constraints | ~6 |
| window/func_test.go | Window functions | ~6 |
| window/frame_test.go | Window frames | ~6 |
| window/partition_test.go | Partitions | ~6 |

**Total New Tests**: ~36

---

## 4. Implementation Order

1. Create `pkg/sqlvibe/context/` subpackage
2. Create `pkg/sqlvibe/window/` subpackage
3. Move code to subpackage files
4. Add context/*_test.go files
5. Add window/*_test.go files
6. Enhance .dump and .import/.export
7. Add group_concat, any_value, mode
8. Run all tests
9. Commit

---

## 5. Success Criteria

- [x] .dump enhancements working
- [x] Import/export enhancements working
- [x] Additional functions implemented (any_value, mode)
- [x] context/ subpackage created
- [x] window/ subpackage created
- [x] context/*_test.go added (~18 tests)
- [x] window/*_test.go added (~18 tests)
- [x] All tests pass
