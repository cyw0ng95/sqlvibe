# Go to C++ Migration Progress

**Start Date**: 2026-03-03  
**Status**: In Progress - Phase 1: VM Layer

---

## Current Architecture

```
Go Application (pkg/sqlvibe, cmd/)
         ↓
Go Binding Layer (internal/cgo/) ← Target: ~400 LOC pure type mapping
         ↓ CGO (~5-260ns overhead)
C Public API (src/core/svdb/svdb.h)
         ↓
C++ Engine (src/core/)
```

---

## Phase 1: VM Layer Migration

### Status: IN PROGRESS

### Files to Migrate

| Go File | LOC | C++ Target | Status |
|---------|-----|------------|--------|
| `internal/VM/engine.go` | 278 | `src/core/VM/vm_execute.cpp` | TODO |
| `internal/VM/exec.go` | 5324 | `src/core/VM/exec.cpp` (exists) | TODO - Remove Go |
| `internal/VM/cursor.go` | ~200 | `src/core/VM/cursor.cpp` (exists) | TODO - Remove Go |
| `internal/VM/registers.go` | ~100 | `src/core/VM/registers.cpp` (exists) | TODO - Remove Go |
| `internal/VM/program.go` | ~150 | `src/core/VM/program.cpp` (exists) | TODO - Remove Go |
| `internal/VM/opcodes.go` | ~300 | `src/core/VM/opcodes.cpp` (exists) | TODO - Remove Go |
| `internal/VM/instruction.go` | ~80 | `src/core/VM/instruction.cpp` (exists) | TODO - Remove Go |
| `internal/VM/compare.go` | ~150 | `src/core/VM/compare.cpp` (exists) | TODO - Remove Go |
| `internal/VM/datetime.go` | ~400 | `src/core/VM/datetime.cpp` (exists) | TODO - Remove Go |
| `internal/VM/string_funcs.go` | ~300 | `src/core/VM/string_funcs.cpp` (exists) | TODO - Remove Go |
| `internal/VM/type_conv.go` | ~200 | `src/core/VM/type_conv.cpp` (exists) | TODO - Remove Go |
| `internal/VM/hash.go` | ~100 | `src/core/VM/hash.cpp` (exists) | TODO - Remove Go |
| `internal/VM/aggregate_funcs.go` | ~400 | `src/core/VM/aggregate.cpp` (exists) | TODO - Remove Go |
| `internal/VM/string_pool.go` | ~150 | `src/core/VM/string_pool.cpp` (exists) | TODO - Remove Go |
| `internal/VM/result_cache.go` | ~100 | `src/core/VM/result_cache.cpp` | TODO - NEW |
| `internal/VM/row_eval.go` | ~150 | `src/core/VM/row_eval.cpp` | TODO - NEW |
| `internal/VM/subquery_cache.go` | ~200 | `src/core/VM/subquery_cache.cpp` | TODO - NEW |

### Key Observations

1. **C++ already has implementations** for most VM opcodes and functions
2. **Go still orchestrates execution** - the main VM loop is in Go
3. **CGO wrappers exist** but call back to Go for orchestration

### Migration Plan

#### Step 1: Create C++ VM Execution Wrapper
- File: `src/core/VM/vm_execute.cpp` + `vm_execute.h`
- Expose: `svdb_vm_run(program, state, result)`
- Handle: Instruction fetch, decode, dispatch loop

#### Step 2: Extend C API
- File: `src/core/svdb/svdb.h`
- Add: `svdb_vm_execute()` function
- Return: Rows affected, result set, error code

#### Step 3: Update Go Wrapper
- File: `internal/cgo/exec_cgo.go`
- Change: Call C++ `svdb_vm_execute()` directly
- Remove: Go VM execution loop

#### Step 4: Remove Go Duplicates
- Delete: `internal/VM/engine.go`, `internal/VM/exec.go` (after migration)
- Keep: CGO wrappers as thin bindings

---

## Phase 2: DS Layer Migration

### Status: PENDING

| Go File | LOC | C++ Target | Status |
|---------|-----|------------|--------|
| `internal/DS/manager.go` | 182 | `src/core/DS/manager.cpp` (exists) | TODO |
| `internal/DS/btree.go` | 843 | `src/core/DS/btree.cpp` (exists) | TODO |
| `internal/DS/balance.go` | ~300 | `src/core/DS/balance.cpp` (exists) | TODO |
| `internal/DS/page.go` | ~200 | `src/core/DS/page.cpp` (exists) | TODO |
| `internal/DS/freelist.go` | ~250 | `src/core/DS/freelist.cpp` (exists) | TODO |
| `internal/DS/wal.go` | ~400 | `src/core/DS/wal.cpp` (exists) | TODO |
| `internal/DS/overflow.go` | ~300 | `src/core/DS/overflow.cpp` (exists) | TODO |
| `internal/DS/cache.go` | ~200 | `src/core/DS/cache.cpp` (exists) | TODO |
| `internal/DS/value.go` | ~250 | `src/core/DS/value.cpp` (exists) | TODO |
| `internal/DS/cell.go` | ~200 | `src/core/DS/cell.cpp` (exists) | TODO |
| `internal/DS/encoding.go` | ~300 | `src/core/DS/varint.cpp` (exists) | TODO |
| `internal/DS/columnar.go` | ~400 | `src/core/DS/columnar.cpp` (exists) | TODO |
| `internal/DS/row_store.go` | ~350 | `src/core/DS/row_store.cpp` (exists) | TODO |
| `internal/DS/hybrid_store.go` | ~300 | `src/core/DS/hybrid_store.cpp` (exists) | TODO |

**Critical Issue**: Index B-Tree page type mismatch
- Go uses: 0x02 (index-interior), 0x0a (index-leaf)
- C++ uses: 0x0a (index-interior), 0x02 (index-leaf) - SQLite canonical
- **Fix**: Update C++ to match Go OR update Go to match C++

---

## Phase 3: QP Layer Migration

### Status: PENDING

**Strategy**: Keep Go AST types as pure data structures, move processing to C++

| Go File | LOC | C++ Target | Status |
|---------|-----|------------|--------|
| `internal/QP/tokenizer.go` | ~500 | `src/core/QP/tokenizer.cpp` (exists) | TODO |
| `internal/QP/parser.go` | 585 | `src/core/QP/parser.cpp` (exists) | TODO |
| `internal/QP/analyzer.go` | ~300 | `src/core/QP/analyzer.cpp` (exists) | TODO |
| `internal/QP/binder.go` | ~400 | `src/core/QP/binder.cpp` (exists) | TODO |
| `internal/QP/optimizer.go` | ~350 | `src/core/QP/optimizer.cpp` | TODO - NEW |

---

## Phase 4: CG Layer Migration

### Status: PENDING

| Go File | LOC | C++ Target | Status |
|---------|-----|------------|--------|
| `internal/CG/compiler.go` | ~400 | `src/core/CG/compiler.cpp` (exists) | TODO |
| `internal/CG/bytecode_compiler.go` | ~500 | `src/core/CG/bytecode_compiler.cpp` (exists) | TODO |
| `internal/CG/optimizer.go` | ~200 | `src/core/CG/optimizer.cpp` (exists) | TODO |
| `internal/CG/plan_cache.go` | ~150 | `src/core/CG/plan_cache.cpp` (exists) | TODO |

---

## Phase 5: TM Layer Migration

### Status: PENDING

| Go File | LOC | C++ Target | Status |
|---------|-----|------------|--------|
| `internal/TM/transaction.go` | ~300 | `src/core/TM/transaction.cpp` (exists) | TODO |
| `internal/TM/mvcc.go` | ~400 | `src/core/TM/mvcc.cpp` | TODO - NEW |
| `internal/TM/lock.go` | ~350 | `src/core/TM/lock_table.cpp` | TODO - NEW |
| `internal/TM/isolation.go` | ~200 | `src/core/TM/isolation.cpp` | TODO - NEW |

---

## Phase 6: Cleanup & Testing

### Status: PENDING

- [ ] Delete migrated Go files
- [ ] Update imports
- [ ] Run all tests
- [ ] Benchmark performance
- [ ] Fix regressions

---

## Metrics

### Code Reduction Target

| Layer | Before (Go LOC) | After (Go LOC) | Reduction |
|-------|-----------------|----------------|-----------|
| VM | ~8000 | ~400 | 95% |
| DS | ~6000 | ~200 | 97% |
| QP | ~3000 | ~500 | 83% |
| CG | ~2000 | ~100 | 95% |
| TM | ~1500 | ~50 | 97% |
| **Total** | **~20500** | **~1250** | **94%** |

### Performance Goals

| Operation | Current | Target | Improvement |
|-----------|---------|--------|-------------|
| SELECT 1K rows | 263 µs | <200 µs | 24% |
| SUM aggregate | 28 µs | <20 µs | 29% |
| GROUP BY | 148 µs | <100 µs | 32% |
| INNER JOIN 1K | 1.12 ms | <0.8 ms | 29% |

---

## Build & Test

```bash
# Build C++ libraries
./build.sh

# Run tests
./build.sh -t

# Run benchmarks
./build.sh -b

# Generate coverage
./build.sh -t -c
```

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Index B-Tree page type mismatch | High | Standardize on SQLite canonical format |
| MVCC concurrency with Go | High | Implement entirely in C++ |
| Memory management (GC vs manual) | Medium | Use arena allocators, RAII |
| AST type exposure complexity | Medium | Keep Go types as pure data |
| CGO call overhead | Low | Batch operations, zero-copy |

---

**Last Updated**: 2026-03-03  
**Next Milestone**: Complete Phase 1 (VM Layer) by 2026-03-17
