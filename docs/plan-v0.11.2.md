# sqlvibe v0.11.2 — Complete Go to C++ Migration Plan

**Date**: 2026-03-04  
**Target Version**: v0.11.2  
**Status**: In Progress (Phase 2 of 4)

---

## Executive Summary

This plan outlines the complete migration of sqlvibe from Go to C++, moving all business logic from `internal/` to `src/core/` while maintaining thin Go CGO wrappers for backward compatibility.

### Goals

1. **100% C++ Core**: All query execution, storage, and processing in C++
2. **Thin Go Wrappers**: Go layer reduced to ~500 LOC of pure type mapping
3. **Zero Performance Regression**: Maintain or improve current benchmarks
4. **Backward Compatible**: Existing Go API remains unchanged

### Current State (v0.11.2)

| Subsystem | C++ Implementation | Go Wrapper | Status |
|-----------|-------------------|------------|--------|
| **VM** (Virtual Machine) | ✅ Complete (2000+ LOC) | ✅ Thin (500 LOC) | ✅ Phase 1 Complete |
| **IS** (Info Schema) | ✅ Complete (300 LOC) | ✅ Thin (200 LOC) | ✅ Complete |
| **PB** (Platform/VFS) | ✅ Complete (240 LOC) | ✅ Thin (200 LOC) | ✅ Complete |
| **DS** (Data Storage) | ⚠️ Partial (3000 LOC) | ⚠️ Heavy (4000 LOC) | 🔄 Phase 2 In Progress |
| **QP** (Query Processing) | ⚠️ Partial (2000 LOC) | ⚠️ Heavy (4000 LOC) | ⏳ Phase 3 Pending |
| **CG** (Code Generation) | ⚠️ Partial (1500 LOC) | ⚠️ Heavy (2500 LOC) | ⏳ Phase 3 Pending |
| **TM** (Transaction Mgmt) | ⚠️ Partial (500 LOC) | ⚠️ Heavy (1500 LOC) | ⏳ Phase 3 Pending |
| **SF** (Standard Funcs) | ✅ Complete (500 LOC) | ✅ Minimal (200 LOC) | ✅ Complete |

**Total Progress**: 45% Complete

---

## Architecture

### Target Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Go Application (pkg/sqlvibe)               │
│  - Database.Open(), Query(), Exec()                    │
│  - Rows.Next(), Scan()                                  │
│  - Stmt.Prepare(), Exec(), Query()                      │
│  (~800 LOC - Public API only)                           │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│          Go CGO Wrappers (internal/)                    │
│  - Type conversions (Go ↔ C++)                          │
│  - Error mapping                                         │
│  - Memory management                                     │
│  (~500 LOC total - No business logic)                   │
└─────────────────────────────────────────────────────────┘
                          ↓ CGO (~5ns overhead)
┌─────────────────────────────────────────────────────────┐
│           C++ Core Engine (src/core/)                   │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │    DS    │    VM    │    QP    │    CG    │         │
│  │  5000    │  2000    │  3000    │  2000    │         │
│  │   LOC    │   LOC    │   LOC    │   LOC    │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │    TM    │    PB    │    IS    │    SF    │         │
│  │  1000    │   500    │   500    │   500    │         │
│  │   LOC    │   LOC    │   LOC    │   LOC    │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
│                                                         │
│  Total: ~15,000 LOC C++                                 │
└─────────────────────────────────────────────────────────┘
```

### Migration Principles

1. **C++ First**: New features implemented in C++ only
2. **Incremental Migration**: Migrate subsystem by subsystem
3. **Zero Downtime**: Each phase must pass all tests
4. **Performance Budget**: No regression >5% on any benchmark
5. **Thin Wrappers**: Go wrappers do type conversion only, no logic

---

## Phase 1: VM Layer ✅ COMPLETE

**Duration**: 2026-02-15 to 2026-03-03  
**Status**: ✅ Complete

### Deliverables

- ✅ `src/core/VM/vm_execute.cpp/h` - VM execution engine (1200 LOC)
- ✅ `src/core/VM/bytecode_vm.cpp/h` - Bytecode VM (500 LOC)
- ✅ `src/core/VM/dispatch.cpp/h` - Opcode dispatch (300 LOC)
- ✅ 50+ opcodes implemented in C++
- ✅ Go wrapper: `internal/VM/` (500 LOC thin wrappers)

### Results

- **Code Reduction**: Go VM layer reduced from 6000 LOC → 500 LOC (92%)
- **Performance**: 52× faster opcode dispatch (260ns → 5ns)
- **Tests**: 33 tests passing (18 C++ + 15 Go)

---

## Phase 2: DS Layer 🔄 IN PROGRESS

**Duration**: 2026-03-04 to 2026-03-25  
**Status**: 🔄 In Progress (30% complete)

### Scope

| Component | C++ Status | Go Status | Priority |
|-----------|------------|-----------|----------|
| BTree | ✅ Complete | ⚠️ Still used | High |
| PageManager | ✅ Complete | ⚠️ Still used | High |
| RowStore | ✅ Complete | ⚠️ Still used | High |
| ColumnStore | ✅ Complete | ⚠️ Still used | High |
| HybridStore | ✅ Complete | ⚠️ Still used | High |
| IndexEngine | ✅ Complete | ⚠️ Still used | Medium |
| WAL | ✅ Complete | ⚠️ Still used | Medium |
| Cache | ✅ Complete | ⚠️ Still used | Low |
| Compression | ✅ Complete | ⚠️ Still used | Low |
| BloomFilter | ✅ Complete | ❌ Remove | Low |
| RoaringBitmap | ✅ Complete | ❌ Remove | Low |
| SkipList | ✅ Complete | ❌ Remove | Low |

### Tasks

- [x] Create C++ arena allocator (`arena.cpp/h`)
- [x] Create C++ worker pool (`worker_pool.cpp/h`)
- [x] Create C++ prefetch (`prefetch.cpp/h`)
- [x] Create C++ slab allocator (`slab.cpp/h`)
- [x] Create C++ row structure (`row.cpp/h`)
- [x] Create C++ metrics (`metrics.cpp/h`)
- [ ] **Create DS CGO wrapper** (`internal/DS/ds_cgo.go`)
- [ ] **Migrate HybridStore.Insert()** to C++
- [ ] **Migrate HybridStore.Scan()** to C++
- [ ] **Migrate HybridStore.ScanWhere()** to C++
- [ ] **Migrate BTree.Search()** to C++
- [ ] **Migrate BTree.Insert()** to C++
- [ ] **Migrate PageManager.ReadPage()** to C++
- [ ] **Migrate PageManager.WritePage()** to C++
- [ ] **Remove Go DS files** after migration
- [ ] **Update all DS imports** to use C++ wrappers
- [ ] **Pass all DS tests**
- [ ] **Benchmark DS operations**

### Go Files to Remove (after migration)

```
internal/DS/btree.go           (843 LOC)
internal/DS/manager.go         (182 LOC)
internal/DS/hybrid_store.go    (468 LOC)
internal/DS/row_store.go       (350 LOC via CGO)
internal/DS/columnar.go        (400 LOC via CGO)
internal/DS/page.go            (200 LOC via CGO)
internal/DS/freelist.go        (250 LOC via CGO)
internal/DS/wal.go             (400 LOC via CGO)
internal/DS/cell.go            (200 LOC via CGO)
internal/DS/value.go           (250 LOC via CGO)
internal/DS/encoding.go        (417 LOC via CGO)
internal/DS/balance.go         (300 LOC via CGO)
internal/DS/overflow.go        (300 LOC via CGO)
internal/DS/cache.go           (200 LOC via CGO)
internal/DS/compression.go     (290 LOC via CGO)
internal/DS/bloom_filter.go    (111 LOC) - Remove
internal/DS/roaring_bitmap.go  (176 LOC) - Remove
internal/DS/skip_list.go       (295 LOC) - Remove
```

**Total Go DS**: ~6000 LOC → ~500 LOC wrappers (92% reduction)

---

## Phase 3: QP/CG/TM Layer ⏳ PENDING

**Duration**: 2026-03-26 to 2026-04-22  
**Status**: ⏳ Pending

### QP (Query Processing)

**Current State**:
- C++: `src/core/QP/` (2000 LOC) - Tokenizer, parser, binder, analyzer
- Go: `internal/QP/` (4000 LOC) - AST types, optimizer, normalizer

**Migration Plan**:
1. Keep Go AST types as pure data structures (no logic)
2. Move tokenizer to C++ calls only
3. Move parser to C++ calls only
4. Move optimizer to C++
5. Move normalizer to C++

**Go Files to Reduce**:
```
internal/QP/tokenizer.go  (795 LOC) → 50 LOC wrapper
internal/QP/parser.go     (584 LOC) → 100 LOC wrapper
internal/QP/analyzer.go   (300 LOC) → 50 LOC wrapper
internal/QP/optimizer.go  (412 LOC) → 50 LOC wrapper
internal/QP/binder.go     (271 LOC) → 50 LOC wrapper
```

### CG (Code Generation)

**Current State**:
- C++: `src/core/CG/` (1500 LOC) - Bytecode compiler, optimizer
- Go: `internal/CG/` (2500 LOC) - Compiler orchestration, plan cache

**Migration Plan**:
1. Move compiler orchestration to C++
2. Move bytecode optimizer to C++
3. Move plan cache to C++
4. Move expression compiler to C++

**Go Files to Reduce**:
```
internal/CG/compiler.go            (1315 LOC) → 100 LOC wrapper
internal/CG/bytecode_compiler.go   (340 LOC) → 50 LOC wrapper
internal/CG/expr_compiler.go       (300 LOC) → 50 LOC wrapper
internal/CG/optimizer.go           (719 LOC) → 50 LOC wrapper
internal/CG/plan_cache.go          (200 LOC) → 50 LOC wrapper
```

### TM (Transaction Management)

**Current State**:
- C++: `src/core/TM/` (500 LOC) - Transaction basics
- Go: `internal/TM/` (1500 LOC) - MVCC, locks, deadlock detection

**Migration Plan**:
1. Move MVCC engine to C++
2. Move lock manager to C++
3. Move deadlock detector to C++
4. Move WAL coordination to C++

**Go Files to Reduce**:
```
internal/TM/transaction.go  (404 LOC) → 50 LOC wrapper
internal/TM/mvcc.go         (400 LOC) → 50 LOC wrapper
internal/TM/lock.go         (350 LOC) → 50 LOC wrapper
internal/TM/isolation.go    (200 LOC) → 50 LOC wrapper
internal/TM/wal.go          (371 LOC) → 50 LOC wrapper
```

---

## Phase 4: Cleanup & Optimization ⏳ PENDING

**Duration**: 2026-04-23 to 2026-05-07  
**Status**: ⏳ Pending

### Tasks

- [ ] Remove all legacy Go implementation files
- [ ] Update documentation
- [ ] Run full test suite
- [ ] Run full benchmark suite
- [ ] Fix any performance regressions
- [ ] Update build scripts
- [ ] Update CI/CD pipelines
- [ ] Create migration guide for users
- [ ] Tag v0.11.2 release

### Final Go Wrapper Structure

```
internal/
├── cgo/              # Main CGO bindings (~300 LOC)
│   ├── db_cgo.go
│   ├── exec_cgo.go
│   ├── rows_cgo.go
│   ├── stmt_cgo.go
│   └── tx_cgo.go
├── DS/               # DS wrappers (~100 LOC)
│   └── ds_cgo.go
├── VM/               # VM wrappers (~100 LOC)
│   └── vm_cgo.go
├── QP/               # QP wrappers (~50 LOC)
│   └── qp_cgo.go
└── CG/               # CG wrappers (~50 LOC)
    └── cg_cgo.go

Total: ~600 LOC Go wrappers
```

---

## Code Metrics

### Before Migration (v0.10.x)

| Layer | Go LOC | C++ LOC | Total |
|-------|--------|---------|-------|
| VM | 6000 | 0 | 6000 |
| DS | 6000 | 3000 | 9000 |
| QP | 4000 | 2000 | 6000 |
| CG | 2500 | 1500 | 4000 |
| TM | 1500 | 500 | 2000 |
| IS | 800 | 300 | 1100 |
| PB | 600 | 240 | 840 |
| SF | 500 | 500 | 1000 |
| **Total** | **21,900** | **8,040** | **29,940** |

### After Migration (v0.11.2 Target)

| Layer | Go LOC | C++ LOC | Total | Reduction |
|-------|--------|---------|-------|-----------|
| VM | 500 | 2000 | 2500 | 58% |
| DS | 500 | 5000 | 5500 | 39% |
| QP | 250 | 3000 | 3250 | 46% |
| CG | 250 | 2000 | 2250 | 44% |
| TM | 250 | 1000 | 1250 | 38% |
| IS | 200 | 500 | 700 | 36% |
| PB | 200 | 500 | 700 | 17% |
| SF | 200 | 500 | 700 | 30% |
| **Total** | **2,350** | **14,500** | **16,850** | **44%** |

**Go Code Reduction**: 21,900 LOC → 2,350 LOC (**89% reduction**)

---

## Performance Targets

### Benchmark Goals

| Operation | Current (v0.10.x) | Target (v0.11.2) | Improvement |
|-----------|------------------|------------------|-------------|
| SELECT 1K rows | 263 µs | <200 µs | 24% faster |
| SELECT 10K rows | 2.26 ms | <1.8 ms | 20% faster |
| SUM aggregate | 28 µs | <20 µs | 29% faster |
| GROUP BY 1K | 148 µs | <100 µs | 32% faster |
| INNER JOIN 1K | 1.12 ms | <0.8 ms | 29% faster |
| WHERE filter | 793 µs | <500 µs | 37% faster |
| INSERT batch 1K | 2.83 ms | <2.5 ms | 12% faster |

### Memory Goals

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| GC pressure | High | Minimal | Zero-GC queries |
| Memory alloc/query | ~100 KB | ~10 KB | 90% reduction |
| CGO overhead | ~260ns/call | ~5ns/call | 52× faster |

---

## Testing Strategy

### Unit Tests

- **C++ Tests**: GoogleTest for all C++ modules
- **Go Tests**: Keep existing Go tests, update to use C++ backend
- **Coverage Target**: >80% for all new C++ code

### Integration Tests

- **SQL Compatibility**: All 84+ SQL:1999 test suites
- **Regression Tests**: All existing regression tests must pass
- **Performance Tests**: Benchmark suite must meet targets

### Test Migration Plan

1. Port critical C++ tests to GoogleTest
2. Keep Go tests for integration testing
3. Add CGO boundary tests
4. Add performance regression tests

---

## Risks & Mitigations

### High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| B-Tree page type mismatch | High | Standardize on SQLite canonical format in C++ |
| MVCC concurrency with Go | High | Implement entirely in C++, expose via C API |
| Memory management (GC vs manual) | Medium | Use arena allocators, RAII in C++ |

### Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| AST type exposure complexity | Medium | Keep Go types as pure data structures |
| CGO call overhead | Low | Batch operations, zero-copy where possible |
| Test coverage gaps | Medium | Port critical tests to C++, keep Go integration tests |

### Low Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Build system complexity | Low | CMake + Go build already working |
| Documentation gaps | Low | Update docs as we migrate |

---

## Timeline Summary

| Phase | Start | End | Duration | Status |
|-------|-------|-----|----------|--------|
| **Phase 1: VM** | 2026-02-15 | 2026-03-03 | 2.5 weeks | ✅ Complete |
| **Phase 2: DS** | 2026-03-04 | 2026-03-25 | 3 weeks | 🔄 In Progress |
| **Phase 3: QP/CG/TM** | 2026-03-26 | 2026-04-22 | 4 weeks | ⏳ Pending |
| **Phase 4: Cleanup** | 2026-04-23 | 2026-05-07 | 2 weeks | ⏳ Pending |

**Total Duration**: 12 weeks (3 months)  
**Completion Target**: 2026-05-07

---

## Success Criteria

### Functional

- [ ] All 84+ SQL:1999 test suites passing
- [ ] All regression tests passing
- [ ] All existing Go API tests passing
- [ ] FTS5, JSON, Math extensions working

### Performance

- [ ] No benchmark regression >5%
- [ ] At least 20% improvement on 3+ key benchmarks
- [ ] Zero-GC query execution for simple queries
- [ ] CGO overhead <10ns per call

### Code Quality

- [ ] >80% test coverage for new C++ code
- [ ] All C++ code follows project style guide
- [ ] All Go wrappers <600 LOC total
- [ ] Documentation updated

---

## Next Steps

### Immediate (Week of 2026-03-04)

1. [ ] Create `internal/DS/ds_cgo.go` - Main DS CGO wrapper
2. [ ] Migrate `HybridStore.Insert()` to C++
3. [ ] Migrate `HybridStore.Scan()` to C++
4. [ ] Write DS CGO tests
5. [ ] Benchmark DS operations

### Week of 2026-03-11

1. [ ] Migrate BTree operations to C++
2. [ ] Migrate PageManager to C++
3. [ ] Remove Go BTree implementation
4. [ ] Update all DS imports

### Week of 2026-03-18

1. [ ] Complete DS layer migration
2. [ ] Pass all DS tests
3. [ ] Meet DS performance targets
4. [ ] Start QP layer planning

---

**Document Version**: 1.0  
**Last Updated**: 2026-03-04  
**Maintainer**: sqlvibe team  
**Next Review**: 2026-03-11
