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
**Status**: 🔄 In Progress (65% complete) - Interface-Based Integration Complete

### Scope

| Component | C++ Status | Go Wrapper | Priority |
|-----------|------------|-----------|----------|
| BTree | ✅ Complete | ✅ CGO Wrapper (btree_cgo.go) | High |
| PageManager | ✅ Complete | ✅ C++ Wrapper (manager_cgo_wrapper.go) | High |
| RowStore | ✅ Complete | ✅ CGO Wrapper (row_store_cgo.go) | High |
| ColumnStore | ✅ Complete | ✅ CGO Wrapper (column_store_cgo.go) | High |
| HybridStore | ✅ Complete | ✅ CGO Wrapper (hybrid_store_cgo.go) | High |
| IndexEngine | ✅ Complete | ✅ CGO Wrapper (hybrid_store_cgo.go) | Medium |
| WAL | ✅ Complete | ✅ CGO Wrapper (wal_cgo.go) | Medium |
| Cache | ✅ Complete | ✅ CGO Wrapper (cache_cgo.go) | Low |
| Compression | ✅ Complete | ⚠️ Go impl (via CGO) | Low |
| BloomFilter | ✅ Complete | ❌ Remove | Low |
| RoaringBitmap | ✅ Complete | ✅ CGO Wrapper | Low |
| SkipList | ✅ Complete | ❌ Remove | Low |

### Tasks

- [x] Create C++ arena allocator (`arena.cpp/h`)
- [x] Create C++ worker pool (`worker_pool.cpp/h`)
- [x] Create C++ prefetch (`prefetch.cpp/h`)
- [x] Create C++ slab allocator (`slab.cpp/h`)
- [x] Create C++ row structure (`row.cpp/h`)
- [x] Create C++ metrics (`metrics.cpp/h`)
- [x] **Build C++ DS library** - libsvdb.so built successfully (2026-03-04)
- [x] **Create DS CGO wrapper** (`internal/DS/ds_cgo.go`) - Consolidated utilities
- [x] **HybridStore.Insert()** - Uses C++ RowStore/ColumnStore
- [x] **HybridStore.Scan()** - Uses Go cache (fast path)
- [x] **HybridStore.ScanWhere()** - Uses C++ IndexEngine
- [x] **BTree.Search()** - C++ BTree with Go PageManager callbacks
- [x] **BTree.Insert()** - C++ BTree with Go PageManager callbacks
- [x] **PageManager callbacks** - goPageRead/Write/Allocate/Free exported to C++
- [x] **C++ PageManager** - Full implementation with VFS, caching, header init
- [x] **C++ PageManager tests** - 8/8 tests passing
- [x] **PageManagerInterface** - Interface for Go/C++ PageManager interoperability
- [x] **QueryEngine migration** - Updated to use PageManagerInterface
- [x] **TransactionManager migration** - Updated to use PageManagerInterface
- [x] **BTree migration** - Updated to use PageManagerInterface
- [ ] **Remove Go PageManager files** after full migration
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

## Phase 3: QP/CG/TM Layer ⏳ PLANNING COMPLETE

**Duration**: 2026-03-11 to 2026-04-08
**Status**: ⏳ Planning Complete - Ready to Start

### Overview

Phase 3 migrates Query Processing (QP), Code Generation (CG), and Transaction Management (TM) layers.

| Sub-Phase | Component | Duration | Target Reduction |
|-----------|-----------|----------|------------------|
| 3.1 | QP Migration | 2026-03-11 to 2026-03-18 | 3000 LOC → 400 LOC (87%) |
| 3.2 | CG Migration | 2026-03-18 to 2026-03-25 | 3200 LOC → 400 LOC (87%) |
| 3.3 | TM Migration | 2026-03-25 to 2026-04-01 | 1700 LOC → 250 LOC (85%) |
| 3.4 | Integration | 2026-04-01 to 2026-04-08 | Testing + benchmarks |

### Detailed Plan

See [`docs/phase3-plan.md`](phase3-plan.md) for the complete Phase 3 migration plan.

### Current State

| Layer | C++ Implementation | Go Wrapper | Status |
|-------|-------------------|------------|--------|
| **QP** (Query Processing) | ✅ 2300 LOC (75%) | ⚠️ 3000 LOC | 📋 Ready |
| **CG** (Code Generation) | ✅ 1600 LOC (50%) | ⚠️ 3200 LOC | 📋 Ready |
| **TM** (Transaction Mgmt) | ⚠️ 500 LOC (25%) | ⚠️ 1700 LOC | 📋 Ready |

### Key Tasks

#### QP Migration (Week of 2026-03-11)

- [ ] Create `internal/QP/qp_cgo.go` wrapper
- [ ] Migrate `Tokenize()` to C++
- [ ] Migrate `Parse()` to C++
- [ ] Create C++ optimizer
- [ ] Migrate `Optimize()` to C++

#### CG Migration (Week of 2026-03-18)

- [ ] Create `internal/CG/cg_cgo.go` wrapper
- [ ] Migrate `Compile()` to C++
- [ ] Enhance C++ bytecode optimizer
- [ ] Migrate plan cache to C++

#### TM Migration (Week of 2026-03-25)

- [ ] Create C++ MVCC engine (`src/core/TM/mvcc.cpp`)
- [ ] Create C++ lock manager (`src/core/TM/lock_manager.cpp`)
- [ ] Create `internal/TM/tm_cgo.go` wrapper
- [ ] Migrate transaction operations to C++

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
| **Phase 2: DS** | 2026-03-04 | 2026-03-25 | 3 weeks | 🔄 In Progress (65%) |
| **Phase 3: QP/CG/TM** | 2026-03-11 | 2026-04-08 | 4 weeks | 📋 Planning Complete |
| **Phase 4: Cleanup** | 2026-04-09 | 2026-04-22 | 2 weeks | ⏳ Pending |

**Total Duration**: 12 weeks (3 months)
**Completion Target**: 2026-04-22

### Phase 2 Progress Log

| Date | Milestone | Notes |
|------|-----------|-------|
| 2026-03-04 | C++ library build | libsvdb.so built successfully (100% compile) |
| 2026-03-04 | Plan updated | Progress tracking in-time |
| 2026-03-04 | DS CGO wrapper | Created internal/DS/ds_cgo.go - consolidated CGO utilities |
| 2026-03-04 | CGO tests pass | 70+ CGO tests passing (BTree, Manager, HybridStore, IndexEngine, etc.) |
| 2026-03-04 | DS migration status | Phase 2: 45% complete - CGO wrappers working, Go tests passing |
| 2026-03-04 | C++ PageManager | Full C++ PageManager implementation with VFS, caching, header init |
| 2026-03-04 | PageManager tests | 8/8 C++ PageManager tests passing (Create, ReadWrite, Free, Header, etc.) |
| 2026-03-04 | VFS fixes | Fixed flag parsing, file extension on write, header initialization |
| 2026-03-04 | Full test suite | DS/VM/TM tests all passing (80+ tests total) |
| 2026-03-04 | Phase 2 status | 55% complete - C++ PageManager ready for integration |
| 2026-03-04 | PageManagerInterface | Created interface for Go/C++ PageManager interoperability |
| 2026-03-04 | QueryEngine migration | Updated to use PageManagerInterface |
| 2026-03-04 | TransactionManager migration | Updated to use PageManagerInterface |
| 2026-03-04 | BTree migration | Updated to use PageManagerInterface |
| 2026-03-04 | Integration complete | All DS/VM/TM tests passing with interface-based design |

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

### Immediate (Week of 2026-03-04) - COMPLETED

1. [x] Build C++ DS library (libsvdb.so)
2. [x] Create `internal/DS/ds_cgo.go` - Main DS CGO wrapper
3. [x] HybridStore.Insert() - Uses C++ RowStore/ColumnStore
4. [x] HybridStore.Scan() - Uses Go cache (fast path)
5. [x] Write DS CGO tests
6. [x] Benchmark DS operations
7. [x] C++ PageManager implementation with full I/O
8. [x] All DS/VM/TM tests passing

### Week of 2026-03-11 - Phase 2 Completion + Phase 3 Start

1. [ ] Complete remaining DS migration tasks
2. [ ] Create `internal/QP/qp_cgo.go` wrapper
3. [ ] Migrate `Tokenize()` to C++
4. [ ] Migrate `Parse()` to C++
5. [ ] Write QP CGO tests
6. [ ] Benchmark QP operations

### Week of 2026-03-18 - CG Migration

1. [ ] Create `internal/CG/cg_cgo.go` wrapper
2. [ ] Migrate `Compile()` to C++
3. [ ] Enhance C++ bytecode optimizer
4. [ ] Migrate plan cache to C++
5. [ ] Write CG CGO tests

### Week of 2026-03-25 - TM Migration

1. [ ] Create C++ MVCC engine
2. [ ] Create C++ lock manager
3. [ ] Create `internal/TM/tm_cgo.go` wrapper
4. [ ] Migrate transaction operations to C++
5. [ ] Write TM CGO tests

### Week of 2026-04-01 - Integration

1. [ ] Full integration testing
2. [ ] Performance benchmarking
3. [ ] Fix any regressions
4. [ ] Documentation update

---

**Document Version**: 1.4
**Last Updated**: 2026-03-04
**Maintainer**: sqlvibe team
**Next Review**: 2026-03-11
