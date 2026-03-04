# sqlvibe v0.11.3 — Complete C++ Migration Plan

**Date**: 2026-03-22
**Target Version**: v0.11.3
**Status**: Planning

---

## Executive Summary

This plan outlines the aggressive migration to achieve **C++-only core** with minimal Go wrappers (~500 LOC target) for v0.11.3. Building on v0.11.2's 89% Go reduction (21,900 → 2,500 LOC), v0.11.3 targets an additional 80% reduction (2,500 → 500 LOC).

### Goals

1. **C++-Only Core**: All business logic in C++, Go is pure type mapping
2. **Eliminate Callbacks**: Remove Go→C++→Go callback pattern
3. **Pure C++ Components**: Self-contained C++ with no Go dependencies
4. **Thin Go Wrappers**: ~500 LOC of pure type conversion (no logic)
5. **Zero Performance Regression**: Maintain or improve v0.11.2 benchmarks

### Current State (v0.11.2)

| Component | Go LOC | C++ LOC | Can Migrate? | Blocker |
|-----------|--------|---------|--------------|---------|
| **VM** | 500 | 2,000 | ✅ Already thin | None |
| **DS** | 1,000 | 5,000 | ⚠️ Partially | Callbacks |
| **QP** | 400 | 2,300 | ⚠️ Partially | AST types |
| **CG** | 400 | 1,600 | ⚠️ Partially | Orchestration |
| **TM** | 250 | 800 | ⚠️ Partially | Lock integration |
| **IS** | 200 | 500 | ✅ Already thin | None |
| **PB** | 200 | 500 | ⚠️ Partially | VFS interface |
| **SF** | 200 | 500 | ✅ Already thin | None |
| **Total** | **~2,500** | **~15,000** | **~1,500 LOC** | **Callbacks** |

**Target for v0.11.3**: ~500 LOC Go wrappers (80% additional reduction)

---

## Architecture

### Target Architecture v0.11.3

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
│  - Type conversions ONLY (no logic)                     │
│  - Pure data structures                                 │
│  - No callbacks, no business logic                      │
│  (~500 LOC total)                                       │
└─────────────────────────────────────────────────────────┘
                          ↓ CGO (one-way, ~5ns)
┌─────────────────────────────────────────────────────────┐
│           C++ Core Engine (src/core/)                   │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │    DS    │    VM    │    QP    │    CG    │         │
│  │  6000    │  2000    │  3000    │  2000    │         │
│  │   LOC    │   LOC    │   LOC    │   LOC    │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │    TM    │    PB    │    IS    │    SF    │         │
│  │  1500    │  800     │   500    │   500    │         │
│  │   LOC    │   LOC    │   LOC    │   LOC    │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
│                                                         │
│  Total: ~17,000 LOC C++ (self-contained, no callbacks) │
└─────────────────────────────────────────────────────────┘
```

### Key Architectural Changes

1. **Eliminate Callbacks**: C++ components own their I/O
   - C++ PageManager with direct file I/O (no Go callbacks)
   - C++ BTree uses C++ PageManager directly
   - No Go→C++→Go round-trips

2. **Pure C++ Business Logic**: All logic in C++
   - Go wrappers do type conversion ONLY
   - No business logic in Go
   - No decision-making in Go

3. **One-Way CGO Calls**: Go calls C++, never vice versa
   - Simplifies memory management
   - Reduces CGO overhead
   - Enables better optimization

---

## Phase 1: Eliminate Callbacks (Week 1-2)

### 1.1 C++ PageManager with Direct I/O

**Current Issue**: C++ BTree uses Go PageManager via callbacks

**Solution**: Create self-contained C++ PageManager

```cpp
// src/core/DS/page_manager_v2.h
class PageManagerV2 {
public:
    PageManagerV2(const std::string& db_path, uint32_t page_size);
    
    // Direct I/O - no callbacks
    Page ReadPage(uint32_t page_num);
    void WritePage(const Page& page);
    uint32_t AllocatePage();
    void FreePage(uint32_t page_num);
    
private:
    std::string db_path_;
    std::fstream file_;
    LRUCache page_cache_;
    FreeList free_list_;
};
```

**Go Wrapper** (thin, no callbacks):
```go
// internal/DS/page_manager_v2_cgo.go
type CppPageManager struct {
    ptr unsafe.Pointer
}

func NewCppPageManager(path string, pageSize uint32) *CppPageManager {
    // Direct C++ call, no callbacks
}

func (pm *CppPageManager) ReadPage(pageNum uint32) *Page {
    // Direct C++ call, returns Page struct
}
```

**Files to Create**:
- `src/core/DS/page_manager_v2.cpp/h` (500 LOC)
- `internal/DS/page_manager_v2_cgo.go` (50 LOC)

**Files to Remove**:
- `internal/DS/overflow_cgo.go` (callback exports)
- `internal/DS/btree_cgo.go` (callback-based)

### 1.2 C++ BTree with C++ PageManager

**Current Issue**: Go BTree + C++ CBTree dual implementation

**Solution**: Pure C++ BTree with C++ PageManager

```cpp
// src/core/DS/btree_v2.h
class BTreeV2 {
public:
    BTreeV2(PageManagerV2* pm, uint32_t root_page, bool is_table);
    
    std::optional<Value> Search(const Key& key);
    void Insert(const Key& key, const Value& value);
    void Delete(const Key& key);
    
private:
    PageManagerV2* pm_;
    uint32_t root_page_;
    bool is_table_;
};
```

**Files to Create**:
- `src/core/DS/btree_v2.cpp/h` (800 LOC)

**Files to Remove**:
- `internal/DS/btree.go` (843 LOC) - Go BTree implementation
- `internal/DS/btree_cgo.go` (200 LOC) - CGO wrapper with callbacks

### 1.3 C++ OverflowManager

**Current Issue**: Go OverflowManager with C++ helpers

**Solution**: Pure C++ OverflowManager

**Files to Create**:
- `src/core/DS/overflow_manager_v2.cpp/h` (300 LOC)

**Files to Remove**:
- `internal/DS/overflow.go` (250 LOC)
- `internal/DS/overflow_cgo.go` (280 LOC)

---

## Phase 2: Migrate Remaining Business Logic (Week 3-4)

### 2.1 C++ IndexEngine (Complete)

**Current State**: Go IndexEngine with C++ helpers

**Solution**: Pure C++ IndexEngine

**Files to Create**:
- `src/core/DS/index_engine_v2.cpp/h` (400 LOC)

**Files to Remove**:
- `internal/DS/index_engine.go` (200 LOC)
- `internal/DS/hybrid_store_cgo.go` (IndexEngine parts)

### 2.2 C++ HybridStore (Complete)

**Current State**: Go HybridStore orchestrating C++ components

**Solution**: Pure C++ HybridStore

**Files to Create**:
- `src/core/DS/hybrid_store_v2.cpp/h` (600 LOC)

**Files to Remove**:
- `internal/DS/hybrid_store.go` (468 LOC)
- `internal/DS/row_store_cgo.go` (350 LOC)
- `internal/DS/column_store_cgo.go` (400 LOC)

### 2.3 C++ Parallel Query Engine

**Current State**: Go parallel query (parallel.go)

**Solution**: C++ parallel query with OpenMP/TBB

**Files to Create**:
- `src/core/VM/parallel_query.cpp/h` (400 LOC)

**Files to Remove**:
- `internal/DS/parallel.go` (330 LOC)

### 2.4 C++ MMap Reader

**Current State**: Go mmap (mmap.go)

**Solution**: C++ mmap with platform abstraction

**Files to Create**:
- `src/core/PB/mmap_reader.cpp/h` (200 LOC)

**Files to Remove**:
- `internal/DS/mmap.go` (150 LOC)

---

## Phase 3: QP/CG/TM Deep Migration (Week 5-6)

### 3.1 QP: Pure C++ Parser/Tokenizer

**Current State**: Go parser with C++ tokenizer

**Solution**: Pure C++ parser with Go type wrapper

**Files to Create**:
- `src/core/QP/parser_v2.cpp/h` (1000 LOC)
- `src/core/QP/ast_v2.cpp/h` (500 LOC)

**Files to Remove**:
- `internal/QP/parser.go` (584 LOC)
- `internal/QP/parser_*.go` (~1500 LOC total)
- `internal/QP/tokenizer.go` (795 LOC)

**Go Wrapper** (50 LOC):
```go
// internal/QP/qp_v2_cgo.go
type ParsedStatement struct {
    Type StmtType
    // Pure data, no methods
}

func ParseC(sql string) (*ParsedStatement, error) {
    // Direct C++ call
}
```

### 3.2 CG: Pure C++ Compiler

**Current State**: Go compiler with C++ optimizer

**Solution**: Pure C++ compiler

**Files to Create**:
- `src/core/CG/compiler_v2.cpp/h` (800 LOC)

**Files to Remove**:
- `internal/CG/compiler.go` (1315 LOC)
- `internal/CG/bytecode_compiler.go` (340 LOC)
- `internal/CG/expr_compiler.go` (300 LOC)

### 3.3 TM: Pure C++ Transaction Manager

**Current State**: Go TransactionManager with C++ MVCC

**Solution**: Pure C++ TransactionManager

**Files to Create**:
- `src/core/TM/transaction_manager_v2.cpp/h` (600 LOC)
- `src/core/TM/lock_manager_v2.cpp/h` (400 LOC)

**Files to Remove**:
- `internal/TM/transaction.go` (404 LOC)
- `internal/TM/lock.go` (350 LOC)
- `internal/TM/mvcc.go` (130 LOC) - Go version for tests

---

## Phase 4: Cleanup & Optimization (Week 7-8)

### 4.1 Remove Legacy Files

**Files to Remove** (estimated 1,500 LOC):

| File | LOC | Reason |
|------|-----|--------|
| `internal/DS/btree.go` | 843 | Replaced by C++ BTreeV2 |
| `internal/DS/manager.go` | 182 | Replaced by C++ PageManagerV2 |
| `internal/DS/hybrid_store.go` | 468 | Replaced by C++ HybridStoreV2 |
| `internal/DS/overflow.go` | 250 | Replaced by C++ OverflowManagerV2 |
| `internal/DS/overflow_cgo.go` | 280 | Callbacks eliminated |
| `internal/DS/btree_cgo.go` | 200 | Callbacks eliminated |
| `internal/QP/parser.go` | 584 | Replaced by C++ parser |
| `internal/QP/tokenizer.go` | 795 | Replaced by C++ tokenizer |
| `internal/CG/compiler.go` | 1315 | Replaced by C++ compiler |
| `internal/TM/transaction.go` | 404 | Replaced by C++ TM |
| **Total** | **~5,000** | |

### 4.2 Optimize CGO Boundary

**Goals**:
- Batch operations to reduce CGO calls
- Zero-copy where possible
- <5ns CGO overhead

**Techniques**:
- Use `runtime.Pinner` for zero-copy
- Batch multiple operations per CGO call
- Pre-allocate C++ objects

### 4.3 Performance Validation

**Benchmarks to Run**:
- All v0.11.2 benchmarks (must not regress)
- New CGO overhead benchmarks
- Memory allocation benchmarks

**Targets**:
- No benchmark regression >5%
- CGO overhead <10ns per call
- Zero-GC for simple queries

---

## Risk Mitigation

### High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Callback elimination breaks BTree | High | Keep Go BTree as fallback initially |
| C++ PageManager I/O bugs | High | Extensive unit tests, fuzzing |
| Memory management issues | High | RAII, smart pointers, valgrind |

### Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| AST type mismatches | Medium | Keep Go AST as pure data structs |
| CGO call overhead increase | Medium | Batch operations, profile hot paths |
| Test coverage gaps | Medium | Port critical tests to C++ GoogleTest |

### Low Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Build complexity | Low | CMake already working, document well |
| Documentation gaps | Low | Update docs as we migrate |

---

## Success Criteria

### Functional

- [ ] All v0.11.2 tests passing
- [ ] All SQL:1999 test suites passing
- [ ] No callback-based code in C++
- [ ] All Go wrappers <500 LOC total

### Performance

- [ ] No benchmark regression >5%
- [ ] CGO overhead <10ns per call
- [ ] Zero-GC for simple queries
- [ ] 10% improvement on 5+ key benchmarks

### Code Quality

- [ ] >80% test coverage for new C++ code
- [ ] All C++ code follows style guide
- [ ] Go wrappers <500 LOC total
- [ ] Documentation updated

---

## Timeline Summary

| Phase | Component | Start | End | Duration |
|-------|-----------|-------|-----|----------|
| **Phase 1** | Eliminate Callbacks | 2026-03-25 | 2026-04-07 | 2 weeks |
| **Phase 2** | Business Logic | 2026-04-08 | 2026-04-21 | 2 weeks |
| **Phase 3** | QP/CG/TM Deep | 2026-04-22 | 2026-05-05 | 2 weeks |
| **Phase 4** | Cleanup | 2026-05-06 | 2026-05-19 | 2 weeks |

**Total Duration**: 8 weeks
**Completion Target**: 2026-05-19

---

## Code Metrics Projection

### Before v0.11.3 (v0.11.2)

| Layer | Go LOC | C++ LOC | Total |
|-------|--------|---------|-------|
| VM | 500 | 2,000 | 2,500 |
| DS | 1,000 | 5,000 | 6,000 |
| QP | 400 | 2,300 | 2,700 |
| CG | 400 | 1,600 | 2,000 |
| TM | 250 | 800 | 1,050 |
| IS | 200 | 500 | 700 |
| PB | 200 | 500 | 700 |
| SF | 200 | 500 | 700 |
| **Total** | **~2,500** | **~15,000** | **~17,500** |

### After v0.11.3 (Target)

| Layer | Go LOC | C++ LOC | Total | Reduction |
|-------|--------|---------|-------|-----------|
| VM | 50 | 2,000 | 2,050 | 90% |
| DS | 100 | 6,000 | 6,100 | 90% |
| QP | 50 | 3,000 | 3,050 | 87% |
| CG | 50 | 2,500 | 2,550 | 87% |
| TM | 50 | 1,500 | 1,550 | 80% |
| IS | 50 | 500 | 550 | 75% |
| PB | 50 | 800 | 850 | 75% |
| SF | 50 | 500 | 550 | 75% |
| **Total** | **~500** | **~17,000** | **~17,500** | **80%** |

**Go Code Reduction**: 2,500 LOC → 500 LOC (**80% additional reduction**)

**Cumulative Reduction** (v0.10.x → v0.11.3): 21,900 LOC → 500 LOC (**98% reduction**)

---

## Next Steps

### Immediate (Week of 2026-03-25)

1. [ ] Create `src/core/DS/page_manager_v2.cpp/h`
2. [ ] Create `src/core/DS/btree_v2.cpp/h`
3. [ ] Create `internal/DS/page_manager_v2_cgo.go`
4. [ ] Write C++ unit tests for PageManagerV2
5. [ ] Write C++ unit tests for BTreeV2

### Week of 2026-04-01

1. [ ] Migrate HybridStore to C++
2. [ ] Migrate OverflowManager to C++
3. [ ] Eliminate callback exports
4. [ ] Update all DS imports

### Week of 2026-04-08

1. [ ] Start QP parser migration
2. [ ] Start CG compiler migration
3. [ ] Start TM migration
4. [ ] Write migration tests

---

**Document Version**: 1.0
**Created**: 2026-03-22
**Maintainer**: sqlvibe team
**Next Review**: 2026-03-29
