# sqlvibe v0.11.3 — Aggressive C++ Migration Plan

**Date**: 2026-03-22
**Target Version**: v0.11.3
**Status**: Architectural Redesign

---

## Executive Summary

This plan proposes **fundamental architectural changes** to enable true C++-only core with ~500 LOC Go wrappers. The current v0.11.2 architecture has inherent limitations (callbacks, Go-owned I/O, bidirectional CGO) that prevent further reduction.

### The Problem: v0.11.2 Architecture Limitations

```
┌─────────────────────────────────────────────────────────┐
│              Go Application (pkg/sqlvibe)               │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│          Go CGO Wrappers (internal/) ~2,500 LOC         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Go Business Logic (BTree, HybridStore, etc.)   │    │
│  │  ↓ CGO callback (Go→C++)                         │    │
│  │  C++ Component (PageManager, etc.)              │    │
│  │  ↓ CGO callback (C++→Go) ← PROBLEM              │    │
│  │  Go callback (page I/O, etc.)                   │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

**Issues**:
1. **Bidirectional CGO**: Go→C++→Go round-trips (high overhead)
2. **Go owns I/O**: C++ can't do direct file I/O
3. **Go business logic**: BTree, HybridStore logic in Go
4. **Memory management**: Go GC manages C++ memory

**Result**: Cannot reduce below ~2,500 LOC Go

---

## Proposed Architecture: v0.11.3

### Core Principle: C++ Owns Everything

```
┌─────────────────────────────────────────────────────────┐
│              Go Application (pkg/sqlvibe)               │
│  - Pure API layer (Open, Query, Exec, etc.)            │
│  - NO business logic                                    │
│  - NO callbacks                                         │
│  (~800 LOC)                                             │
└─────────────────────────────────────────────────────────┘
                          ↓ One-way CGO (~5ns)
┌─────────────────────────────────────────────────────────┐
│          Go Type Wrappers (internal/) ~500 LOC          │
│  - Pure type conversions (Go Value ↔ C++ Value)        │
│  - NO business logic                                    │
│  - NO callbacks                                         │
└─────────────────────────────────────────────────────────┘
                          ↓ One-way CGO (~5ns)
┌─────────────────────────────────────────────────────────┐
│           C++ Core Engine (src/core/) ~20,000 LOC       │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ Storage Layer (owns ALL I/O)               │   │
│  │  - PageManager (direct file I/O)                │   │
│  │  - BTree (uses C++ PageManager)                 │   │
│  │  - HybridStore (orchestrates C++ components)    │   │
│  │  - Cache, WAL, Compression (all C++)            │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ Query Layer (owns ALL execution)           │   │
│  │  - Tokenizer, Parser (pure C++)                 │   │
│  │  - Compiler (pure C++)                          │   │
│  │  - VM (pure C++)                                │   │
│  │  - Optimizer (pure C++)                         │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ Transaction Layer (owns ALL txn logic)     │   │
│  │  - TransactionManager (pure C++)                │   │
│  │  - LockManager (pure C++)                       │   │
│  │  - MVCC (pure C++)                              │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  Total: ~20,000 LOC C++ (self-contained, NO callbacks) │
└─────────────────────────────────────────────────────────┘
```

### Key Architectural Changes

#### 1. C++ Owns File I/O

**Current**: Go PageManager does file I/O, C++ calls back to Go

**New**: C++ PageManager does direct file I/O

```cpp
// src/core/DS/page_manager_v2.h
class PageManagerV2 {
public:
    // Constructor opens file directly
    explicit PageManagerV2(const std::string& db_path, uint32_t page_size);
    
    // Direct I/O - no callbacks
    Page ReadPage(uint32_t page_num);
    void WritePage(const Page& page);
    uint32_t AllocatePage();
    void FreePage(uint32_t page_num);
    
    // Direct file sync
    void Sync();
    
private:
    std::string db_path_;
    std::fstream file_;  // C++ owns file handle
    LRUCache page_cache_;
    FreeList free_list_;
    std::mutex mutex_;
};
```

**Go Wrapper** (no callbacks, pure type conversion):
```go
// internal/DS/page_manager_v2_cgo.go
type CppPageManager struct {
    ptr unsafe.Pointer  // *C.svdb_page_manager_v2_t
}

func NewCppPageManager(path string, pageSize uint32) *CppPageManager {
    cPath := C.CString(path)
    defer C.free(unsafe.Pointer(cPath))
    ptr := C.svdb_page_manager_v2_create(cPath, C.uint32_t(pageSize))
    return &CppPageManager{ptr: unsafe.Pointer(ptr)}
}

func (pm *CppPageManager) ReadPage(pageNum uint32) *Page {
    // Direct C++ call, returns Page struct
    cPage := C.svdb_page_manager_v2_read_page(
        (*C.svdb_page_manager_v2_t)(pm.ptr),
        C.uint32_t(pageNum),
    )
    return cPageToGo(cPage)  // Pure type conversion
}
```

#### 2. C++ BTree Uses C++ PageManager

**Current**: Go BTree + C++ CBTree (dual implementation with callbacks)

**New**: Pure C++ BTree with C++ PageManager

```cpp
// src/core/DS/btree_v2.h
class BTreeV2 {
public:
    // Takes ownership of PageManager (or reference)
    BTreeV2(std::shared_ptr<PageManagerV2> pm, 
            uint32_t root_page, 
            bool is_table);
    
    std::optional<Value> Search(const Key& key);
    void Insert(const Key& key, const Value& value);
    void Delete(const Key& key);
    
    uint32_t Depth() const;
    uint32_t LeafCount() const;
    
private:
    std::shared_ptr<PageManagerV2> pm_;
    uint32_t root_page_;
    bool is_table_;
};
```

**Go Wrapper** (no callbacks):
```go
// internal/DS/btree_v2_cgo.go
type CppBTree struct {
    ptr unsafe.Pointer  // *C.svdb_btree_v2_t
    pm  *CppPageManager // Keep PageManager alive
}

func NewCppBTree(pm *CppPageManager, rootPage uint32, isTable bool) *CppBTree {
    ptr := C.svdb_btree_v2_create(
        (*C.svdb_page_manager_v2_t)(pm.ptr),
        C.uint32_t(rootPage),
        C.int(boolToCInt(isTable)),
    )
    return &CppBTree{ptr: unsafe.Pointer(ptr), pm: pm}
}

func (bt *CppBTree) Search(key []byte) ([]byte, error) {
    // Direct C++ call, no callbacks
    var result C.svdb_value_t
    found := C.svdb_btree_v2_search(
        (*C.svdb_btree_v2_t)(bt.ptr),
        (*C.uint8_t)(unsafe.Pointer(&key[0])),
        C.size_t(len(key)),
        &result,
    )
    if found == 0 {
        return nil, nil
    }
    return cValueToGoBytes(result), nil  // Pure type conversion
}
```

#### 3. C++ Memory Management

**Current**: Go GC manages memory, C++ allocates via Go

**New**: C++ arena allocators, Go just holds references

```cpp
// src/core/DS/arena_v2.h
class ArenaV2 {
public:
    explicit ArenaV2(size_t chunk_size = 256 * 1024);
    ~ArenaV2();
    
    void* Alloc(size_t size);
    void Reset();
    size_t BytesUsed() const;
    
private:
    std::vector<std::unique_ptr<char[]>> chunks_;
    char* current_;
    size_t offset_;
    size_t used_;
};
```

**Go Wrapper** (no GC pressure):
```go
// internal/DS/arena_v2_cgo.go
type CppArena struct {
    ptr unsafe.Pointer  // *C.svdb_arena_v2_t
}

func NewCppArena(size int) *CppArena {
    ptr := C.svdb_arena_v2_create(C.size_t(size))
    return &CppArena{ptr: unsafe.Pointer(ptr)}
}

func (a *CppArena) Alloc(size int) []byte {
    // C++ allocates, Go gets slice (no GC)
    ptr := C.svdb_arena_v2_alloc(
        (*C.svdb_arena_v2_t)(a.ptr),
        C.size_t(size),
    )
    // Go slice points to C++ memory (valid until Reset/Free)
    return unsafe.Slice((*byte)(ptr), size)
}

func (a *CppArena) Reset() {
    C.svdb_arena_v2_reset((*C.svdb_arena_v2_t)(a.ptr))
}
```

#### 4. C++ Query Execution

**Current**: Go QueryEngine orchestrates C++ VM

**New**: Pure C++ query execution

```cpp
// src/core/VM/query_engine_v2.h
class QueryEngineV2 {
public:
    explicit QueryEngineV2(std::shared_ptr<PageManagerV2> pm);
    
    // Execute SQL directly, return results
    QueryResult Execute(const std::string& sql);
    
    // Prepare statement for repeated execution
    std::shared_ptr<PreparedStatement> Prepare(const std::string& sql);
    
private:
    std::shared_ptr<PageManagerV2> pm_;
    Tokenizer tokenizer_;
    Parser parser_;
    Compiler compiler_;
    VM vm_;
};

class QueryResult {
public:
    std::vector<std::string> ColumnNames;
    std::vector<Value> Rows;  // Flat: [row0_col0, row0_col1, row1_col0, ...]
    int64_t RowsAffected;
    std::string Error;
};
```

**Go Wrapper** (pure API):
```go
// internal/VM/query_engine_v2_cgo.go
type CppQueryEngine struct {
    ptr unsafe.Pointer  // *C.svdb_query_engine_v2_t
}

func NewCppQueryEngine(pm *CppPageManager) *CppQueryEngine {
    ptr := C.svdb_query_engine_v2_create(
        (*C.svdb_page_manager_v2_t)(pm.ptr),
    )
    return &CppQueryEngine{ptr: unsafe.Pointer(ptr)}
}

func (qe *CppQueryEngine) Execute(sql string) *QueryResult {
    cSQL := C.CString(sql)
    defer C.free(unsafe.Pointer(cSQL))
    
    cResult := C.svdb_query_engine_v2_execute(
        (*C.svdb_query_engine_v2_t)(qe.ptr),
        cSQL,
    )
    
    return cQueryResultToGo(cResult)  // Pure type conversion
}
```

#### 5. C++ Transaction Management

**Current**: Go TransactionManager with C++ MVCC helper

**New**: Pure C++ TransactionManager

```cpp
// src/core/TM/transaction_manager_v2.h
class TransactionManagerV2 {
public:
    explicit TransactionManagerV2(std::shared_ptr<PageManagerV2> pm);
    
    std::shared_ptr<Transaction> Begin(TransactionType type);
    void Commit(std::shared_ptr<Transaction> txn);
    void Rollback(std::shared_ptr<Transaction> txn);
    
    MVCCStore* GetMVCCStore() { return &mvcc_store_; }
    LockManager* GetLockManager() { return &lock_manager_; }
    
private:
    std::shared_ptr<PageManagerV2> pm_;
    MVCCStore mvcc_store_;
    LockManager lock_manager_;
    std::atomic<uint64_t> next_txn_id_;
};
```

**Go Wrapper** (pure API):
```go
// internal/TM/transaction_manager_v2_cgo.go
type CppTransactionManager struct {
    ptr unsafe.Pointer  // *C.svdb_transaction_manager_v2_t
}

func NewCppTransactionManager(pm *CppPageManager) *CppTransactionManager {
    ptr := C.svdb_transaction_manager_v2_create(
        (*C.svdb_page_manager_v2_t)(pm.ptr),
    )
    return &CppTransactionManager{ptr: unsafe.Pointer(ptr)}
}

func (tm *CppTransactionManager) Begin(txType TransactionType) *CppTransaction {
    cTxn := C.svdb_transaction_manager_v2_begin(
        (*C.svdb_transaction_manager_v2_t)(tm.ptr),
        C.int(txType),
    )
    return &CppTransaction{ptr: unsafe.Pointer(cTxn)}
}
```

---

## Migration Strategy

### Phase 0: Foundation (Week 0-1)

**Goal**: Create C++ foundation classes

**Deliverables**:
- `src/core/DS/page_manager_v2.cpp/h` (500 LOC)
- `src/core/DS/arena_v2.cpp/h` (200 LOC)
- `src/core/DS/cache_v2.cpp/h` (300 LOC)
- `src/core/DS/free_list_v2.cpp/h` (200 LOC)

**Tests**: C++ unit tests (GoogleTest)

### Phase 1: Storage Layer (Week 1-3)

**Goal**: Migrate storage layer to C++

**Deliverables**:
- `src/core/DS/btree_v2.cpp/h` (800 LOC)
- `src/core/DS/overflow_manager_v2.cpp/h` (300 LOC)
- `src/core/DS/row_store_v2.cpp/h` (400 LOC)
- `src/core/DS/column_store_v2.cpp/h` (400 LOC)
- `src/core/DS/hybrid_store_v2.cpp/h` (600 LOC)
- `src/core/DS/index_engine_v2.cpp/h` (400 LOC)

**Go Wrappers** (~200 LOC):
- `internal/DS/page_manager_v2_cgo.go`
- `internal/DS/btree_v2_cgo.go`
- `internal/DS/hybrid_store_v2_cgo.go`

**Files to Remove** (~2,500 LOC):
- `internal/DS/btree.go` (843 LOC)
- `internal/DS/manager.go` (182 LOC)
- `internal/DS/hybrid_store.go` (468 LOC)
- `internal/DS/overflow.go` (250 LOC)
- `internal/DS/overflow_cgo.go` (280 LOC)
- `internal/DS/btree_cgo.go` (200 LOC)
- `internal/DS/row_store_cgo.go` (350 LOC)
- `internal/DS/column_store_cgo.go` (400 LOC)

### Phase 2: Query Layer (Week 3-5)

**Goal**: Migrate query layer to C++

**Deliverables**:
- `src/core/QP/tokenizer_v2.cpp/h` (400 LOC)
- `src/core/QP/parser_v2.cpp/h` (1000 LOC)
- `src/core/QP/ast_v2.cpp/h` (500 LOC)
- `src/core/CG/compiler_v2.cpp/h` (800 LOC)
- `src/core/VM/query_engine_v2.cpp/h` (600 LOC)

**Go Wrappers** (~100 LOC):
- `internal/QP/qp_v2_cgo.go`
- `internal/CG/cg_v2_cgo.go`
- `internal/VM/query_engine_v2_cgo.go`

**Files to Remove** (~3,000 LOC):
- `internal/QP/tokenizer.go` (795 LOC)
- `internal/QP/parser.go` (584 LOC)
- `internal/QP/parser_*.go` (~1,000 LOC)
- `internal/CG/compiler.go` (1315 LOC)
- `internal/CG/bytecode_compiler.go` (340 LOC)

### Phase 3: Transaction Layer (Week 5-6)

**Goal**: Migrate transaction layer to C++

**Deliverables**:
- `src/core/TM/transaction_manager_v2.cpp/h` (600 LOC)
- `src/core/TM/lock_manager_v2.cpp/h` (400 LOC)
- `src/core/TM/mvcc_v2.cpp/h` (500 LOC)

**Go Wrappers** (~50 LOC):
- `internal/TM/tm_v2_cgo.go`

**Files to Remove** (~900 LOC):
- `internal/TM/transaction.go` (404 LOC)
- `internal/TM/lock.go` (350 LOC)
- `internal/TM/mvcc.go` (130 LOC)

### Phase 4: Utilities (Week 6-7)

**Goal**: Migrate utilities to C++

**Deliverables**:
- `src/core/VM/parallel_query_v2.cpp/h` (400 LOC)
- `src/core/PB/mmap_reader_v2.cpp/h` (200 LOC)
- `src/core/DS/compression_v2.cpp/h` (300 LOC)

**Go Wrappers** (~50 LOC):
- `internal/DS/parallel_v2_cgo.go`
- `internal/DS/mmap_v2_cgo.go`

**Files to Remove** (~500 LOC):
- `internal/DS/parallel.go` (330 LOC)
- `internal/DS/mmap.go` (150 LOC)
- `internal/DS/compression.go` (partial)

### Phase 5: Cleanup (Week 7-8)

**Goal**: Remove all legacy code, optimize

**Tasks**:
- Remove all files from Phases 1-4
- Optimize CGO boundary (batching, zero-copy)
- Performance validation
- Documentation update

**Files to Remove** (remaining ~500 LOC):
- Various utility files
- Test utilities
- Deprecated wrappers

---

## Risk Analysis

### Critical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| C++ file I/O bugs | Medium | High | Extensive unit tests, fuzzing, integration tests |
| Memory leaks in C++ | Medium | High | RAII, smart pointers, valgrind, ASan |
| Callback elimination breaks BTree | High | High | Keep Go BTree as fallback for 2 weeks |
| CGO overhead increases | Low | Medium | Profile hot paths, batch operations |

### Mitigation Strategy

1. **Parallel Implementation**: Keep v0.11.2 code as fallback during migration
2. **Gradual Rollout**: Migrate one component at a time
3. **Extensive Testing**: C++ unit tests + Go integration tests
4. **Performance Monitoring**: Benchmark after each phase

---

## Success Criteria

### Functional

- [ ] All v0.11.2 tests passing
- [ ] All SQL:1999 test suites passing
- [ ] Zero callback-based code
- [ ] All Go wrappers <500 LOC total

### Performance

- [ ] No benchmark regression >5%
- [ ] CGO overhead <5ns per call (down from ~10ns)
- [ ] Zero-GC for simple queries
- [ ] 20% improvement on 5+ key benchmarks

### Code Quality

- [ ] >80% test coverage for new C++ code
- [ ] All C++ code follows style guide
- [ ] Go wrappers <500 LOC total
- [ ] Documentation updated

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
| VM | 50 | 2,500 | 2,550 | 90% |
| DS | 100 | 7,000 | 7,100 | 90% |
| QP | 50 | 3,500 | 3,550 | 87% |
| CG | 50 | 2,500 | 2,550 | 87% |
| TM | 50 | 1,500 | 1,550 | 80% |
| IS | 50 | 500 | 550 | 75% |
| PB | 50 | 800 | 850 | 75% |
| SF | 50 | 500 | 550 | 75% |
| **Total** | **~500** | **~20,000** | **~20,500** | **80%** |

**Go Code Reduction**: 2,500 LOC → 500 LOC (**80% additional reduction**)

**Cumulative Reduction** (v0.10.x → v0.11.3): 21,900 LOC → 500 LOC (**98% reduction**)

---

## Timeline Summary

| Phase | Component | Start | End | Duration |
|-------|-----------|-------|-----|----------|
| **Phase 0** | Foundation | 2026-03-25 | 2026-04-01 | 1 week |
| **Phase 1** | Storage Layer | 2026-04-02 | 2026-04-15 | 2 weeks |
| **Phase 2** | Query Layer | 2026-04-16 | 2026-04-29 | 2 weeks |
| **Phase 3** | Transaction Layer | 2026-04-30 | 2026-05-13 | 2 weeks |
| **Phase 4** | Utilities | 2026-05-14 | 2026-05-20 | 1 week |
| **Phase 5** | Cleanup | 2026-05-21 | 2026-05-27 | 1 week |

**Total Duration**: 9 weeks
**Completion Target**: 2026-05-27

---

## Next Steps

### Immediate (Week of 2026-03-25)

1. [ ] Create `src/core/DS/page_manager_v2.cpp/h`
2. [ ] Create `src/core/DS/arena_v2.cpp/h`
3. [ ] Create `src/core/DS/cache_v2.cpp/h`
4. [ ] Create `src/core/DS/free_list_v2.cpp/h`
5. [ ] Write C++ unit tests for all foundation classes
6. [ ] Set up GoogleTest framework

### Week of 2026-04-01

1. [ ] Create `src/core/DS/btree_v2.cpp/h`
2. [ ] Create `internal/DS/btree_v2_cgo.go`
3. [ ] Migrate BTree tests to C++
4. [ ] Keep Go BTree as fallback

---

**Document Version**: 2.0 (Architectural Redesign)
**Created**: 2026-03-22
**Maintainer**: sqlvibe team
**Next Review**: 2026-03-25
