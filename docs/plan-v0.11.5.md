# sqlvibe v0.11.5 Development Plan

**Version**: v0.11.5  
**Date**: 2026-03-06  
**Focus**: C++ Performance Optimization & Module Structure  

---

## Overview

v0.11.5 focuses on optimizing the C++ core engine after the module structure reorganization (v0.11.4). The goal is to identify and implement high-impact performance improvements across memory management, SIMD utilization, query execution, and build optimization.

---

## Completed Tasks

### [x] Module Structure Optimization (v0.11.4 → v0.11.5)

**Status**: ✅ Complete  
**Commit**: `8ccf4e4 refactor: Optimize C++ module structure into src/core/[SubSystem]`

**Changes**:
- Created `SC/` (System Composer) subsystem for C API and orchestration
- Moved `svdb/*` → `SC/` (unified C public API)
- Moved `wrapper/*` → `SC/` (invoke chain wrappers)
- Moved `cgo/hash_join.*` → `VM/` (hash join is VM execution)
- Removed 6 redundant subsystem CMakeLists.txt files (CG, DS, QP, VM, cgo, wrapper)
- Updated Go CGO include paths to `core/SC`
- Single `libsvdb.so` build from `src/CMakeLists.txt`

**Result**: All C++ core code organized under `src/core/[SubSystem]`
```
src/core/
├── CG/  (Code Generation)
├── DS/  (Data Storage)
├── IS/  (Information Schema)
├── PB/  (Platform Bridges)
├── QP/  (Query Processing)
├── SC/  (System Composer) ← NEW
├── SF/  (System Framework)
├── TM/  (Transaction Management)
└── VM/  (Virtual Machine)
```

---

## Architecture Review Findings

### Current State Analysis

| Subsystem | LOC | Key Components | Status |
|-----------|-----|----------------|--------|
| CG | ~1,500 | Bytecode compiler, optimizer, plan cache | ✅ Stable |
| DS | ~3,500 | B-Tree, page manager, cache, arena, SIMD | ✅ Stable |
| IS | ~500 | Virtual tables, schema registry | ⚠️ TODOs present |
| PB | ~300 | VFS abstraction | ✅ Stable |
| QP | ~2,000 | Tokenizer, parser, analyzer, binder | ✅ Stable |
| SC | ~8,000 | C API, orchestration, invoke chain | ✅ Stable |
| SF | ~200 | Assertions, types | ✅ Stable |
| TM | ~400 | MVCC, transactions | ✅ Stable |
| VM | ~4,000 | Bytecode executor, engines, opcodes | ✅ Stable |

**Total**: ~20,400 LOC C++ core

---

## Optimization Opportunities

### Priority Matrix

| Priority | Area | Effort | Impact | Est. Gain |
|----------|------|--------|--------|-----------|
| **P0** | Memory Management (Arena) | Medium | High | 40% GC reduction |
| **P0** | Batch Query Engine | High | High | 5-10x analytical |
| **P1** | Cache Hierarchy | High | High | 20-30% hit rate |
| **P1** | SIMD Expansion | Medium | High | 4x batch ops |
| **P1** | Bytecode Optimizer | Medium | Medium | 2-3x complex queries |
| **P2** | Index Statistics | Low | Medium | Better plans |
| **P2** | Parser string_view | Low | Low | 30% faster parse |
| **P2** | LTO/PGO | Low | Medium | 10-15% overall |

---

## Detailed Optimization Plans

### P0: Memory Management with Arena Allocator

**Problem**: Mixed allocation strategies (`new[]`, `malloc`, `free`) cause fragmentation and GC pressure

**Files to Modify**:
- `src/core/VM/hash_join.cpp`
- `src/core/VM/engine/engine.cpp`
- `src/core/VM/vm_opcode.cpp`
- `src/core/DS/arena_v2.h` (extend)

**Implementation**:
```cpp
// Current (hash_join.cpp:113-117)
svdb_row_t* new_rows = new svdb_row_t[new_capacity];
std::copy(result.rows, result.rows + result.capacity, new_rows);
delete[] result.rows;

// Optimized: Use arena allocator
void* mem = arena->Alloc(new_capacity * sizeof(svdb_row_t));
svdb_row_t* new_rows = new(mem) svdb_row_t[new_capacity];
```

**Tasks**:
- [ ] Extend `ArenaV2` with placement new helpers
- [ ] Replace `new[]`/`delete[]` in hash_join.cpp
- [ ] Replace `malloc`/`free` in vm_opcode.cpp
- [ ] Add per-query arena lifecycle management
- [ ] Benchmark GC pressure reduction

**Expected Impact**: 40% reduction in Go GC pressure, 15% latency improvement

---

### P0: Batch Query Engine

**Problem**: Row-by-row processing in `VM/engine/engine.cpp`

**Files to Modify**:
- `src/core/VM/engine/engine.cpp`
- `src/core/VM/engine/engine_api.h`
- `src/core/VM/batch_engine.h` (new)
- `src/core/VM/batch_engine.cpp` (new)

**Implementation**:
```cpp
// Add batch execution mode
struct BatchEngine {
    static constexpr int BATCH_SIZE = 256;
    svdb_value_t registers[BATCH_SIZE][64];  // Pre-allocated
    
    void ExecuteFilterBatch(const uint8_t* predicates, size_t count);
    void ExecuteAggregateBatch(const AggFunc* funcs, size_t count);
    void ExecuteProjectBatch(const Expr* exprs, size_t count);
};
```

**Tasks**:
- [ ] Design BatchEngine API
- [ ] Implement batch filter execution
- [ ] Implement batch aggregate execution
- [ ] Implement batch projection
- [ ] Integrate with existing engine
- [ ] Add benchmarks

**Expected Impact**: 5-10x speedup for analytical queries (SUM, COUNT, GROUP BY)

---

### P1: Tiered Cache Architecture

**Problem**: Single-level LRU cache with mutex contention

**Files to Modify**:
- `src/core/DS/cache_v2.h`
- `src/core/DS/cache_v2.cpp`
- `src/core/DS/tiered_cache.h` (new)
- `src/core/DS/tiered_cache.cpp` (new)

**Implementation**:
```cpp
class TieredCache {
    LRUCacheV2 hot_cache;    // 256 pages, thread-local
    LRUCacheV2 cold_cache;   // 4K pages, global mutex
    
    void* Get(uint32_t page_num, size_t* out_size);
    void Put(uint32_t page_num, const uint8_t* data, size_t size);
    void Prefetch(uint32_t page_num);  // Async prefetch
};
```

**Tasks**:
- [ ] Design tiered cache API
- [ ] Implement hot/cold separation
- [ ] Add prefetch mechanism
- [ ] Integrate with PageManager
- [ ] Benchmark cache hit rate

**Expected Impact**: 20-30% cache hit rate improvement, reduced mutex contention

---

### P1: SIMD Expansion

**Problem**: SIMD only used for basic vector ops, not query execution

**Files to Modify**:
- `src/core/VM/compare.cpp`
- `src/core/VM/aggregate.cpp`
- `src/core/VM/hash.cpp`
- `src/core/DS/simd.h` (extend)
- `src/core/DS/simd.cpp` (extend)

**Implementation**:
```cpp
// Add SIMD batch compare (VM/compare.cpp)
void svdb_batch_compare_int64(const int64_t* a, const int64_t* b, 
                               uint8_t* results, size_t n) {
#ifdef __AVX2__
    for (size_t i = 0; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i cmp = _mm256_cmpeq_epi64(va, vb);
        _mm256_storeu_si256((__m256i*)&results[i], cmp);
    }
#endif
}
```

**Tasks**:
- [ ] Add batch compare SIMD functions
- [ ] Add batch aggregate SIMD functions
- [ ] Add hash probe SIMD functions
- [ ] Integrate into VM execution paths
- [ ] Benchmark speedup

**Expected Impact**: 4x speedup for batch comparisons, aggregates, hash probes

---

### P1: Bytecode Optimizer Enhancements

**Problem**: Optimizer only handles basic arithmetic

**Files to Modify**:
- `src/core/CG/optimizer.cpp`
- `src/core/CG/optimizer.h`

**Implementation**:
```cpp
// Add predicate pushdown optimization
// Before: OpLoadConst → OpFilter → OpResultRow
// After:  OpSeekGE (uses index) → OpResultRow

// Add loop-invariant code motion
// Detect constants in loop bodies, hoist to pre-header
```

**Tasks**:
- [ ] Implement predicate pushdown
- [ ] Implement loop-invariant code motion
- [ ] Implement index-aware opcode selection
- [ ] Add optimizer statistics
- [ ] Benchmark complex queries

**Expected Impact**: 2-3x speedup for complex queries with repeated expressions

---

### P2: Index Statistics

**Problem**: No index statistics for cost-based optimization

**Files to Modify**:
- `src/core/IS/is_registry.cpp`
- `src/core/IS/is_registry.h`
- `src/core/IS/index_stats.h` (new)

**Implementation**:
```cpp
struct IndexStats {
    uint32_t cardinality;
    uint32_t height;
    double selectivity;
};

int ChooseIndex(const QueryPlan& plan, const IndexStats* stats, size_t n);
```

**Tasks**:
- [ ] Design index statistics collection
- [ ] Implement statistics update on write
- [ ] Add index selection algorithm
- [ ] Integrate with query planner
- [ ] Benchmark plan quality

**Expected Impact**: Better query plans for multi-index scenarios

---

### P2: Parser Zero-Copy with string_view

**Problem**: Parser creates many temporary `std::string` allocations

**Files to Modify**:
- `src/core/QP/parser.cpp`
- `src/core/QP/parser.h`
- `src/core/QP/tokenizer.h` (extend)

**Implementation**:
```cpp
// Use string_view for zero-copy parsing
struct Token {
    TokenType type;
    std::string_view text;  // Points into original SQL buffer
};

std::string_view read_ident(const std::string& sql, size_t& pos);
```

**Tasks**:
- [ ] Convert Token to use string_view
- [ ] Update parser functions to return string_view
- [ ] Update AST nodes to use string_view
- [ ] Ensure SQL buffer lifetime management
- [ ] Benchmark parsing speed

**Expected Impact**: 30% faster parsing, reduced allocation overhead

---

### P2: Build Optimization (LTO/PGO)

**Problem**: No profile-guided or link-time optimization

**Files to Modify**:
- `CMakeLists.txt`
- `src/CMakeLists.txt`

**Implementation**:
```cmake
# Add LTO
include(CheckIPOSupported)
check_ipo_supported(RESULT lto_supported)
if(lto_supported)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# Add PGO (optional)
option(ENABLE_PGO "Enable profile-guided optimization" OFF)
if(ENABLE_PGO)
    add_compile_options(-fprofile-generate)
    add_link_options(-fprofile-generate)
endif()
```

**Tasks**:
- [ ] Enable LTO in CMakeLists.txt
- [ ] Add PGO build option
- [ ] Create benchmark suite for PGO training
- [ ] Document PGO build process
- [ ] Benchmark overall speedup

**Expected Impact**: 10-15% speedup across all workloads

---

## Quick Wins (1-2 days each)

These optimizations provide immediate value with minimal effort:

### [ ] 1. Parser string_view
- **Files**: `QP/parser.cpp`, `QP/parser.h`
- **Effort**: 1 day
- **Impact**: 30% faster parsing

### [ ] 2. Enable LTO
- **Files**: `CMakeLists.txt`
- **Effort**: 2 hours
- **Impact**: 10-15% overall

### [ ] 3. Arena for hash_join
- **Files**: `VM/hash_join.cpp`, `DS/arena_v2.h`
- **Effort**: 1 day
- **Impact**: 40% GC reduction

### [ ] 4. SIMD batch compare
- **Files**: `VM/compare.cpp`, `DS/simd.cpp`
- **Effort**: 2 days
- **Impact**: 4x batch comparisons

---

## Performance Targets

| Workload | v0.11.2 | v0.11.5 Target | Improvement |
|----------|---------|----------------|-------------|
| SELECT all (100K) | 27.4 ms | 20 ms | 1.4x faster |
| SUM aggregate (100K) | 2.33 ms | 1.0 ms | 2.3x faster |
| GROUP BY (100K) | 11.7 ms | 3.0 ms | 3.9x faster |
| WHERE filter (10K) | 8.39 ms | 4.0 ms | 2.1x faster |
| INNER JOIN (10K) | 11.5 ms | 5.0 ms | 2.3x faster |
| ORDER BY + LIMIT (100K) | 35.7 ms | 20 ms | 1.8x faster |

---

## Milestones

### M1: Quick Wins (Week 1)
- [ ] Parser string_view
- [ ] Enable LTO
- [ ] Arena for hash_join
- [ ] SIMD batch compare

### M2: Memory & Cache (Week 2-3)
- [ ] Full arena integration
- [ ] Tiered cache design
- [ ] Batch engine prototype

### M3: Query Engine (Week 4-5)
- [ ] Batch filter execution
- [ ] Batch aggregate execution
- [ ] SIMD expansion

### M4: Optimizer (Week 6)
- [ ] Bytecode optimizer enhancements
- [ ] Index statistics
- [ ] PGO integration

### M5: Validation (Week 7)
- [ ] Full benchmark suite
- [ ] Regression tests
- [ ] Documentation

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Arena allocator complexity | Medium | Incremental rollout, fallback path |
| Batch engine breaking changes | High | Maintain row-by-row compatibility |
| SIMD portability | Low | Runtime CPU feature detection |
| PGO build complexity | Low | Optional build flag, documented |

---

## Success Metrics

1. **Performance**: 2x average speedup across all benchmarks
2. **Memory**: 40% reduction in Go GC pressure
3. **Code Quality**: No regressions in SQL:1999 tests
4. **Build Time**: <5% increase in C++ build time

---

## References

- Architecture: `docs/ARCHITECTURE.md`
- v0.11.2 Performance Analysis: `README.md`
- C++ Module Structure: Commit `8ccf4e4`
