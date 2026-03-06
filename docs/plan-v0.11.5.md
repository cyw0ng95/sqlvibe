# sqlvibe v0.11.5 Development Plan

**Version**: 0.11.5  
**Date**: 2026-03-06  
**Focus**: Complete C++ Performance Optimization  

---

## Overview

v0.11.5 completes ALL performance optimizations in a single release. Building on v0.11.4 baseline benchmarks, this plan executes 8 parallel workstreams covering JOIN engine, COUNT(*) optimization, VM dispatch, memory system, SIMD, query processing, code generation, and transaction management.

**Target Performance**:
- INNER JOIN: 60× speedup (6034ms → 100ms)
- COUNT(*): 98× speedup (4.89ms → 50µs)
- SELECT all: 5× speedup (47.8ms → 10ms)
- Overall: ~15× geometric mean improvement

---

## Baseline Performance (v0.11.5)

| Workload | Scale | SQLite | sqlvibe | Ratio | Status |
|----------|-------|--------|---------|-------|--------|
| INNER JOIN | 1K | 619 µs | 6034 ms | 9746× | 🔴 CRITICAL |
| COUNT(*) | 10K | 5.2 µs | 4.89 ms | 940× | 🔴 CRITICAL |
| SELECT all | 10K | 4.22 ms | 47.8 ms | 11.3× | 🔴 slower |
| SUM aggregate | 10K | 375 µs | 18.5 ms | 49.3× | 🔴 slower |
| GROUP BY | 10K | 3.27 ms | 49.0 ms | 15.0× | 🔴 slower |
| INSERT batch | 1K | 3.98 ms | 2.28 ms | 0.6× | 🟢 faster |

---

## Workstream Architecture

```
v0.11.5 Workstreams (Parallel Execution)
=================================================================
┌─────────────┬─────────────┬─────────────┬─────────────┐
│  WS1: JOIN │  WS2: COUNT│  WS3: VM   │  WS4: QP/CG │
│   P0       │   P0       │   P1       │   P1        │
├─────────────┼─────────────┼─────────────┼─────────────┤
│  WS5: DS   │  WS6: MEM  │  WS7: SIMD │  WS8: TM/IS │
│   P1       │   P1       │   P1       │   P2        │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

---

## Workstream 1: JOIN Engine (P0-CRITICAL)

**Target**: INNER JOIN 6034ms → 100ms (60× speedup)

### Root Causes
1. std::unordered_map<std::string, vector<size_t>> — heap allocation per key
2. normalizeKey() creates temporary std::string for every row
3. No SIMD acceleration for key comparison
4. Naive hash table with linear resize during build

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 1.1 | Implement open-addressing FastHashTable with ArenaV2 | HIGH | - |
| 1.2 | Add SIMD CRC32 hash computation | MEDIUM | 1.1 |
| 1.3 | Pre-size hash table based on cardinality | MEDIUM | 1.1 |
| 1.4 | Implement vectorized probe phase | HIGH | 1.2 |
| 1.5 | Add null-handling fast path | MEDIUM | 1.1 |
| 1.6 | Support LEFT/RIGHT/INNER/FULL joins | MEDIUM | 1.4 |
| 1.7 | Benchmark and verify 100ms target | - | 1.6 |

### Implementation

```cpp
// src/core/VM/hash_join.cpp - Complete Rewrite

struct FastHashTable {
    static constexpr size_t BUCKET_COUNT = 65536;
    static constexpr size_t MAX_PROBE = 16;
    
    uint64_t* keys;        // Hash values (Arena allocated)
    uint32_t* values;     // Row indices
    uint8_t* probes;      // Probe lengths
    size_t count;
    size_t capacity;
    ArenaV2* arena;
    
    static FastHashTable* Build(const svdb_row_t* rows, size_t n, ArenaV2* arena);
    uint32_t* Find(uint64_t hash, size_t* count);
};

extern "C" {
    svdb_join_result_t svdb_hash_join_batch_optimized(
        const svdb_row_t* left_rows, size_t left_count,
        const svdb_row_t* right_rows, size_t right_count,
        size_t left_key_col, size_t right_key_col,
        size_t num_left_cols, size_t num_right_cols,
        ArenaV2* arena
    );
}
```

### Files Modified
- src/core/VM/hash_join.cpp (rewrite)
- src/core/VM/hash_join.h (new API)
- src/core/VM/fast_hash_table.h (new)

### Expected Impact
- 60× speedup for INNER JOIN (6034ms → 100ms)

---

## Workstream 2: COUNT(*) Optimization (P0-CRITICAL)

**Target**: COUNT(*) 10K: 4.89ms → 50µs (98× speedup)

### Root Causes
1. Full B-tree leaf scan for every COUNT(*)
2. No metadata cache for row counts (infrastructure exists but disabled)
3. SIGFPE bug in cache update path (division by zero)

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 2.1 | Fix SIGFPE bug in metadata cache (division by zero) | MEDIUM | - |
| 2.2 | Implement TableMetadata with row count | MEDIUM | 2.1 |
| 2.3 | Add cache invalidation hooks | MEDIUM | 2.2 |
| 2.4 | Implement COUNT(*) fast path | MEDIUM | 2.2 |
| 2.5 | Add MVCC-safe counter with version | HIGH | 2.3 |
| 2.6 | Benchmark and verify 50µs target | - | 2.5 |

### Implementation

```cpp
// src/core/IS/is_registry.cpp + src/core/SC/query.cpp

struct TableMetadata {
    uint64_t row_count;
    uint64_t row_count_version;  // MVCC visibility
    uint32_t schema_version;
    uint64_t last_modified_txn;
    bool valid;
    
    std::atomic<uint64_t> counter;  // Thread-safe counter
};

class MetadataCache {
    std::unordered_map<std::string, TableMetadata> cache_;
    
public:
    uint64_t GetRowCount(const std::string& table, uint64_t read_version);
    void IncrementRowCount(const std::string& table);
    void DecrementRowCount(const std::string& table);
    void Invalidate(const std::string& table);
};

// Fast path in query execution
svdb_result_t ExecuteCountStar(const std::string& table, uint64_t txn_id) {
    if (auto meta = cache_.Get(table)) {
        if (meta->valid && meta->schema_version == current_schema) {
            return meta->row_count;  // O(1)
        }
    }
    return BTreeCount(table);  // Fallback
}
```

### Files Modified
- src/core/IS/is_registry.cpp
- src/core/IS/is_registry.h
- src/core/SC/query.cpp

### Expected Impact
- 98× speedup for COUNT(*) (4.89ms → 50µs)

---

## Workstream 3: VM Dispatch Optimization (P1)

**Target**: SELECT all 11× → 2× slowdown (5× improvement)

### Root Causes
1. Per-instruction switch dispatch overhead
2. No instruction batching
3. Excessive bounds checking in hot path
4. Function call overhead for each opcode handler

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 3.1 | Implement computed goto dispatch | HIGH | - |
| 3.2 | Inline hot opcode handlers (LOAD_CONST, ADD, MOVE) | MEDIUM | 3.1 |
| 3.3 | Remove redundant bounds checks in hot path | MEDIUM | 3.1 |
| 3.4 | Implement 256-instruction batch execution | HIGH | 3.2 |
| 3.5 | Add branch prediction hints | LOW | 3.3 |
| 3.6 | Benchmark and verify 2× target | - | 3.5 |

### Implementation

```cpp
// src/core/VM/vm_execute.cpp

// Computed goto dispatch
void VMExecute(Program* program) {
    static const void* opcodes[] = {
        &&OP_HALT, &&OP_LOAD_CONST, &&OP_MOVE, &&OP_ADD,
        &&OP_SUB, &&OP_MUL, &&OP_DIV, &&OP_RESULT_ROW,
        // ... all opcodes
    };
    
    const Instr* instr = program->instructions;
    goto* opcodes[instr->op];
    
OP_LOAD_CONST:
    registers[instr->p1] = constants[instr->p2];
    instr++;
    goto* opcodes[instr->op];
    
OP_ADD:
    registers[instr->p3].int_val = 
        registers[instr->p1].int_val + registers[instr->p2].int_val;
    instr++;
    goto* opcodes[instr->op];
    
    // ... other handlers
}

// Batch execution
void ExecuteBatch(VM* vm, const Instr* instrs, size_t count) {
    // Process 256 instructions without per-instruction dispatch
    svdb_value_t batch_regs[256][64];
    // Vectorized execution
}
```

### Files Modified
- src/core/VM/vm_execute.cpp
- src/core/VM/vm_execute.h

### Expected Impact
- 5× speedup for SELECT all (47.8ms → 10ms)

---

## Workstream 4: Query Processing + Code Gen (P1)

### QP Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 4.1 | Parser string_view conversion | LOW | - |
| 4.2 | Placeholder pre-scanning (O(n) → O(1)) | LOW | 4.1 |
| 4.3 | AST node pool allocator | MEDIUM | 4.1 |
| 4.4 | Implement predicate pushdown | MEDIUM | - |
| 4.5 | Loop-invariant code motion | MEDIUM | 4.4 |
| 4.6 | Constant propagation | LOW | - |
| 4.7 | Plan cache with LRU | MEDIUM | - |
| 4.8 | Index-aware opcode selection | HIGH | - |

### Implementation

```cpp
// src/core/QP/parser.cpp - string_view

struct Token {
    TokenType type;
    std::string_view text;  // Points into original SQL buffer
};

std::string_view read_ident(const std::string& sql, size_t& pos) {
    // No copy, returns view
}

// src/core/CG/optimizer.cpp - Predicate Pushdown

void OptimizePredicatePushdown(Program* program) {
    for (auto& instr : program->instructions) {
        if (instr.op == OP_FILTER && instr.predicate_is_constant) {
            // Evaluate predicate once, modify opcode to seek
            instr.op = OP_SEEK;
            // Push constant to SARGable form
        }
    }
}
```

### Files Modified
- src/core/QP/parser.cpp
- src/core/QP/parser.h
- src/core/QP/binder.cpp
- src/core/QP/tokenizer.h
- src/core/CG/optimizer.cpp
- src/core/CG/optimizer.h
- src/core/CG/bytecode_compiler.cpp
- src/core/CG/plan_cache.h (new)

### Expected Impact
- 30% faster parsing
- 2-3× speedup for complex queries

---

## Workstream 5: Data Storage (P1)

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 5.1 | B-Tree key pre-extraction buffer | MEDIUM | - |
| 5.2 | SIMD binary search | MEDIUM | 5.1 |
| 5.3 | Page prefetching | LOW | - |
| 5.4 | Bloom filter integration | MEDIUM | - |
| 5.5 | Roaring bitmap SIMD ops | MEDIUM | - |
| 5.6 | WAL batch commit | MEDIUM | WS8 |

### Implementation

```cpp
// src/core/DS/btree.cpp - SIMD Search

int btree_simd_search(const uint8_t* page_data, size_t page_size,
                      const uint8_t* key, size_t key_len) {
#ifdef __AVX2__
    // Load all cell pointers at once
    // Compare keys in parallel
    // Binary search with SIMD masks
#endif
}

// src/core/DS/page_manager_v2.cpp - Prefetch

void PrefetchPages(uint32_t* page_nums, size_t count) {
    for (size_t i = 0; i < count; i++) {
        prefetch(page_data[i], PREFETCH_HINT_T0);
    }
}
```

### Files Modified
- src/core/DS/btree.cpp
- src/core/DS/btree.h
- src/core/DS/manager.cpp
- src/core/DS/page_manager_v2.cpp
- src/core/DS/roaring.cpp

### Expected Impact
- 2× B-Tree search improvement

---

## Workstream 6: Memory System (P1)

**Target**: 40% GC reduction

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 6.1 | Hash join Arena integration | HIGH | WS1 |
| 6.2 | VM register file Arena | MEDIUM | WS3 |
| 6.3 | Engine result row Arena | MEDIUM | - |
| 6.4 | SafeArena with limits | LOW | - |
| 6.5 | String pool pre-allocation | LOW | - |
| 6.6 | CGO boundary batch transfer | HIGH | - |

### Implementation

```cpp
// src/core/DS/arena_v2.h

class SafeArena {
    std::vector<std::unique_ptr<char[]>> chunks_;
    size_t total_allocated_ = 0;
    const size_t max_size_;
    
public:
    SafeArena(size_t max_size = 64 * 1024 * 1024) : max_size_(max_size) {}
    
    void* Alloc(size_t size) {
        if (total_allocated_ + size > max_size_) {
            throw std::bad_alloc("Arena exhausted");
        }
        // Bump pointer allocation
    }
    
    void Reset() { total_allocated_ = 0; }  // O(1) free
    
    void DumpStats() const;
};

// Usage in query execution
class QueryExecutor {
    std::unique_ptr<SafeArena> query_arena_;
    
public:
    svdb_result_t Execute(const Query& q) {
        query_arena_ = std::make_unique<SafeArena>(256 * 1024);
        // All allocations use arena
        // Automatic cleanup
    }
};
```

### Files Modified
- src/core/DS/arena_v2.cpp
- src/core/DS/arena_v2.h
- src/core/SC/database.cpp (unique_ptr)
- src/core/VM/engine/engine.cpp

### Expected Impact
- 40% reduction in Go GC pressure

---

## Workstream 7: SIMD Library (P1)

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 7.1 | SIMD batch compare (int64, float64) | MEDIUM | - |
| 7.2 | SIMD aggregate (SUM, COUNT, AVG) | MEDIUM | - |
| 7.3 | SIMD hash probe | HIGH | WS1 |
| 7.4 | SIMD string comparison | MEDIUM | - |
| 7.5 | SIMD bitmap operations | MEDIUM | WS5 |
| 7.6 | CRC32/xxHash SIMD | MEDIUM | WS1 |

### Implementation

```cpp
// src/core/DS/simd.cpp - Already partially implemented, extend

void svdb_batch_compare_int64_simd(const int64_t* a, const int64_t* b,
                                    int8_t* results, size_t n) {
#ifdef __AVX2__
    for (size_t i = 0; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i cmp = _mm256_cmpgt_epi64(va, vb);
        _mm256_storeu_si256((__m256i*)&results[i], cmp);
    }
#endif
}

void svdb_batch_sum_int64_simd(const int64_t* values, int64_t* result, size_t n) sum with reduction
}
```

### Files Modified
- src {
    // Horizontal/core/DS/simd.cpp
- src/core/DS/simd.h
- src/core/VM/expr_eval.cpp
- src/core/VM/aggregate_engine.cpp
- src/core/VM/sort.cpp

### Expected Impact
- 4× speedup for batch operations

---

## Workstream 8: Transaction + Schema (P2)

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 8.1 | WAL batch commit | MEDIUM | - |
| 8.2 | Version chain optimization | MEDIUM | - |
| 8.3 | Index statistics collection | MEDIUM | - |
| 8.4 | FTS5 optimization | MEDIUM | - |
| 8.5 | Checkpoint optimization | MEDIUM | - |

### Files Modified
- src/core/TM/mvcc.cpp
- src/core/TM/transaction.cpp
- src/core/TM/wal.cpp
- src/core/IS/is_registry.cpp
- src/core/IS/schema.cpp
- src/core/IS/vtab_fts5.cpp

### Expected Impact
- 30% improvement in write throughput

---

## Workstream 9: Build + Infrastructure (P2)

### Tasks

| # | Task | Difficulty | Dependency |
|---|------|------------|------------|
| 9.1 | Enable LTO (already done in v0.11.5) | LOW | - |
| 9.2 | PGO build support | MEDIUM | - |
| 9.3 | Benchmark infrastructure | LOW | - |
| 9.4 | Performance regression CI | MEDIUM | 9.3 |

### Files Modified
- CMakeLists.txt
- src/CMakeLists.txt
- scripts/benchmark_*.sh (already created in v0.11.5)

---

## Complete File List

```
src/core/
├── VM/
│   ├── hash_join.cpp       [WS1] - COMPLETE REWRITE
│   ├── hash_join.h         [WS1] - New API
│   ├── fast_hash_table.h   [WS1] - NEW
│   ├── vm_execute.cpp      [WS3] - Dispatch optimization
│   ├── aggregate_engine.cpp [WS3] - SIMD aggregation
│   ├── sort.cpp            [WS3] - Hybrid sort
│   └── expr_eval.cpp       [WS7] - SIMD batch ops
├── DS/
│   ├── btree.cpp           [WS5] - SIMD search, metadata
│   ├── manager.cpp          [WS5] - Row count tracking
│   ├── page_manager_v2.cpp  [WS5] - Prefetch
│   ├── simd.cpp             [WS7] - Extend SIMD
│   ├── simd.h              [WS7] - New functions
│   ├── arena_v2.cpp        [WS6] - SafeArena
│   ├── arena_v2.h          [WS6] - SafeArena
│   ├── roaring.cpp         [WS5] - SIMD bitmaps
│   └── cache_v2.cpp        [WS5] - Bloom filter
├── QP/
│   ├── parser.cpp          [WS4] - string_view
│   ├── parser.h            [WS4] - string_view
│   ├── binder.cpp          [WS4] - O(n) placeholder
│   └── tokenizer.h         [WS4] - Token caching
├── CG/
│   ├── optimizer.cpp       [WS4] - Predicate pushdown
│   ├── optimizer.h         [WS4] - New passes
│   ├── bytecode_compiler.cpp [WS4] - Optimization
│   └── plan_cache.h       [WS4] - NEW
├── IS/
│   ├── is_registry.cpp     [WS2] - Metadata cache fix
│   ├── is_registry.h       [WS2] - Metadata struct
│   ├── schema.cpp         [WS8] - Statistics
│   └── vtab_fts5.cpp      [WS8] - FTS5 opt
├── SC/
│   ├── query.cpp          [WS2] - COUNT fast path
│   ├── database.cpp       [WS6] - unique_ptr
│   └── statement.cpp      [WS5] - Cache
├── TM/
│   ├── mvcc.cpp           [WS8] - Version chain
│   ├── transaction.cpp    [WS8] - WAL batch
│   └── wal.cpp            [WS8] - Batch commit
└── PB/
    └── vfs.cpp            [WS5] - mmap support
```

---

## Milestone Schedule

| Week | Focus | Deliverables |
|------|-------|--------------|
| **Week 1** | WS1 + WS2 | Hash join rewrite, COUNT(*) fix |
| **Week 2** | WS3 + WS6 | VM dispatch, Arena integration |
| **Week 3** | WS4 + WS7 | Parser, Optimizer, SIMD |
| **Week 4** | WS5 + WS8 | DS optimization, TM/IS |
| **Week 5** | Integration | Full benchmark suite |
| **Week 6** | Testing | SQL:1999 regression, bug fixes |
| **Week 7** | Polish | Documentation, release |

---

## Parallel Development Strategy

```bash
# Team Structure (4 parallel tracks)
Track A (P0):     JOIN + COUNT     → 2 engineers
Track B (P1):     VM + MEM + SIMD  → 2 engineers  
Track C (P1):     QP + CG + DS    → 2 engineers
Track D (P2):     TM + IS + Infra  → 1 engineer

# Daily Integration
- Morning: Standup, sync on blockers
- Evening: Merge to integration branch
- Daily: Full benchmark run

# Weekly Milestone
- Monday: Feature freeze for milestone
- Friday: Benchmark comparison, bug bash
```

---

## Success Criteria

| Metric | Baseline | Target | Improvement |
|--------|----------|--------|-------------|
| INNER JOIN 1K | 6034 ms | 100 ms | **60×** |
| COUNT(*) 10K | 4.89 ms | 50 µs | **98×** |
| SELECT all 10K | 47.8 ms | 10 ms | **5×** |
| SUM aggregate 10K | 18.5 ms | 2 ms | **9×** |
| GROUP BY 10K | 49.0 ms | 5 ms | **10×** |
| INSERT batch 1K | 2.28 ms | 1.5 ms | **1.5×** (maintain) |
| Go GC Pressure | Baseline | -40% | **40% reduction** |
| **Geometric Mean** | - | - | **~15× faster** |

---

## Benchmark Verification

### Required Tests Per Workstream

| Workstream | Test | Pass Criteria |
|------------|------|---------------|
| WS1: JOIN | BenchmarkCompare_InnerJoin_1K | < 100ms |
| WS2: COUNT | BenchmarkCompare_CountStar_10K | < 50µs |
| WS3: VM | BenchmarkCompare_SelectAll_10K | < 10ms |
| WS4: QP/CG | Parser microbenchmark | 30% faster |
| WS5: DS | B-Tree search microbenchmark | 2× faster |
| WS6: MEM | Memory profile | -40% allocations |
| WS7: SIMD | Batch ops microbenchmark | 4× faster |
| WS8: TM/IS | Write benchmark | 30% faster |

### Full Benchmark Suite

```bash
# Run complete benchmark
./scripts/benchmark_collect.sh

# Compare against baseline
python3 scripts/benchmark_compare.py \
    .build/benchmarks/baseline_v0.11.5.csv \
    .build/benchmarks/after_v0.11.5.csv
```

---

## Regression Prevention

### Test Suite Requirements

- [ ] SQL:1999 test suite (89+ suites)
- [ ] Regression tests (all previously fixed bugs)
- [ ] Edge cases: NULL handling, empty tables, large values
- [ ] Concurrency: multi-threaded access
- [ ] Recovery: crash during transaction

### CI/CD Integration

```yaml
# .github/workflows/perf.yml
name: Performance Regression
on: [push, pull_request]
jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build
        run: ./build.sh
      - name: Run benchmarks
        run: ./scripts/benchmark_collect.sh
      - name: Compare
        run: python3 scripts/benchmark_compare.py baseline.csv current.csv
      - name: Alert on regression
        if: failure()
        run: echo "Performance regression detected"
```

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Hash join rewrite complexity | HIGH | Incremental rollout, keep fallback path |
| Arena allocator memory leaks | MEDIUM | Per-query lifecycle, audit all allocations |
| VM dispatch breaking changes | HIGH | Maintain switch fallback, gradual migration |
| COUNT(*) SIGFPE regression | HIGH | Extensive edge case testing |
| Schedule slip | MEDIUM | Buffer 1 week, cut P2 if needed |
| SIMD portability | LOW | Runtime CPU feature detection, scalar fallback |

---

## Commit Strategy

### Per-Workstream Commits

```bash
# Workstream 1: Hash Join
git add src/core/VM/hash_join.cpp src/core/VM/fast_hash_table.h
git commit -m "perf: Rewrite hash join with Arena + SIMD

- Open-addressing hash table implementation
- ArenaV2 for zero-copy allocations
- SIMD CRC32 hash computation
- Target: 60x speedup (6034ms -> 100ms)
Test: BenchmarkCompare_InnerJoin_1K"

# Workstream 2: COUNT(*)
git add src/core/IS/is_registry.cpp src/core/SC/query.cpp
git commit -m "perf: Fix COUNT(*) metadata cache

- Fix SIGFPE division by zero bug
- Implement thread-safe row counter
- Add MVCC-safe cache validation
- Target: 98x speedup (4.89ms -> 50us)
Test: BenchmarkCompare_CountStar_10K"
```

### Weekly Integration Commits

```bash
# Friday integration
git add -A
git commit -m "perf: v0.11.5 Week 1 integration

- WS1: Hash join rewrite (60% complete)
- WS2: COUNT(*) fix (80% complete)
- Benchmark: INNER JOIN 6034ms -> 800ms (7.5x)
"
```

---

## References

- v0.11.5 Plan: docs/plan-v0.11.5.md
- Architecture: docs/ARCHITECTURE.md
- Benchmark Infrastructure: scripts/benchmark_*.sh
- C++ Module Structure: src/core/
- Hash Join Implementation: src/core/VM/hash_join.cpp
- Arena Allocator: src/core/DS/arena_v2.h

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.11.5 | 2026-03-06 | sqlvibe team | Initial plan |

