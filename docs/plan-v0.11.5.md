# sqlvibe v0.11.5 Development Plan

**Version**: v0.11.5  
**Date**: 2026-03-06  
**Focus**: C++ Performance Optimization & Module Structure  

---

## Overview

v0.11.5 focuses on optimizing the C++ core engine after the module structure reorganization (v0.11.4). This plan establishes a comprehensive performance baseline with detailed benchmarks, root cause analysis, and prioritized optimization targets for v0.11.5 → v0.11.6.

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

## Performance Baseline (v0.11.5)

**Test Hardware**: 13th Gen Intel(R) Core(TM) i7-13650HX (20 cores), Linux, Go 1.21+  
**Benchmarks**: In-memory database, `-benchtime=500ms` for 1K/10K tests

---

## Benchmark Execution Guide

### Running Benchmarks

```bash
# Build with all extensions
./build.sh

# Run full benchmark suite
./build.sh -b

# Run specific benchmarks
go test ./tests/Benchmark/... -bench=BenchmarkCompare_ -benchmem -benchtime=1s

# Run with CPU profiling
go test ./tests/Benchmark/... -bench=BenchmarkCompare_ -cpuprofile=cpu.prof -memprofile=mem.prof

# Run single workload for detailed analysis
go test ./tests/Benchmark/... -bench=BenchmarkCompare_SelectAll_1K -benchmem -v
```

### Automated Benchmark Collection Script

Create `scripts/benchmark_collect.sh`:

```bash
#!/bin/bash
# benchmark_collect.sh - Collect performance data for analysis

set -euo pipefail

OUTPUT_DIR=".build/benchmarks"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="${OUTPUT_DIR}/benchmark_${TIMESTAMP}.csv"

mkdir -p "$OUTPUT_DIR"

echo "Collecting benchmarks on: $(uname -a)"
echo "Go version: $(go version)"
echo "Output: ${OUTPUT_FILE}"

# Header
echo "workload,scale,sqlite_ns,sqlvibe_ns,ratio,status" > "$OUTPUT_FILE"

# Run benchmarks and parse results
go test ./tests/Benchmark/... \
    -bench=BenchmarkCompare_ \
    -benchmem \
    -benchtime=1s \
    -count=5 \
    -run=^$ \
    2>&1 | tee "${OUTPUT_FILE%.csv}.txt" | \
    grep -E "BenchmarkCompare" | \
    awk '{
        # Parse: BenchmarkCompare_SelectAll_1K_SQLite-20  1388  441523 ns/op
        name=$1; iters=$2; ns=$3;
        gsub(/-.*$/, "", name);  # Remove -20 suffix
        gsub(/_SQLite$/, "", name);
        print name","ns
    }' >> "$OUTPUT_FILE"

echo "Benchmark collection complete: $OUTPUT_FILE"
```

### Benchmark Data Analysis Script

Create `scripts/benchmark_analyze.py`:

```python
#!/usr/bin/env python3
# benchmark_analyze.py - Analyze benchmark results

import csv
import sys
from collections import defaultdict

def analyze_benchmark(csv_file):
    data = defaultdict(dict)
    
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            workload = row['workload']
            scale = row['scale']
            ratio = float(row['ratio'])
            data[f"{workload}_{scale}"] = ratio
    
    # Sort by slowdown
    sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)
    
    print("=" * 60)
    print("BENCHMARK ANALYSIS - Sorted by Slowdown")
    print("=" * 60)
    
    for (name, ratio) in sorted_data:
        status = "🔴 CRITICAL" if ratio > 100 else "🟡 HIGH" if ratio > 10 else "🟢 OK"
        print(f"{name:30s} {ratio:8.1f}×  {status}")
    
    # Calculate priority scores
    print("\n" + "=" * 60)
    print("OPTIMIZATION PRIORITIES")
    print("=" * 60)
    
    critical = [(n, r) for n, r in sorted_data if r > 100]
    high = [(n, r) for n, r in sorted_data if 10 < r <= 100]
    
    if critical:
        print("\nP0-CRITICAL (100×+ slowdown):")
        for name, ratio in critical:
            print(f"  - {name}: {ratio:.0f}×")
    
    if high:
        print("\nP1-HIGH (10-100× slowdown):")
        for name, ratio in high:
            print(f"  - {name}: {ratio:.1f}×")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: benchmark_analyze.py <benchmark.csv>")
        sys.exit(1)
    analyze_benchmark(sys.argv[1])
```

### Usage Workflow

```bash
# 1. Collect baseline benchmarks
./scripts/benchmark_collect.sh

# 2. Analyze results
python3 scripts/benchmark_analyze.py .build/benchmarks/benchmark_20260306_120000.csv

# 3. Implement optimization (e.g., hash join fast path)
# ... code changes ...

# 4. Re-run benchmarks
./scripts/benchmark_collect.sh

# 5. Compare before/after
python3 scripts/benchmark_compare.py \
    .build/benchmarks/benchmark_before.csv \
    .build/benchmarks/benchmark_after.csv
```

### Benchmark Comparison Script

Create `scripts/benchmark_compare.py`:

```python
#!/usr/bin/env python3
# benchmark_compare.py - Compare before/after optimization

import csv
import sys

def compare_benchmarks(before_file, after_file):
    before = {}
    after = {}
    
    with open(before_file) as f:
        for row in csv.DictReader(f):
            key = f"{row['workload']}_{row['scale']}"
            before[key] = float(row['sqlvibe_ns'])
    
    with open(after_file) as f:
        for row in csv.DictReader(f):
            key = f"{row['workload']}_{row['scale']}"
            after[key] = float(row['sqlvibe_ns'])
    
    print("=" * 70)
    print("BENCHMARK COMPARISON - Before vs After Optimization")
    print("=" * 70)
    print(f"{'Workload':<25s} {'Before':>12s} {'After':>12s} {'Speedup':>10s} {'Status':<8s}")
    print("-" * 70)
    
    improvements = []
    regressions = []
    
    for key in sorted(before.keys()):
        if key not in after:
            continue
        
        before_ns = before[key]
        after_ns = after[key]
        speedup = before_ns / after_ns if after_ns > 0 else float('inf')
        
        if speedup > 1.0:
            status = "✅"
            improvements.append((key, speedup))
        elif speedup < 1.0:
            status = "⚠️"
            regressions.append((key, 1.0/speedup))
        else:
            status = "➡️"
        
        before_ms = before_ns / 1e6
        after_ms = after_ns / 1e6
        print(f"{key:<25s} {before_ms:>10.2f}ms {after_ms:>10.2f}ms {speedup:>8.2f}× {status}")
    
    print("-" * 70)
    
    if improvements:
        print("\n✅ IMPROVEMENTS:")
        for name, speedup in sorted(improvements, key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {name}: {speedup:.2f}× faster")
    
    if regressions:
        print("\n⚠️ REGRESSIONS:")
        for name, speedup in sorted(regressions, key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {name}: {speedup:.2f}× slower")
    
    # Summary
    geo_mean = 1.0
    count = 0
    for key in before:
        if key in after:
            ratio = before[key] / after[key]
            geo_mean *= ratio
            count += 1
    
    if count > 0:
        geo_mean = geo_mean ** (1.0 / count)
        print(f"\n📊 GEOMETRIC MEAN SPEEDUP: {geo_mean:.2f}×")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: benchmark_compare.py <before.csv> <after.csv>")
        sys.exit(1)
    compare_benchmarks(sys.argv[1], sys.argv[2])
```

### Profiling Guide

```bash
# CPU profiling - identify hot spots
go test ./tests/Benchmark/... \
    -bench=BenchmarkCompare_InnerJoin_1K \
    -cpuprofile=cpu.prof \
    -benchtime=30s

# Analyze CPU profile
go tool pprof -http=:8080 cpu.prof

# Memory profiling - find allocations
go test ./tests/Benchmark/... \
    -bench=BenchmarkCompare_SelectAll_1K \
    -memprofile=mem.prof \
    -benchtime=30s

# Analyze memory profile
go tool pprof -http=:8080 mem.prof

# Trace profiling - execution timeline
go test ./tests/Benchmark/... \
    -bench=BenchmarkCompare_SUM_1K \
    -trace=trace.out \
    -benchtime=30s

# View trace
go tool trace trace.out
```

### Benchmark Data Collection Checklist

- [ ] Run `./build.sh` to ensure clean build
- [ ] Close background applications (reduce noise)
- [ ] Run each benchmark 5 times (`-count=5`)
- [ ] Collect CPU and memory profiles for critical workloads
- [ ] Save raw output (`.txt`) and parsed data (`.csv`)
- [ ] Document hardware/OS/Go version in output file
- [ ] Compare against baseline after each optimization
- [ ] Update README.md with new results

---

## Performance Baseline Results (v0.11.5)

### Full Benchmark Results

| Workload | Scale | SQLite | sqlvibe | Ratio | Status |
|----------|-------|--------|---------|-------|--------|
| **SELECT all** | 1K | 442 µs | 4.17 ms | 9.4× | 🔴 slower |
| **SELECT all** | 10K | 4.22 ms | 47.8 ms | 11.3× | 🔴 slower |
| **COUNT(*)** | 1K | 3.9 µs | 510 µs | 130.8× | 🔴 slower |
| **COUNT(*)** | 10K | 5.2 µs | 4.89 ms | 940.4× | 🔴 slower |
| **SUM aggregate** | 1K | 43 µs | 1.90 ms | 44.2× | 🔴 slower |
| **SUM aggregate** | 10K | 375 µs | 18.5 ms | 49.3× | 🔴 slower |
| **GROUP BY** (4 groups) | 1K | 312 µs | 4.75 ms | 15.2× | 🔴 slower |
| **GROUP BY** (4 groups) | 10K | 3.27 ms | 49.0 ms | 15.0× | 🔴 slower |
| **INSERT batch** | 1K | 3.98 ms | 2.28 ms | **1.7×** | 🟢 faster |
| **INNER JOIN** | 1K | 619 µs | 6034 ms | 9746.4× | 🔴 slower |

### Scalability Analysis (Slowdown Factor vs SQLite)

| Workload | 1K Slowdown | 10K Slowdown | Scaling |
|----------|-------------|--------------|---------|
| SELECT all | 9.4× | 11.3× | +20% |
| COUNT(*) | 130.8× | 940.4× | +619% ⚠️ |
| SUM aggregate | 44.2× | 49.3× | +12% |
| GROUP BY | 15.2× | 15.0× | -1% ✓ |
| INSERT batch | 0.6× | N/A | N/A |

### Key Insights

- **INSERT batch**: 1.7× faster — only workload where sqlvibe leads (C++ direct insert fast path)
- **GROUP BY**: Best scaling behavior (-1% slowdown at 10K, stable ratio)
- **COUNT(*)**: Worst scaling (940× slower at 10K) — needs index-only scan optimization
- **INNER JOIN**: Critical bottleneck (9746× slower) — hash join needs urgent optimization
- **SELECT all**: Moderate overhead (9-11×) — bytecode VM dispatch cost

---

## Optimization Priorities (v0.11.5 → v0.11.6)

Based on benchmark analysis, here are the prioritized optimization targets:

| Priority | Area | Current | Target | Impact |
|----------|------|---------|--------|--------|
| **P0-CRITICAL** | INNER JOIN | 6034 ms | 100 ms | 60× speedup |
| **P0-CRITICAL** | COUNT(*) 10K | 4.89 ms | 50 µs | 98× speedup |
| **P1-HIGH** | SELECT all 10K | 47.8 ms | 5 ms | 10× speedup |
| **P1-HIGH** | SUM aggregate 10K | 18.5 ms | 500 µs | 37× speedup |
| **P2** | GROUP BY 10K | 49.0 ms | 3 ms | 16× speedup |

---

## Root Cause Analysis

| Bottleneck | Component | Root Cause | Solution |
|------------|-----------|------------|----------|
| VM dispatch | VM/exec.cpp | Per-instruction function call overhead | Batch execution, inline dispatch |
| Hash join | VM/hash_join.cpp | Naive hash table, no SIMD | Hash table tuning, SIMD probes |
| COUNT(*) | DS/btree.cpp | Full table scan | Index-only scan, metadata cache |
| Memory alloc | Multiple | malloc/new per-row | Arena allocator (ArenaV2) |
| GC pressure | Go runtime | CGO → Go boundary allocations | Reduce crossing, batch results |

---

## Memory Management Strategy: Hybrid Approach

### Smart Pointers vs Arena Allocators

**Analysis**: Smart pointers solve ownership, not performance. For sqlvibe's optimization targets (60× JOIN, 98× COUNT), arena allocators are necessary in hot paths. Use smart pointers to *own* arenas, not for per-row allocations.

| Type | Memory Overhead | CPU Overhead | Best Use Case |
|------|-----------------|--------------|---------------|
| `unique_ptr` | 8 bytes (pointer) | Minimal | Single ownership, RAII |
| `shared_ptr` | 16-32 bytes (control block) | Atomic refcount | Shared ownership |
| `weak_ptr` | 16 bytes | Atomic refcount | Non-owning references |
| **Arena** | **0 bytes** (bulk) | **~5ns** (pointer bump) | **Query execution, hot paths** |

### Allocation Comparison

```cpp
// Current: ~2000 allocations per 1K rows (hash join)
std::unordered_map<std::string, std::vector<size_t>> hashTable;
for (size_t i = 0; i < right_count; i++) {
    std::string key = normalizeKey(keyVal, keyLen);  // malloc #1
    hashTable[key].push_back(i);  // vector realloc #2-N
}

// With unique_ptr: Same allocations + 8 bytes/entry overhead
std::unordered_map<std::string, std::unique_ptr<std::vector<size_t>>>;

// With Arena: 1 allocation for entire hash table
void* mem = arena->Alloc(sizeof(FastHashTable));
FastHashTable* table = new(mem) FastHashTable();
```

### Hybrid Strategy

#### Use Arena Allocators For (Hot Paths)

| Component | Reason | Files | Expected Impact |
|-----------|--------|-------|-----------------|
| Hash join build/probe | 1000s of rows, short-lived | `VM/hash_join.cpp` | 60× speedup |
| Query result rows | Bulk allocation, query lifetime | `VM/engine/engine.cpp` | 40% GC reduction |
| VM registers | Fixed size, batch execution | `VM/vm_execute.cpp` | 5× dispatch speedup |
| String copies | Per-column, eliminated by string_view | `VM/vm_opcode.cpp` | 15% latency improvement |

#### Use Smart Pointers For (Long-lived/Complex Ownership)

| Component | Type | Reason | Files |
|-----------|------|--------|-------|
| Database handle | `unique_ptr` | Single owner, clear lifecycle | `SC/database.cpp` |
| Statement cache | `unique_ptr` | Cache owns statements | `SC/statement.cpp` |
| Virtual table modules | `shared_ptr` | Shared between queries | `IS/vtab_api.cpp` |
| Transaction state | `unique_ptr` | Clear ownership | `TM/transaction.cpp` |
| Arena instances | `unique_ptr` | RAII cleanup | All query executors |

### Recommended Pattern: Smart Pointer + Arena

```cpp
// Query executor: unique_ptr owns arena, arena owns query memory
class QueryExecutor {
    std::unique_ptr<ArenaV2> query_arena;  // Smart pointer for arena ownership
    
public:
    svdb_result_t Execute(const Query& query) {
        // Arena for bulk query allocations (freed when query completes)
        query_arena = std::make_unique<ArenaV2>(256 * 1024);
        
        // Hash join uses arena - no per-row malloc
        auto* hash_table = FastHashTable::Build(
            query.left_rows, 
            query.right_rows, 
            query_arena.get()  // Arena passed to hot path
        );
        
        // Results copied to arena memory
        svdb_result_t result = hash_table->ExecuteJoin(query_arena.get());
        
        // Arena freed here - all query memory released in O(1)
        query_arena.reset();
        return result;
    }
};

// Database handle: unique_ptr for clear ownership
class Database {
    std::unique_ptr<PageManager> pm_;
    std::unique_ptr<MetadataCache> cache_;
    
public:
    Database() 
        : pm_(std::make_unique<PageManager>())
        , cache_(std::make_unique<MetadataCache>()) 
    {}
    // Automatic cleanup via RAII, no manual delete
};
```

### Safe Arena Implementation

```cpp
// ArenaV2 with smart pointer integration and safety limits
class SafeArena {
    std::vector<std::unique_ptr<char[]>> chunks_;  // Smart pointer owns chunks
    size_t total_allocated_ = 0;
    const size_t max_size_;
    
public:
    SafeArena(size_t max_size = 64 * 1024 * 1024) 
        : max_size_(max_size) {}
    
    // Automatic cleanup via unique_ptr destructor
    ~SafeArena() = default;  // chunks_ freed automatically
    
    void* Alloc(size_t size) {
        if (total_allocated_ + size > max_size_) {
            throw std::bad_alloc("Arena size limit exceeded");
        }
        // ... allocation logic
    }
    
    // Debug: track allocations
    void DumpStats() const {
        printf("Arena: %zu bytes in %zu chunks\n", 
               total_allocated_, chunks_.size());
    }
    
    // Reset for reuse (O(1) free without deallocating chunks)
    void Reset() {
        total_allocated_ = 0;
        // chunks_ retained for next query
    }
};
```

### Memory Strategy by Priority

| Priority | Component | Strategy | Rationale |
|----------|-----------|----------|-----------|
| P0 | Hash join | **Arena** | 60× speedup requires eliminating malloc |
| P0 | COUNT(*) cache | **unique_ptr** | Long-lived, clear ownership |
| P1 | VM dispatch | **Arena** | Batch execution needs contiguous memory |
| P1 | Engine row copies | **Arena** | 40% GC reduction target |
| P2 | Database handle | **unique_ptr** | RAII, no performance impact |
| P2 | VTab modules | **shared_ptr** | Shared between concurrent queries |

### Tasks

- [ ] Audit all `malloc`/`new` calls in hot paths
- [ ] Replace per-query allocations with ArenaV2
- [ ] Wrap arena instances in `std::unique_ptr`
- [ ] Add `SafeArena` with size limits and debug stats
- [ ] Document memory ownership in code comments

**Expected Impact**: Zero memory leaks (RAII), 40% GC reduction (arenas), improved code clarity (smart pointers for ownership)

---

## Detailed Optimization Plans

### P0-CRITICAL: Hash Join Optimization

**Current Performance**: 6034 ms for 1K rows (9746× slower than SQLite)  
**Target**: 100 ms (60× speedup)

**Root Causes**:
1. `std::unordered_map<std::string, vector<size_t>>` — heap allocation per key
2. `normalizeKey()` creates temporary `std::string` for every row
3. No SIMD acceleration for key comparison
4. Naive hash table with linear resize during build phase
5. No pre-sizing based on input cardinality

**Files to Modify**:
- `src/core/VM/hash_join.cpp` (complete rewrite)
- `src/core/VM/hash_join.h` (new API)
- `src/core/VM/fast_hash_table.h` (new)
- `src/core/VM/hash_table_prober.h` (new)

**Implementation Plan**:

```cpp
// Current (hash_join.cpp:50-70)
std::unordered_map<std::string, std::vector<size_t>> hashTable;
hashTable.reserve(right_count);

for (size_t i = 0; i < right_count; i++) {
    std::string key = normalizeKey(keyVal, keyLen);  // HEAP ALLOC
    hashTable[key].push_back(i);  // HEAP ALLOC for vector growth
}

// Optimized: Open-addressing hash table with SIMD
struct FastHashTable {
    static constexpr size_t BUCKET_COUNT = 65536;  // Power of 2
    static constexpr size_t MAX_PROBE = 16;
    
    uint64_t keys[BUCKET_COUNT];      // Hash values (not strings)
    uint32_t values[BUCKET_COUNT];    // Row indices
    uint8_t probes[BUCKET_COUNT];     // Probe lengths
    
    void Insert(uint64_t hash, uint32_t row_idx);
    uint32_t* Find(uint64_t hash, size_t* count);
};

// SIMD key comparison (16 bytes at once)
inline uint64_t compute_hash_simd(const char* data, size_t len) {
#ifdef __AVX2__
    __m128i v = _mm_loadu_si128((const __m128i*)data);
    return _mm_crc32_u64(0, _mm_extract_epi64(v, 0));
#endif
}
```

**Tasks**:
- [ ] Design `FastHashTable` with open addressing
- [ ] Implement SIMD hash computation
- [ ] Pre-size hash table based on input cardinality
- [ ] Use ArenaV2 for hash table storage
- [ ] Add vectorized probe phase
- [ ] Benchmark: target 100ms for 1K rows

**Expected Impact**: 60× speedup for INNER JOIN (6034ms → 100ms)

---

### P0-CRITICAL: COUNT(*) Index-Only Scan

**Current Performance**: 4.89 ms for 10K rows (940× slower than SQLite)  
**Target**: 50 µs (98× speedup)

**Root Causes**:
1. Full B-tree leaf scan for every COUNT(*)
2. No metadata cache for row counts
3. Row materialization even though only count is needed
4. No index-only scan path in query engine

**Files to Modify**:
- `src/core/DS/btree.cpp` (add metadata methods)
- `src/core/DS/manager.cpp` (add row count tracking)
- `src/core/IS/is_registry.h` (add metadata cache)
- `src/core/SC/query.cpp` (add COUNT(*) fast path)

**Implementation Plan**:

```cpp
// Add metadata cache for table row counts
struct TableMetadata {
    uint64_t row_count;
    uint32_t schema_version;
    uint64_t last_modified_counter;
    bool valid;
};

class MetadataCache {
    std::unordered_map<std::string, TableMetadata> cache_;
    
public:
    uint64_t GetRowCount(const std::string& table);
    void Invalidate(const std::string& table);
    void Update(const std::string& table, int64_t delta);
};

// Add COUNT(*) fast path in query engine
svdb_result_t execute_count_star(const std::string& table) {
    if (metadata_cache_.HasValidCount(table)) {
        return metadata_cache_.GetRowCount(table);  // O(1) lookup
    }
    // Fallback to B-tree scan
    return btree_count_all(table);
}
```

**Tasks**:
- [ ] Add `TableMetadata` struct with row count
- [ ] Implement `MetadataCache` class
- [ ] Add hooks to update count on INSERT/DELETE
- [ ] Add COUNT(*) fast path in query engine
- [ ] Invalidate cache on schema changes
- [ ] Benchmark: target 50µs for 10K rows

**Expected Impact**: 98× speedup for COUNT(*) (4.89ms → 50µs)

---

### P1-HIGH: VM Dispatch Optimization

**Current Performance**: 9-11× slower for SELECT all  
**Target**: 2× slower (5× improvement)

**Root Causes**:
1. Per-instruction switch dispatch overhead
2. No instruction batching
3. Excessive bounds checking in hot path
4. Function call overhead for each opcode handler

**Files to Modify**:
- `src/core/VM/vm_execute.cpp` (dispatch loop)
- `src/core/VM/vm_opcode.h` (inline handlers)
- `src/core/VM/dispatch.h` (new)

**Implementation Plan**:

```cpp
// Current: Switch dispatch
for (size_t pc = 0; pc < program->num_instructions; pc++) {
    const Instruction* instr = &program->instructions[pc];
    switch (instr->op) {
        case OP_LOAD_CONST: exec_load_const(instr); break;
        case OP_ADD: exec_add(instr); break;
        // ...
    }
}

// Optimized: Computed goto (threaded code)
#define DISPATCH() goto* opcodes[instr->op]
#define NEXT() ((instr++), DISPATCH())

void execute(Program* program) {
    static const void* opcodes[] = {
        &&op_load_const, &&op_add, &&op_sub, ...
    };
    
    const Instruction* instr = program->instructions;
    DISPATCH();
    
    op_load_const:
        // inline handler
        NEXT();
    op_add:
        // inline handler
        NEXT();
    // ...
}

// Or: Batch execution (process 256 instructions at once)
struct BatchVM {
    static constexpr int BATCH_SIZE = 256;
    svdb_value_t registers[BATCH_SIZE][64];
    
    void ExecuteBatch(const Instruction* instrs, size_t count);
};
```

**Tasks**:
- [ ] Implement computed goto dispatch
- [ ] Inline hot opcode handlers (LOAD_CONST, ADD, MOVE)
- [ ] Remove redundant bounds checks in hot path
- [ ] Add batch execution mode for 256 instructions
- [ ] Benchmark: target 2× slowdown (from 11×)

**Expected Impact**: 5× speedup for SELECT all (11× → 2× slowdown)

---

### P1-HIGH: Memory Arena Integration (with Smart Pointers)

**Problem**: Mixed allocation strategies (`new[]`, `malloc`, `free`) cause fragmentation and GC pressure

**Strategy**: Hybrid approach — arenas for hot paths (query execution), smart pointers for ownership (database, caches)

**Files to Modify**:
- `src/core/VM/hash_join.cpp`
- `src/core/VM/engine/engine.cpp`
- `src/core/VM/vm_opcode.cpp`
- `src/core/DS/arena_v2.h` (extend with SafeArena)
- `src/core/DS/arena_v2.cpp` (extend)
- `src/core/SC/database.cpp` (add unique_ptr)

**Implementation**:

```cpp
// Current (hash_join.cpp:113-117)
svdb_row_t* new_rows = new svdb_row_t[result.capacity];
std::copy(result.rows, result.rows + result.capacity, new_rows);
delete[] result.rows;

// Optimized: Use arena allocator
void* mem = arena->Alloc(new_capacity * sizeof(svdb_row_t));
svdb_row_t* new_rows = new(mem) svdb_row_t[new_capacity];

// Current (engine/engine.cpp:18-30)
static svdb_value_t copy_val(const svdb_value_t* v) {
    char* p = (char*)malloc(v->str_len + 1);  // MALLOC
    memcpy(p, v->str_data, v->str_len);
    // ...
}

// Optimized: Arena allocation
char* p = (char*)arena->Alloc(v->str_len + 1);
memcpy(p, v->str_data, v->str_len);
```

**Tasks**:
- [ ] Audit all `malloc`/`new` calls in hot paths (hash_join, engine, vm_opcode)
- [ ] Extend `ArenaV2` with `SafeArena` features (size limits, debug stats)
- [ ] Add per-query arena lifecycle (begin/end) with `std::unique_ptr<ArenaV2>`
- [ ] Replace `new[]`/`delete[]` in hash_join.cpp with arena allocation
- [ ] Replace `malloc`/`free` in engine/engine.cpp with arena allocation
- [ ] Replace `malloc`/`free` in vm_opcode.cpp with arena allocation
- [ ] Wrap Database members in `std::unique_ptr` (PageManager, MetadataCache)
- [ ] Benchmark GC pressure reduction

**Expected Impact**: Zero memory leaks (RAII), 40% reduction in Go GC pressure, 15% latency improvement

---

### P1: SIMD Expansion

**Problem**: SIMD only used for basic vector ops, not query execution

**Files to Modify**:
- `src/core/VM/compare.cpp`
- `src/core/VM/aggregate.cpp`
- `src/core/VM/hash_join.cpp`
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
    for (; i < n; i++) {
        results[i] = (a[i] == b[i]) ? 1 : 0;
    }
#endif
}

// Add SIMD batch SUM (VM/aggregate.cpp)
void svdb_batch_sum_int64(const int64_t* values, uint8_t* mask,
                          int64_t* result, size_t n) {
#ifdef __AVX2__
    __m256i acc = _mm256_setzero_si256();
    for (size_t i = 0; i + 4 <= n; i += 4) {
        __m256i v = _mm256_loadu_si256((const __m256i*)&values[i]);
        acc = _mm256_add_epi64(acc, v);
    }
    *result = _mm256_extract_epi64(acc, 0)
            + _mm256_extract_epi64(acc, 1)
            + _mm256_extract_epi64(acc, 2)
            + _mm256_extract_epi64(acc, 3);
#endif
}
```

**Tasks**:
- [ ] Add batch compare SIMD functions
- [ ] Add batch aggregate SIMD functions (SUM, COUNT, AVG)
- [ ] Add SIMD hash probe functions
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

### [ ] 1. Enable LTO
- **Files**: `CMakeLists.txt`
- **Effort**: 2 hours
- **Impact**: 10-15% overall

### [ ] 2. Arena for hash_join
- **Files**: `VM/hash_join.cpp`, `DS/arena_v2.h`
- **Effort**: 1 day
- **Impact**: 40% GC reduction

### [ ] 3. Parser string_view
- **Files**: `QP/parser.cpp`, `QP/parser.h`
- **Effort**: 1 day
- **Impact**: 30% faster parsing

### [ ] 4. SIMD batch compare
- **Files**: `VM/compare.cpp`, `DS/simd.cpp`
- **Effort**: 2 days
- **Impact**: 4x batch comparisons

---

## Updated Milestones

### M0: Baseline Collection (Day 1-2)
- [ ] Create `scripts/benchmark_collect.sh`
- [ ] Create `scripts/benchmark_analyze.py`
- [ ] Create `scripts/benchmark_compare.py`
- [ ] Collect v0.11.5 baseline (5 runs each)
- [ ] Generate CPU/memory profiles for top 3 bottlenecks
- [ ] Save results to `.build/benchmarks/baseline_v0.11.5.csv`

### M1: Critical Fixes (Week 1-2)
- [ ] Hash join fast path (60× target)
- [ ] COUNT(*) metadata cache (98× target)
- [ ] Enable LTO (10-15% free performance)
- [ ] **Benchmark**: Run `./scripts/benchmark_collect.sh` after each fix
- [ ] **Analyze**: Compare against baseline with `benchmark_compare.py`

### M2: VM Optimization (Week 3-4)
- [ ] Computed goto dispatch (5× target)
- [ ] Arena integration (40% GC reduction)
- [ ] Inline hot opcode handlers
- [ ] **Benchmark**: Collect memory profiles to verify GC reduction
- [ ] **Profile**: CPU profiles to verify dispatch overhead reduced

### M3: SIMD & Batch (Week 5-6)
- [ ] SIMD batch compare/aggregate (4× target)
- [ ] Batch execution engine (5-10× analytical)
- [ ] Hash table SIMD probes
- [ ] **Benchmark**: Isolate SIMD impact with micro-benchmarks
- [ ] **Verify**: Check CPU flags (`grep avx2 /proc/cpuinfo`)

### M4: Optimizer & Statistics (Week 7-8)
- [ ] Bytecode optimizer enhancements
- [ ] Index statistics collection
- [ ] Cost-based index selection
- [ ] **Benchmark**: Complex query workloads (multi-join, subqueries)

### M5: Validation & Release (Week 9)
- [ ] Full benchmark suite re-run (5 runs each)
- [ ] Regression tests (SQL:1999, 89+ suites)
- [ ] Documentation updates (README.md performance tables)
- [ ] **Compare**: Final vs baseline geometric mean speedup
- [ ] **Release**: v0.11.6 with performance changelog

---

## Benchmark-Driven Development Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. COLLECT BASELINE                                             │
│    ./scripts/benchmark_collect.sh                               │
│    → .build/benchmarks/baseline_v0.11.5.csv                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. ANALYZE                                                      │
│    python3 scripts/benchmark_analyze.py baseline_v0.11.5.csv    │
│    → Identifies P0-CRITICAL bottlenecks                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. IMPLEMENT OPTIMIZATION                                       │
│    (e.g., hash join fast path)                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. COLLECT POST-OPTIMIZATION DATA                               │
│    ./scripts/benchmark_collect.sh                               │
│    → .build/benchmarks/after_hash_join.csv                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. COMPARE                                                      │
│    python3 scripts/benchmark_compare.py                         │
│      baseline_v0.11.5.csv after_hash_join.csv                   │
│    → Shows speedup, geometric mean                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. PROFILE (if targets not met)                                 │
│    go test -bench=X -cpuprofile=cpu.prof                        │
│    go tool pprof cpu.prof                                       │
│    → Identifies remaining hot spots                             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. ITERATE or MERGE                                             │
│    If targets met: merge to dev, update README                  │
│    If targets missed: return to step 3                          │
└─────────────────────────────────────────────────────────────────┘
```

### Success Criteria per Optimization

| Optimization | Target | Measurement | Pass Criteria |
|--------------|--------|-------------|---------------|
| Hash join | 60× | `INNER_JOIN_1K` | < 100ms |
| COUNT(*) | 98× | `COUNT_STAR_10K` | < 50µs |
| VM dispatch | 5× | `SELECT_ALL_10K` | < 10ms |
| Arena memory | 40% GC | `mem.prof` allocs | -40% allocations |
| SIMD batch | 4× | `SUM_AGGREGATE_10K` | < 5ms |
| LTO | 10% | Geometric mean | 1.10× overall |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Hash join rewrite complexity | High | Incremental rollout, keep fallback path |
| Arena allocator memory leaks | Medium | Per-query lifecycle, audit all allocations |
| VM dispatch breaking changes | High | Maintain switch fallback, gradual migration |
| SIMD portability | Low | Runtime CPU feature detection, scalar fallback |
| PGO build complexity | Low | Optional build flag, documented process |

---

## Success Metrics

1. **Performance**: 
   - INNER JOIN: 60× speedup (6034ms → 100ms)
   - COUNT(*): 98× speedup (4.89ms → 50µs)
   - SELECT all: 5× speedup (11× → 2× slowdown)
2. **Memory**: 40% reduction in Go GC pressure
3. **Code Quality**: No regressions in SQL:1999 tests (89+ suites)
4. **Build Time**: <5% increase in C++ build time

---

## References

- Architecture: `docs/ARCHITECTURE.md`
- Performance Baseline: `README.md` (v0.11.5 benchmarks)
- C++ Module Structure: Commit `8ccf4e4`
- Hash Join Implementation: `src/core/VM/hash_join.cpp`
- Arena Allocator: `src/core/DS/arena_v2.h`
