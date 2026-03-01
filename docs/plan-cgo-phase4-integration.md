# Plan CGO Switch - Phase 4: Module Integration

## Summary

Integrate existing CGO modules by optimizing the **invoke chain** (call sequence), NOT by merging code. Phase 1-3 created individual CGO components; Phase 4 connects them efficiently while keeping original subsystems intact.

## Key Principles

1. **Keep Original Subsystems Intact**
   - DO NOT merge code from different modules
   - DO NOT refactor existing CGO libraries
   - ONLY optimize how modules are called together
   - Original APIs remain unchanged

2. **Consolidate VM Libraries**
   - Merge `libsvdb_vm_phase2.so` into `libsvdb_vm.so`
   - Single VM library for all VM-related CGO
   - Simplifies build and deployment

3. **Minimize CGO Boundary Crossings**
   - Batch multiple operations into single CGO call
   - Create wrapper functions that call multiple subsystems
   - Reduce Go ↔ CGO context switching overhead

## Overview

### Current State (Post Phase 1-3)

We have multiple independent CGO modules:

```
Extensions:     [math] [json] [fts5]
                ↓      ↓      ↓
Data Storage:   [B-Tree] [SIMD] [Compression] [RoaringBitmap]
                ↓        ↓       ↓            ↓
VM Core:        [Hash] [Compare] [Batch] [Sort] [ExprEval] [Dispatch] [TypeConv] [StringFuncs] [DateTime] [Aggregates]
                ↓        ↓         ↓        ↓       ↓          ↓          ↓           ↓            ↓           ↓
Query Parser:   [Tokenizer]
```

**Problem:** These modules operate independently. Data must cross CGO boundaries multiple times during query execution, adding overhead.

### Target State (Phase 4)

**Original subsystems kept intact**, with optimized invoke chains:

```
┌─────────────────────────────────────────────────────────────┐
│              Query Execution (Go Layer)                      │
│                        ↓                                      │
│         ┌──────────────┴──────────────┐                       │
│         │  Invoke Chain Wrapper (CGO) │                       │
│         │  - Single CGO call          │                       │
│         │  - Calls multiple subsystems│                       │
│         └──────────────┬──────────────┘                       │
│                        ↓                                      │
│    ┌─────────────┬─────────────┬────────────────────┐        │
│    │  libsvdb_cg │  libsvdb_vm │  libsvdb_ds        │        │
│    │  (Compiler) │  (Executor) │  (Storage)         │        │
│    │             │             │                    │        │
│    │  [Original  │  [Original  │  [Original         │        │
│    │   code      │   code      │   code             │        │
│    │   intact]   │   intact]   │   intact]          │        │
│    └─────────────┴─────────────┴────────────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

**Key Changes:**
1. `libsvdb_vm_phase2.so` → merged into `libsvdb_vm.so`
2. New "invoke chain wrapper" functions (thin layer)
3. Original subsystem code unchanged

**Goal:** Reduce CGO boundary crossings from ~100s per query to ~10s per query.

---

## Phase 4.1: Library Consolidation

### libsvdb_vm_phase2 → libsvdb_vm

**Current State:**
```
.build/cmake/lib/
├── libsvdb_vm.so           (VM core functions)
└── libsvdb_vm_phase2.so    (VM phase 2 functions)
```

**Problem:** Two separate VM libraries, requires multiple `-l` flags.

**Target State:**
```
.build/cmake/lib/
└── libsvdb_vm.so           (All VM functions combined)
```

### Implementation

**Step 1: Merge CMakeLists.txt**

```cmake
# internal/VM/cgo/CMakeLists.txt

# VM library sources (Phase 1 + Phase 2)
add_library(svdb_vm SHARED
    # Phase 1: VM core
    hash.cpp
    compare.cpp
    sort.cpp
    aggregate_funcs.cpp
    datetime.cpp
    string_funcs.cpp
    type_conv.cpp
    dispatch.cpp
    expr_eval.cpp
    
    # Phase 2: Additional VM functions
    hash_join.cpp
)

target_include_directories(svdb_vm PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(svdb_vm PROPERTIES
    OUTPUT_NAME "svdb_vm"
    VERSION 1.0.0
    SOVERSION 1
)
```

**Step 2: Update Go CGO bindings**

```go
// Before (two libraries)
/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../.build/cmake/lib -lsvdb_vm -lsvdb_vm_phase2
*/

// After (single library)
/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../.build/cmake/lib -lsvdb_vm
*/
```

**Step 3: Remove old library reference**

```bash
# Delete old Phase 2 CMakeLists.txt
rm pkg/sqlvibe/cgo/CMakeLists.txt

# Move hash_join.cpp to internal/VM/cgo/
mv pkg/sqlvibe/cgo/hash_join.cpp internal/VM/cgo/
mv pkg/sqlvibe/cgo/hash_join.h internal/VM/cgo/
```

---

## Phase 4.2: Invoke Chain Optimization

### Motivation

Current query execution flow (many CGO crossings):

```
Go: Parse SQL → CGO: Tokenize → Go: Build AST → CGO: Compile → 
Go: Create VM → CGO: Execute Expr → Go: Fetch Row → CGO: Compare → 
Go: Filter → CGO: Aggregate → Go: Sort → CGO: Return
```

**Problem:** 10+ CGO boundary crossings per query.

Target flow (minimal CGO crossings):

```
Go: Parse SQL → CGO: [Tokenize + Compile + Execute + Return]
```

**Goal:** 2-3 CGO boundary crossings per query.

### Architecture: Invoke Chain Wrapper

**Key Principle:** Create thin wrapper functions that call multiple existing subsystems in sequence.

```cpp
// invoke_chain_wrapper.hpp
// Thin wrapper - calls existing subsystems, no code merging

#pragma once

#include "qp/tokenizer.h"
#include "cg/compiler.h"
#include "vm/executor.h"
#include "ds/storage.h"

namespace svdb::wrapper {

// Single function that chains multiple subsystem calls
// All work happens inside single CGO call
QueryResult execute_query(
    const std::string& sql,
    qp::Tokenizer* tokenizer,
    cg::Compiler* compiler,
    vm::Executor* executor,
    ds::Storage* storage
) {
    // Step 1: Tokenize (existing subsystem)
    auto tokens = tokenizer->tokenize(sql);
    
    // Step 2: Compile (existing subsystem)
    auto program = compiler->compile(tokens);
    
    // Step 3: Execute (existing subsystem)
    auto result = executor->execute(program, storage);
    
    // Step 4: Return (single result)
    return result;
}

} // namespace svdb::wrapper
```

### CGO Integration

```go
// internal/wrapper/invoke_chain_cgo.go
package wrapper

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../.build/cmake/lib -lsvdb_vm -lsvdb_cg -lsvdb_ds
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "invoke_chain_wrapper.h"
*/
import "C"

// ExecuteQuery runs complete query in single CGO call
// Internally calls: Tokenize → Compile → Execute → Return
func ExecuteQuery(sql string) (*QueryResult, error) {
    var cResult C.QueryResult
    var errorBuf [1024]C.char
    
    ret := C.execute_query(
        C.CString(sql),
        &cResult,
        &errorBuf[0],
        C.size_t(len(errorBuf)),
    )
    
    if ret != 0 {
        return nil, fmt.Errorf("query error: %s", C.GoString(&errorBuf[0]))
    }
    
    return convertResult(&cResult), nil
}
```

### Expected Performance Gains

| Operation | Current (many crossings) | Optimized (few crossings) | Speedup |
|-----------|-------------------------|--------------------------|---------|
| Simple SELECT | 50 µs | 20 µs | 2.5× |
| SELECT + WHERE | 80 µs | 30 µs | 2.7× |
| SELECT + GROUP BY | 150 µs | 50 µs | 3.0× |
| SELECT + ORDER BY | 200 µs | 70 µs | 2.9× |
| Complex query | 500 µs | 150 µs | 3.3× |
```

### Expected Performance Gains

| Operation | Current (many crossings) | Optimized (few crossings) | Speedup |
|-----------|-------------------------|--------------------------|---------|
| Simple SELECT | 50 µs | 20 µs | 2.5× |
| SELECT + WHERE | 80 µs | 30 µs | 2.7× |
| SELECT + GROUP BY | 150 µs | 50 µs | 3.0× |
| SELECT + ORDER BY | 200 µs | 70 µs | 2.9× |
| Complex query | 500 µs | 150 µs | 3.3× |

---

## Phase 4.3: Expression Batch Wrapper

### Motivation

Current expression evaluation (per-row CGO calls):

```
For each row:
  Go: Load column → CGO: Compare → Go: Branch → CGO: Arithmetic → Go: Store
```

**Problem:** 4+ CGO calls per expression per row.

Target (batch evaluation with existing subsystems):

```
CGO Wrapper: [Load + Compare + Arithmetic + Store] for all rows at once
              ↓        ↓          ↓            ↓
           (calls existing VM SIMD functions internally)
```

**Key Principle:** Wrapper calls existing `vm/simd_compare.hpp` and `vm/simd_arith.hpp` functions in batch.

### Architecture: Batch Wrapper

```cpp
// expr_batch_wrapper.hpp
// Thin wrapper - calls existing VM SIMD functions

#pragma once

#include "vm/expr_eval.hpp"
#include "vm/simd_compare.hpp"
#include "vm/simd_arith.hpp"
#include "ds/column_vector.hpp"

namespace svdb::wrapper {

// Batch expression evaluation wrapper
// Calls existing VM SIMD functions internally
class ExprBatchWrapper {
public:
    // Evaluate expression for entire column batch
    // Internally calls existing vm::ExprEval + vm::SIMD functions
    static void evaluateBatch(
        const vm::ExprBytecode* bytecode,
        const ds::ColumnVector& input,
        ds::ColumnVector& output,
        size_t rowCount
    ) {
        // Call existing VM expression evaluator
        // No code merging, just efficient sequencing
        vm::ExprEval eval(bytecode);
        
        // Process in batches of 4 (AVX2)
        for (size_t i = 0; i < rowCount; i += 4) {
            size_t batchSize = std::min(size_t(4), rowCount - i);
            eval.evaluateSIMD(input, output, i, batchSize);
        }
    }
    
    // Filter with WHERE clause
    // Calls existing vm::SIMDCompare internally
    static uint64_t* evaluateWhere(
        const ds::ColumnVector* columns,
        const vm::ExprBytecode* whereExpr,
        size_t rowCount,
        size_t* outMaskSize
    ) {
        // Call existing VM SIMD compare functions
        return vm::SIMDCompare::evaluateWhere(columns, whereExpr, rowCount, outMaskSize);
    }
};

} // namespace svdb::wrapper
```

### Expected Performance Gains

| Operation | Current (per-row) | Batch Wrapper | Speedup |
|-----------|-------------------|---------------|---------|
| Simple expression | 100 ns/row | 25 ns/row | 4.0× |
| Comparison | 80 ns/row | 20 ns/row | 4.0× |
| Arithmetic | 60 ns/row | 15 ns/row | 4.0× |
| Complex expression | 200 ns/row | 40 ns/row | 5.0× |
| Filter (WHERE) | 150 ns/row | 30 ns/row | 5.0× |

---

## Phase 4.4: Storage Access Wrapper

### Motivation

Current storage access (multiple CGO calls):

```
Go: Open table → CGO: B-Tree search → Go: Decode row → 
CGO: Decompress → Go: Parse → CGO: Index lookup
```

**Problem:** 5+ CGO calls per row access.

Target (wrapper with existing subsystems):

```
CGO Wrapper: [Open + Search + Decode + Decompress + Index] in single call
              ↓       ↓        ↓         ↓           ↓
         (calls existing DS functions internally)
```

### Architecture: Storage Wrapper

```cpp
// storage_wrapper.hpp
// Thin wrapper - calls existing DS functions

#pragma once

#include "ds/btree.hpp"
#include "ds/compression.hpp"
#include "ds/roaring_bitmap.hpp"
#include "ds/column_store.hpp"

namespace svdb::wrapper {

// Storage access wrapper
// Calls existing DS functions in efficient sequence
class StorageWrapper {
public:
    // Scan table with filter
    // Internally calls: B-Tree → Decompress → Filter
    static std::vector<Row> scanWithFilter(
        ds::BTree* btree,
        ds::Compression* compression,
        const vm::ExprBytecode* filter,
        size_t limit
    ) {
        std::vector<Row> results;
        
        // Call existing B-Tree scan
        auto cursor = btree->scan();
        
        while (cursor.hasMore() && results.size() < limit) {
            // Call existing decompression
            auto compressedData = cursor.next();
            auto row = compression->decompress(compressedData);
            
            // Call existing filter evaluation
            if (filter->evaluate(row)) {
                results.push_back(row);
            }
        }
        
        return results;
    }
    
    // Index lookup with row fetch
    // Internally calls: Index → B-Tree → Decompress
    static Row indexLookup(
        ds::RoaringBitmap* index,
        ds::BTree* btree,
        ds::Compression* compression,
        const Value& key
    ) {
        // Call existing index lookup
        auto rowIds = index->lookup(key);
        
        // Call existing B-Tree fetch
        auto compressedData = btree->fetch(rowIds[0]);
        
        // Call existing decompression
        return compression->decompress(compressedData);
    }
};

} // namespace svdb::wrapper
```

### Expected Performance Gains

| Operation | Current (multiple calls) | Wrapper (single call) | Speedup |
|-----------|-------------------------|----------------------|---------|
| Row lookup | 5 µs | 1.5 µs | 3.3× |
| Batch insert (100 rows) | 200 µs | 50 µs | 4.0× |
| Scan + filter | 10 µs/row | 2 µs/row | 5.0× |
| Index lookup | 3 µs | 1 µs | 3.0× |
| Scan + index + filter | 15 µs/row | 3 µs/row | 5.0× |

---

## Phase 4.5: Implementation Plan

### Week 1-2: Library Consolidation
- [ ] Merge `libsvdb_vm_phase2.so` into `libsvdb_vm.so`
- [ ] Update `internal/VM/cgo/CMakeLists.txt`
- [ ] Update Go CGO bindings
- [ ] Remove old `pkg/sqlvibe/cgo/` directory
- [ ] Test all VM functions still work

### Week 3-4: Invoke Chain Wrapper
- [ ] Create `internal/wrapper/` directory
- [ ] Implement `invoke_chain_wrapper.hpp`
- [ ] Create CGO bindings
- [ ] Integration tests
- [ ] Benchmark vs current implementation

### Week 5-6: Expression Batch Wrapper
- [ ] Implement `expr_batch_wrapper.hpp`
- [ ] Call existing VM SIMD functions
- [ ] Create CGO bindings
- [ ] Benchmark batch vs per-row

### Week 7-8: Storage Access Wrapper
- [ ] Implement `storage_wrapper.hpp`
- [ ] Call existing DS functions
- [ ] Create CGO bindings
- [ ] Integration tests

### Week 9-10: Integration Testing
- [ ] End-to-end tests
- [ ] Performance benchmarks
- [ ] Memory leak detection (AddressSanitizer)
- [ ] Stability testing

### Week 11-12: Cleanup
- [ ] Remove old wrapper code
- [ ] Update documentation
- [ ] Final performance validation
- [ ] Release preparation

---

## Phase 4.6: Expected Overall Performance Gains

| Query Type | Current | After Phase 4 | Speedup |
|------------|---------|---------------|---------|
| Simple SELECT | 100 µs | 30 µs | 3.3× |
| SELECT + WHERE | 150 µs | 40 µs | 3.8× |
| SELECT + JOIN | 500 µs | 120 µs | 4.2× |
| SELECT + GROUP BY | 300 µs | 80 µs | 3.8× |
| SELECT + ORDER BY | 400 µs | 100 µs | 4.0× |
| Complex query | 1000 µs | 200 µs | 5.0× |

**Overall Expected Speedup:** 3-5× for typical queries

---

## Phase 4.7: Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Wrapper complexity | Low | Thin wrappers only, call existing functions |
| Performance regression | Medium | Benchmark at each step, rollback plan |
| Memory management | Medium | RAII, smart pointers, sanitizers |
| API compatibility | Low | Wrapper layer maintains backward compatibility |

---

## Phase 4.8: Success Criteria

- [ ] All existing tests pass with wrappers
- [ ] 3-5× speedup for typical queries
- [ ] Reduced CGO boundary crossings (100s → 10s per query)
- [ ] Original subsystem code unchanged
- [ ] `libsvdb_vm_phase2.so` merged into `libsvdb_vm.so`
- [ ] No memory leaks (AddressSanitizer clean)
- [ ] Documentation updated

---

## Timeline

| Phase | Duration | Milestone |
|-------|----------|-----------|
| 4.1 | Week 1-2 | Library Consolidation |
| 4.2 | Week 3-4 | Invoke Chain Wrapper |
| 4.3 | Week 5-6 | Expression Batch Wrapper |
| 4.4 | Week 7-8 | Storage Access Wrapper |
| 4.5 | Week 9-10 | Integration Testing |
| 4.6 | Week 11-12 | Cleanup & Release |

**Total Estimated Effort:** 12 weeks (3 months)

---

## Summary

Phase 4 optimizes CGO module integration by:

1. **Library Consolidation**: Merge `libsvdb_vm_phase2.so` into `libsvdb_vm.so`
2. **Invoke Chain Wrapper**: Single CGO call for complete query
3. **Expression Batch Wrapper**: Batch SIMD evaluation (calls existing VM functions)
4. **Storage Access Wrapper**: Integrated storage access (calls existing DS functions)

**Key Principle:** Original subsystems kept intact. Only invoke chain optimized.

**Expected Outcome:** 3-5× end-to-end query performance improvement.
    RowIDVector* indexRange(Index* index, 
                            const Value& startKey,
                            const Value& endKey);

private:

---

## Summary

Phase 4 optimizes CGO module integration by:

1. **Library Consolidation**: Merge `libsvdb_vm_phase2.so` into `libsvdb_vm.so`
2. **Invoke Chain Wrapper**: Single CGO call for complete query
3. **Expression Batch Wrapper**: Batch SIMD evaluation (calls existing VM functions)
4. **Storage Access Wrapper**: Integrated storage access (calls existing DS functions)

**Key Principle:** Original subsystems kept intact. Only invoke chain optimized.

**Expected Outcome:** 3-5× end-to-end query performance improvement.
