# Plan CGO Switch - Phase 4: Module Integration

## Summary

Integrate existing CGO modules into cohesive subsystems. Phase 1-3 created individual CGO components; Phase 4 connects them to create larger, optimized modules with end-to-end CGO acceleration.

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

Integrated subsystems with minimal CGO boundary crossings:

```
┌─────────────────────────────────────────────────────────────┐
│              Integrated Query Execution Engine               │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐ │
│  │   CGO-CG    │→ │  CGO-VM      │→ │  CGO-DS Storage    │ │
│  │  Compiler   │  │  Executor    │  │  Engine            │ │
│  └─────────────┘  └──────────────┘  └────────────────────┘ │
│         ↓                ↓                    ↓              │
│  [Parser + Opt]   [Expr + Agg + Sort]  [B-Tree + Index]    │
└─────────────────────────────────────────────────────────────┘
```

**Goal:** Reduce CGO boundary crossings from ~100s per query to ~10s per query.

---

## Phase 4.1: Integrated Query Execution Engine

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

### Architecture

```cpp
// integrated_query_engine.hpp
#pragma once

#include "cg/compiler.hpp"
#include "vm/executor.hpp"
#include "ds/storage_engine.hpp"

namespace svdb::integrated {

// Single entry point for query execution
class QueryEngine {
public:
    QueryEngine();
    ~QueryEngine();
    
    // Execute complete query, return results
    // Minimal CGO crossings: 1 for compile, 1 for execute
    QueryResult execute(const std::string& sql);
    
    // Prepared statement workflow
    PreparedStatement* prepare(const std::string& sql);
    QueryResult executePrepared(PreparedStatement* stmt, 
                                const std::vector<Value>& params);
    
    // Batch execution (multiple queries in single CGO call)
    std::vector<QueryResult> executeBatch(const std::vector<std::string>& queries);

private:
    cg::Compiler compiler_;
    vm::Executor executor_;
    ds::StorageEngine storage_;
    
    // Integrated optimization pipeline
    optimizer::IntegratedOptimizer optimizer_;
};

// Query result (returned to Go in single CGO call)
struct QueryResult {
    std::vector<std::string> columnNames;
    std::vector<std::vector<Value>> rows;
    int64_t rowsAffected;
    std::string error;  // Empty on success
};

} // namespace svdb::integrated
```

### CGO Integration

```go
// internal/integrated/query_engine_cgo.go
package integrated

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../.build/cmake/lib -lsvdb_integrated
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "integrated_query_engine.h"
*/
import "C"

import (
    "unsafe"
)

// QueryEngine wraps C++ integrated query engine
type QueryEngine struct {
    handle *C.svdb_query_engine_t
}

// Execute runs complete query in single CGO call
func (qe *QueryEngine) Execute(sql string) (*QueryResult, error) {
    var cResult C.svdb_query_result_t
    var errorBuf [1024]C.char
    
    ret := C.svdb_query_engine_execute(
        qe.handle,
        C.CString(sql),
        &cResult,
        &errorBuf[0],
        C.size_t(len(errorBuf)),
    )
    
    if ret != 0 {
        return nil, fmt.Errorf("query error: %s", C.GoString(&errorBuf[0]))
    }
    
    // Convert result (single memory copy)
    return convertResult(&cResult), nil
}
```

### Expected Performance Gains

| Operation | Current (many crossings) | Integrated (few crossings) | Speedup |
|-----------|-------------------------|----------------------------|---------|
| Simple SELECT | 50 µs | 20 µs | 2.5× |
| SELECT + WHERE | 80 µs | 30 µs | 2.7× |
| SELECT + GROUP BY | 150 µs | 50 µs | 3.0× |
| SELECT + ORDER BY | 200 µs | 70 µs | 2.9× |
| Complex query | 500 µs | 150 µs | 3.3× |

---

## Phase 4.2: Integrated Expression Evaluation Pipeline

### Motivation

Current expression evaluation (per-row CGO calls):

```
For each row:
  Go: Load column → CGO: Compare → Go: Branch → CGO: Arithmetic → Go: Store
```

**Problem:** 4+ CGO calls per expression per row.

Target (batch evaluation):

```
CGO: [Load + Compare + Arithmetic + Store] for all rows at once
```

### Architecture

```cpp
// integrated_expr_pipeline.hpp
#pragma once

#include "vm/expr_eval.hpp"
#include "vm/simd_compare.hpp"
#include "vm/simd_arith.hpp"
#include "ds/column_vector.hpp"

namespace svdb::integrated {

// Batch expression evaluator
class ExprPipeline {
public:
    ExprPipeline(const vm::ExprBytecode* bytecode);
    
    // Evaluate expression for entire column batch
    // Single CGO call processes all rows
    void evaluateBatch(const ds::ColumnVector& input,
                       ds::ColumnVector& output,
                       size_t rowCount);
    
    // Evaluate with filter mask (skip non-matching rows)
    void evaluateFiltered(const ds::ColumnVector& input,
                          ds::ColumnVector& output,
                          const uint64_t* filterMask,
                          size_t rowCount);
    
    // Chained evaluation (multiple expressions in pipeline)
    void evaluateChain(const std::vector<vm::ExprBytecode>& exprs,
                       const std::vector<ds::ColumnVector>& inputs,
                       std::vector<ds::ColumnVector>& outputs,
                       size_t rowCount);

private:
    vm::ExprBytecode bytecode_;
    
    // SIMD-optimized evaluation kernels
    void evaluateSIMD(const ds::ColumnVector& input,
                      ds::ColumnVector& output,
                      size_t rowCount);
};

// Filter pipeline (WHERE clause evaluation)
class FilterPipeline {
public:
    // Evaluate WHERE clause, return bitmask of matching rows
    uint64_t* evaluateWhere(const ds::ColumnVector* columns,
                            const vm::ExprBytecode* whereExpr,
                            size_t rowCount,
                            size_t* outMaskSize);
    
    // Count matching rows from mask
    static size_t countMatches(const uint64_t* mask, size_t size);
    
    // Apply mask to extract matching rows
    static void applyMask(ds::ColumnVector& output,
                          const ds::ColumnVector& input,
                          const uint64_t* mask,
                          size_t maskSize);
};

} // namespace svdb::integrated
```

### SIMD Optimization

```cpp
// SIMD expression evaluation - processes 4 rows per cycle (AVX2)
void ExprPipeline::evaluateSIMD(const ds::ColumnVector& input,
                                 ds::ColumnVector& output,
                                 size_t rowCount) {
    const int64_t* inputData = input.data<int64_t>();
    int64_t* outputData = output.data<int64_t>();
    
    // Process 4 rows at a time with AVX2
    size_t i = 0;
    for (; i + 4 <= rowCount; i += 4) {
        __m256i vals = _mm256_loadu_si256((__m256i*)&inputData[i]);
        
        // Apply expression operations (example: multiply by 2)
        __m256i result = _mm256_mullo_epi32(vals, _mm256_set1_epi32(2));
        
        _mm256_storeu_si256((__m256i*)&outputData[i], result);
    }
    
    // Handle remainder
    for (; i < rowCount; i++) {
        outputData[i] = inputData[i] * 2;
    }
}
```

### Expected Performance Gains

| Operation | Current (per-row) | Integrated (batch) | Speedup |
|-----------|-------------------|--------------------|---------|
| Simple expression | 100 ns/row | 25 ns/row | 4.0× |
| Comparison | 80 ns/row | 20 ns/row | 4.0× |
| Arithmetic | 60 ns/row | 15 ns/row | 4.0× |
| Complex expression | 200 ns/row | 40 ns/row | 5.0× |
| Filter (WHERE) | 150 ns/row | 30 ns/row | 5.0× |

---

## Phase 4.3: Integrated Storage Engine

### Motivation

Current storage access (multiple CGO calls):

```
Go: Open table → CGO: B-Tree search → Go: Decode row → 
CGO: Decompress → Go: Parse → CGO: Index lookup
```

**Problem:** 5+ CGO calls per row access.

Target (integrated storage):

```
CGO: [Open + Search + Decode + Decompress + Index] in single call
```

### Architecture

```cpp
// integrated_storage_engine.hpp
#pragma once

#include "ds/btree.hpp"
#include "ds/compression.hpp"
#include "ds/roaring_bitmap.hpp"
#include "ds/column_store.hpp"

namespace svdb::integrated {

// Unified storage engine
class StorageEngine {
public:
    StorageEngine();
    ~StorageEngine();
    
    // Table operations
    Table* openTable(const std::string& tableName);
    void createTable(const std::string& tableName, const Schema& schema);
    void dropTable(const std::string& tableName);
    
    // Row operations (batch)
    size_t insertBatch(Table* table, const std::vector<Row>& rows);
    size_t updateBatch(Table* table, const Row& newRow, const vm::ExprBytecode* where);
    size_t deleteBatch(Table* table, const vm::ExprBytecode* where);
    
    // Query operations (integrated scan + filter + index)
    QueryResult scan(Table* table,
                     const std::vector<std::string>& columns,
                     const vm::ExprBytecode* where,
                     const std::vector<OrderBy>& orderBy,
                     int64_t limit,
                     int64_t offset);
    
    // Index operations (integrated with B-Tree)
    Index* createIndex(Table* table, 
                       const std::string& indexName,
                       const std::vector<std::string>& columns);
    RowIDVector* indexLookup(Index* index, const Value& key);
    RowIDVector* indexRange(Index* index, 
                            const Value& startKey,
                            const Value& endKey);

private:
    ds::BTree btree_;
    ds::Compression compression_;
    ds::RoaringBitmap index_;
    ds::ColumnStore columnStore_;
    
    // Integrated row decoder (decompress + decode in single pass)
    Row decodeRow(const uint8_t* compressedData, size_t compressedSize);
};

// Table scan with integrated filtering
class TableScan {
public:
    TableScan(Table* table, const vm::ExprBytecode* filter);
    
    // Scan with filter, return matching rows
    // Single CGO call does: scan + filter + decode
    std::vector<Row> scanFiltered(size_t limit);
    
    // Scan with filter and projection (select specific columns)
    std::vector<Row> scanProjected(const std::vector<int>& columnIndices,
                                   size_t limit);

private:
    Table* table_;
    vm::ExprBytecode filter_;
    ds::BTreeCursor cursor_;
};

} // namespace svdb::integrated
```

### Expected Performance Gains

| Operation | Current (multiple calls) | Integrated (single call) | Speedup |
|-----------|-------------------------|--------------------------|---------|
| Row lookup | 5 µs | 1.5 µs | 3.3× |
| Batch insert (100 rows) | 200 µs | 50 µs | 4.0× |
| Scan + filter | 10 µs/row | 2 µs/row | 5.0× |
| Index lookup | 3 µs | 1 µs | 3.0× |
| Scan + index + filter | 15 µs/row | 3 µs/row | 5.0× |

---

## Phase 4.4: Integrated Optimizer

### Motivation

Current optimization (Go-side, multiple passes):

```
Go: Parse → Go: Optimize (pass 1) → Go: Optimize (pass 2) → 
Go: Compile → CGO: Execute
```

**Problem:** Optimization happens in Go, missing CGO-specific optimizations.

Target (integrated optimization):

```
Go: Parse → CGO: [Optimize + Compile + Execute]
```

### Architecture

```cpp
// integrated_optimizer.hpp
#pragma once

#include "cg/optimizer.hpp"
#include "vm/optimizer.hpp"
#include "ds/statistics.hpp"

namespace svdb::integrated {

// Cross-layer optimizer
class IntegratedOptimizer {
public:
    IntegratedOptimizer(ds::Statistics* stats);
    
    // Optimize complete query plan
    OptimizedPlan optimize(const qp::SelectStmt* stmt);
    
    // Optimization passes (run in order)
    void predicatePushdown(OptimizedPlan& plan);
    void columnPruning(OptimizedPlan& plan);
    void indexSelection(OptimizedPlan& plan);
    void joinReordering(OptimizedPlan& plan);
    void projectionPushdown(OptimizedPlan& plan);
    
    // Cost estimation (uses DS statistics)
    double estimateCost(const OptimizedPlan& plan);

private:
    ds::Statistics* stats_;
    cg::Optimizer cgOptimizer_;
    vm::Optimizer vmOptimizer_;
};

// Optimized query plan
struct OptimizedPlan {
    // Chosen access method (scan, index scan, etc.)
    enum AccessMethod { TABLE_SCAN, INDEX_SCAN, INDEX_SEEK };
    AccessMethod accessMethod;
    
    // Pushed-down predicates (evaluated at storage layer)
    std::vector<vm::ExprBytecode> pushedPredicates;
    
    // Remaining predicates (evaluated at VM layer)
    std::vector<vm::ExprBytecode> remainingPredicates;
    
    // Projected columns (after pruning)
    std::vector<int> projectedColumns;
    
    // Chosen index (if any)
    std::string chosenIndex;
    
    // Estimated cost
    double estimatedCost;
};

} // namespace svdb::integrated
```

### Expected Performance Gains

| Optimization | Current (Go) | Integrated (CGO) | Speedup |
|--------------|--------------|------------------|---------|
| Optimization pass | 30 µs | 10 µs | 3.0× |
| Predicate pushdown | 20 µs | 5 µs | 4.0× |
| Index selection | 15 µs | 5 µs | 3.0× |
| Total optimization | 100 µs | 30 µs | 3.3× |

---

## Phase 4.5: Implementation Plan

### Week 1-2: Integrated Query Engine
- [ ] Create `internal/integrated/` directory
- [ ] Implement `QueryEngine` C++ class
- [ ] Create CGO bindings
- [ ] Integration tests

### Week 3-4: Expression Pipeline
- [ ] Implement `ExprPipeline` C++ class
- [ ] Implement SIMD kernels
- [ ] Integrate with VM expression eval
- [ ] Benchmark vs current implementation

### Week 5-6: Storage Engine
- [ ] Implement `StorageEngine` C++ class
- [ ] Integrate B-Tree + compression + index
- [ ] Implement `TableScan` with filtering
- [ ] Integration tests

### Week 7-8: Optimizer
- [ ] Implement `IntegratedOptimizer` C++ class
- [ ] Cross-layer optimization passes
- [ ] Cost estimation with statistics
- [ ] Benchmark optimization speed

### Week 9-10: Integration Testing
- [ ] End-to-end tests
- [ ] Performance benchmarks
- [ ] Memory leak detection (AddressSanitizer)
- [ ] Stability testing

### Week 11-12: Cleanup
- [ ] Remove old modular CGO code
- [ ] Update documentation
- [ ] Final performance validation
- [ ] Release preparation

---

## Phase 4.6: Expected Overall Performance Gains

| Query Type | Current | Phase 4 Integrated | Speedup |
|------------|---------|--------------------|---------|
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
| Integration complexity | High | Incremental integration, extensive testing |
| Performance regression | Medium | Benchmark at each step, rollback plan |
| Memory management | Medium | RAII, smart pointers, sanitizers |
| API compatibility | Low | Wrapper layer for backward compatibility |

---

## Phase 4.8: Success Criteria

- [ ] All existing tests pass with integrated modules
- [ ] 3-5× speedup for typical queries
- [ ] Reduced CGO boundary crossings (100s → 10s per query)
- [ ] No memory leaks (AddressSanitizer clean)
- [ ] Documentation updated
- [ ] Old modular code removed

---

## Timeline

| Phase | Duration | Milestone |
|-------|----------|-----------|
| 4.1 | Week 1-2 | Integrated Query Engine |
| 4.2 | Week 3-4 | Expression Pipeline |
| 4.3 | Week 5-6 | Storage Engine |
| 4.4 | Week 7-8 | Optimizer |
| 4.5 | Week 9-10 | Integration Testing |
| 4.6 | Week 11-12 | Cleanup & Release |

**Total Estimated Effort:** 12 weeks (3 months)

---

## Summary

Phase 4 transforms individual CGO modules into integrated subsystems:

1. **Query Execution Engine**: Single CGO call for complete query
2. **Expression Pipeline**: Batch SIMD evaluation for expressions
3. **Storage Engine**: Integrated B-Tree + compression + index
4. **Optimizer**: Cross-layer optimization with statistics

**Expected Outcome:** 3-5× end-to-end query performance improvement.
