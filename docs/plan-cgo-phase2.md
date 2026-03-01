# Plan CGO Switch - Phase 2: Query Execution Layer

## Summary

Implement Phase 2 of CGO optimization by converting query execution layer packages to use C++ for performance-critical operations, building on the completed Phase 1 (Extensions + DS + VM Core).

## Overview

Phase 1 (Phases 1-18) is complete. All extension and VM core optimizations are implemented and tested.

Phase 2 focuses on the query execution layer in `pkg/sqlvibe/` to address remaining performance bottlenecks.

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 19 | pkg/sqlvibe/vm: Query Optimization | libsvdb_vm | Pending |
| Phase 20 | pkg/sqlvibe: Hash JOIN | libsvdb_vm | Pending |
| Phase 21 | pkg/sqlvibe: Batch DML / CTE | libsvdb_vm | Pending |
| Phase 22 | pkg/sqlvibe: VM Context | libsvdb_vm | Pending |
| Phase 23 | pkg/sqlvibe: Window Functions | libsvdb_vm | Pending |
| Phase 24 | pkg/sqlvibe: Set Operations | libsvdb_vm | Pending |

---

## 1. Build System

### 1.1 Build Tags

| Tag | Description | Default |
|-----|-------------|---------|
| `SVDB_ENABLE_CGO` | Enable **all** C++ implementations | **Always enabled** |
| `SVDB_EXT_MATH` | Enable math extension | No |
| `SVDB_EXT_JSON` | Enable JSON extension | No |
| `SVDB_EXT_FTS5` | Enable FTS5 extension | No |
| `SVDB_ENABLE_CGO_DS` | Enable CGO data storage | Always enabled |
| `SVDB_ENABLE_CGO_VM` | Enable CGO VM execution | Always enabled |

> **Note:** CGO is always enabled. C++ libraries are built automatically by `./build.sh`.

### 1.2 Build Commands

```bash
# Default: CGO always enabled
./build.sh

# Run tests
./build.sh -t

# Run benchmarks
./build.sh -b

# Run tests with coverage
./build.sh -t -c

# With specific CGO components (fine-grained control)
go build -tags "SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5,SVDB_ENABLE_CGO_DS" ./...
```

---

## 2. Architecture Pattern

Each Phase 2 component follows the same pattern as Phase 1:

```go
// pkg/sqlvibe/xxx_cgo.go - CGO implementation
// +build SVDB_ENABLE_CGO_VM

package sqlvibe

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_vm
#include "xxx.h"
*/
import "C"

// CGO-accelerated implementation
func someFunc(...) ... {
    C.svdb_some_func(...)
}
```

```go
// pkg/sqlvibe/xxx_pure.go - Pure Go fallback
// +build !SVDB_ENABLE_CGO_VM

package sqlvibe

// Pure Go implementation
func someFunc(...) ... {
    // Go implementation
}
```

---

## 3. Phase 19 - Query Optimization (pkg/sqlvibe/vm/)

### Motivation
- Column pruning logic runs for **every SELECT query**
- `CollectColumnRefs()` traverses AST recursively
- Currently pure Go with slice allocations
- Affects query compilation time

### Target Components

#### pkg/sqlvibe/vm/optimize.go
**Functions to CGO-ize:**
- `PruneColumns(stmt *SelectStmt, available []string) []string` - Column pruning
- `CollectColumnRefs(expr Expr) []string` - AST traversal
- `CanPushdownWhere(where Expr) bool` - Predicate analysis

### File Structure
```
pkg/sqlvibe/vm/cgo/
├── optimize.h          # Query optimization header
├── optimize.cpp        # AST traversal in C++
└── CMakeLists.txt     # Build configuration
```

### Expected Performance Gains
- 1.5-2× faster query compilation
- 2-3× faster column pruning for wide tables
- Reduced allocation from AST traversal

### Implementation Notes
- Batch AST traversal for multiple columns
- Use C++ string views to avoid allocations
- Keep Go fallback for simple queries

---

## 4. Phase 20 - Hash JOIN (pkg/sqlvibe/hash_join.go)

### Motivation
- Hash JOIN is still **1.0-1.2× slower than SQLite** at 100K rows
- Build phase iterates all rows from build table
- Probe phase checks hash for every probe row
- Currently uses Go `map[string]interface{}` boxing

### Target Components

#### pkg/sqlvibe/hash_join.go
**Functions to CGO-ize:**
- `execHashJoin(stmt *SelectStmt) ([][]interface{}, []string, bool)` - Main JOIN logic
- Hash table build phase
- Probe phase with hash lookup

### File Structure
```
pkg/sqlvibe/cgo/
├── hash_join.h         # Hash JOIN header
├── hash_join.cpp       # Batch hash join implementation
└── CMakeLists.txt     # Build configuration
```

### CGO Integration Pattern
```cpp
// Batch hash join - build and probe in C++
extern "C" void svdb_hash_join_batch(
    const svdb_row_t* build_rows,
    size_t build_count,
    const svdb_row_t* probe_rows,
    size_t probe_count,
    int join_key_cols,
    svdb_row_t* results,
    size_t* result_count
);
```

### Expected Performance Gains
- 1.5-2× faster JOIN execution
- Match or exceed SQLite performance
- Reduced GC pressure from pooled memory

### Implementation Notes
- **Highest priority** - JOIN is fundamental SQL operation
- Use C++ unordered_map for hash table
- Batch probe operations for cache efficiency
- Clear performance target: match SQLite at all scales

---

## 5. Phase 21 - Batch DML / CTE (pkg/sqlvibe/database.go)

### Motivation
- INSERT/UPDATE/DELETE are write-heavy workloads
- Recursive CTE iteration happens in Go
- Currently per-row interface{} boxing
- Fixed-point detection checks all rows

### Target Components

#### pkg/sqlvibe/database.go (4,952 lines)
**Functions to CGO-ize:**

**Batch INSERT:**
- `execInsertBatch(stmt *InsertStmt) (Result, error, bool)` - Batch INSERT
- `execInsertOnConflict(stmt *InsertStmt) (Result, error)` - INSERT ... ON CONFLICT
- `execInsertOrReplace(stmt *InsertStmt) (Result, error)` - INSERT OR REPLACE

**Batch UPDATE/DELETE:**
- `execUpdateFrom(stmt *UpdateStmt) (Result, error)` - UPDATE ... FROM
- `execDeleteUsing(stmt *DeleteStmt) (Result, error)` - DELETE ... USING

**Recursive CTE:**
- `execRecursiveCTE(cte *CTEClause) (*Rows, error)` - Recursive CTE execution

### File Structure
```
pkg/sqlvibe/cgo/
├── dml_batch.h         # Batch DML header
├── dml_batch.cpp       # Batch INSERT/UPDATE/DELETE
├── recursive_cte.h     # Recursive CTE header
├── recursive_cte.cpp   # CTE iteration in C++
└── CMakeLists.txt     # Build configuration
```

### Expected Performance Gains
- 2-3× faster batch INSERT (1000+ rows)
- 2-3× faster recursive CTE queries
- Reduced allocation from row pooling

### Implementation Notes
- High impact - DML is common operation
- Batch processing amortizes CGO overhead
- Recursive CTE benefits from C++ memory management

---

## 6. Phase 22 - VM Context (pkg/sqlvibe/vm_context.go)

### Motivation
- `GetTableData()`, `InsertRow()`, `UpdateRow()` called per-row
- Currently copies `map[string]interface{}` for every row
- Interface conversions on hot path
- 800+ lines of context management

### Target Components

#### pkg/sqlvibe/vm_context.go
**Functions to CGO-ize:**
- `GetTableData(tableName string) ([]map[string]interface{}, error)` - Table access
- `InsertRow(tableName string, row map[string]interface{}) error` - Row insert
- `UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error` - Row update
- `DeleteRow(tableName string, rowIndex int) error` - Row delete

### File Structure
```
pkg/sqlvibe/cgo/
├── vm_context.h        # VM context header
├── vm_context.cpp      # Context operations
└── CMakeLists.txt     # Build configuration
```

### Expected Performance Gains
- 1.5-2× faster row operations
- Reduced memory copying
- Better cache locality

### Implementation Notes
- Use C++ struct-of-arrays for row storage
- Batch row access where possible
- Maintain Go map interface for compatibility

---

## 7. Phase 23 - Window Functions (pkg/sqlvibe/window.go)

### Motivation
- Window functions (ROW_NUMBER, RANK, LAG, LEAD) process all rows
- Frame calculations iterate partitions
- Currently pure Go with slice allocations
- Complex queries benefit most

### Target Components

#### pkg/sqlvibe/window.go
**Functions to CGO-ize:**
- Window frame evaluation
- Partition processing
- Ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
- Value functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)

### File Structure
```
pkg/sqlvibe/cgo/
├── window.h            # Window function header
├── window.cpp          # Window evaluation in C++
└── CMakeLists.txt     # Build configuration
```

### Expected Performance Gains
- 2-3× faster window function queries
- Reduced allocation from frame buffers
- Better partition handling

### Implementation Notes
- 4-5 days effort - complex logic
- Batch partition processing
- Reuse sorted order from ORDER BY

---

## 8. Phase 24 - Set Operations (pkg/sqlvibe/setops.go)

### Motivation
- UNION/INTERSECT/EXCEPT process all rows from both sides
- Deduplication uses Go maps
- Called for set operation queries
- Lower frequency but still useful

### Target Components

#### pkg/sqlvibe/setops.go
**Functions to CGO-ize:**
- `execSetOp(stmt *SelectStmt, originalSQL string) (*Rows, error)` - Set operation execution
- `applySetOp(left, right [][]interface{}, op string, all bool) [][]interface{}` - Set logic

### File Structure
```
pkg/sqlvibe/cgo/
├── setops.h            # Set operations header
├── setops.cpp          # UNION/INTERSECT/EXCEPT
└── CMakeLists.txt     # Build configuration
```

### Expected Performance Gains
- 1.5-2× faster set operations
- Reduced deduplication overhead

### Implementation Notes
- 2-3 days effort
- Use C++ hash set for deduplication
- Batch row comparison

---

## 9. Success Criteria

### Phase 19: Query Optimization
- [ ] Column pruning 1.5-2× faster
- [ ] AST traversal optimized
- [ ] No breaking changes to API

### Phase 20: Hash JOIN
- [ ] JOIN matches SQLite performance at all scales
- [ ] 1.5-2× speedup over current implementation
- [ ] All JOIN tests pass (inner, left, right, full)

### Phase 21: Batch DML / CTE
- [ ] 2-3× faster batch INSERT (1000+ rows)
- [ ] 2-3× faster recursive CTE
- [ ] ON CONFLICT handling correct

### Phase 22: VM Context
- [ ] 1.5-2× faster row operations
- [ ] All context tests pass
- [ ] Memory copying reduced

### Phase 23: Window Functions
- [ ] 2-3× faster window queries
- [ ] All window functions supported
- [ ] Frame handling correct

### Phase 24: Set Operations
- [ ] 1.5-2× faster UNION/INTERSECT/EXCEPT
- [ ] Deduplication correct
- [ ] ALL modifier supported

### General Requirements
- [ ] All tests pass with `-t` (pure Go)
- [ ] All tests pass with `-t -n` (CGO)
- [ ] C++ builds output to `.build/cmake/lib/`
- [ ] LD_LIBRARY_PATH set correctly for CGO
- [ ] No breaking changes to existing API
- [ ] Benchmark improvements documented

---

## 10. Implementation Effort Estimates

| Phase | Component | Effort | Expected Speedup | Priority |
|-------|-----------|--------|------------------|----------|
| 19 | Query Optimization | 2-3 days | 1.5-2× | ⭐ High |
| 20 | Hash JOIN | 3-4 days | 1.5-2× | 🔥 Critical |
| 21 | Batch DML / CTE | 4-5 days | 2-3× | 🔥 Critical |
| 22 | VM Context | 3-4 days | 1.5-2× | ⭐ High |
| 23 | Window Functions | 4-5 days | 2-3× | ⭐ High |
| 24 | Set Operations | 2-3 days | 1.5-2× | 📦 Medium |

**Total Estimated Effort:** 18-24 days
**Overall Expected Speedup:** 1.5-3× for remaining bottlenecks

---

## 11. Recommended Implementation Order

1. **Phase 20 (Hash JOIN)** - JOIN is still slower than SQLite, highest visibility
2. **Phase 21 (Batch DML)** - INSERT/UPDATE/DELETE are common operations
3. **Phase 19 (Query Optimization)** - Affects all SELECT queries
4. **Phase 22 (VM Context)** - Row operations are frequent
5. **Phase 23 (Window Functions)** - Complex queries benefit most
6. **Phase 24 (Set Operations)** - Lower frequency, but still useful

---

## 12. Risk Assessment

| Phase | Risk Level | Mitigation |
|-------|-----------|------------|
| 19 | Low | Well-defined API, batch operations |
| 20 | Medium | Hash table complexity, test coverage critical |
| 21 | Medium | DML correctness critical, ON CONFLICT edge cases |
| 22 | Low | Simple row operations, easy to test |
| 23 | Medium | Window frame logic complex, partition handling |
| 24 | Low | Set operations straightforward, easy fallback |

---

## 13. Notes

### General Principles
- Build outputs stay in `.build/` directory
- Pure Go is default, CGO is opt-in via `-n` flag
- Each component can be independently enabled/disabled
- CGO implementations must produce identical results to pure Go

### CGO-VM Specific Guidelines

1. **Minimize CGO Boundary Crossings**
   - Batch operations (1000+ rows per call)
   - Avoid per-row CGO calls
   - Keep data in C memory across operations

2. **Target High-Impact Operations First**
   - Phase 20: Hash JOIN (matches SQLite performance)
   - Phase 21: Batch DML (common operations)
   - Phase 19: Query optimization (all queries)

3. **Use Mature C++ Libraries**
   - `std::unordered_map` for hash tables
   - `std::string_view` for zero-copy string handling
   - Avoid external dependencies when possible

4. **Performance Targets**
   - Hash JOIN: Match or exceed SQLite (currently 1.0-1.2× slower)
   - Batch DML: 2-3× speedup for 1000+ rows
   - Query optimization: 1.5-2× compilation speedup

5. **Testing Requirements**
   - All existing tests must pass with CGO
   - JOIN correctness tests (all join types)
   - DML correctness tests (INSERT/UPDATE/DELETE)
   - Benchmark comparison against pure Go and SQLite
