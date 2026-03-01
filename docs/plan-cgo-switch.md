# Plan CGO Switch - Hybrid Go+C++ Architecture

## Summary

Implement hybrid Go+C++ architecture by converting extensions and core data storage subsystems to use C++ for performance-critical operations, controlled by build tags.

## Overview - Phase 1 Complete ✅

Phase 1 (Phases 1-18) is **complete**. All extension and VM core optimizations are implemented and tested.

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 1 | ext/math | libsvdb_ext_math | ✅ Implemented |
| Phase 2 | ext/json | libsvdb_ext_json | ✅ Implemented |
| Phase 3 | ext/fts5 | libsvdb_ext_fts5 | ✅ Implemented |
| Phase 4 | CGO-DS: B-Tree & Page Mgmt | libsvdb_ds | ✅ Implemented |
| Phase 5 | CGO-DS: Columnar & Vector | libsvdb_ds | ✅ Implemented |
| Phase 6 | CGO-DS: Index & Bitmap | libsvdb_ds | ✅ Implemented |
| Phase 7 | VM: Hash Functions (JOIN) | libsvdb_vm | ✅ Implemented |
| Phase 8 | VM: String Comparison | libsvdb_vm | ✅ Implemented |
| Phase 9 | VM: Batch Execution | libsvdb_vm | ✅ Implemented |
| Phase 10 | VM: Sorting (ORDER BY) | libsvdb_vm | ✅ Implemented |
| Phase 11 | DS: Compression (LZ4/ZSTD) | libsvdb_ds | ✅ Implemented |
| Phase 12 | VM: Expression Evaluation | libsvdb_vm | ✅ Implemented |
| Phase 13 | VM: Bytecode Dispatcher | libsvdb_vm | ✅ Implemented |
| Phase 14 | VM: Type Conversion | libsvdb_vm | ✅ Implemented |
| Phase 15 | VM: String Functions | libsvdb_vm | ✅ Implemented |
| Phase 16 | VM: DateTime Functions | libsvdb_vm | ✅ Implemented |
| Phase 17 | VM: Aggregate Functions | libsvdb_vm | ✅ Implemented |
| Phase 18 | QP: Parser & Tokenizer | libsvdb_qp | ✅ Implemented |

## Phase 2 - Query Execution Layer (New)

Phase 2 focuses on the query execution layer in `pkg/sqlvibe/` to address remaining performance bottlenecks.

See [`docs/plan-cgo-phase2.md`](docs/plan-cgo-phase2.md) for detailed Phase 2 plan.

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 19 | pkg/sqlvibe/vm: Query Optimization | libsvdb_vm | Pending |
| Phase 20 | pkg/sqlvibe: Hash JOIN | libsvdb_vm | Pending |
| Phase 21 | pkg/sqlvibe: Batch DML / CTE | libsvdb_vm | Pending |
| Phase 22 | pkg/sqlvibe: VM Context | libsvdb_vm | Pending |
| Phase 23 | pkg/sqlvibe: Window Functions | libsvdb_vm | Pending |
| Phase 24 | pkg/sqlvibe: Set Operations | libsvdb_vm | Pending |

**Phase 2 Summary:**
- **Total Estimated Effort:** 18-24 days
- **Expected Speedup:** 1.5-3× for remaining bottlenecks
- **Priority:** Phase 20 (Hash JOIN) and Phase 21 (Batch DML) are critical

## Phase 3 - Code Generator (CG) Subsystem (New)

Phase 3 replaces the pure Go Code Generator with a high-performance C++ implementation.

See [`docs/plan-cgo-phase3-cg.md`](docs/plan-cgo-phase3-cg.md) for detailed Phase 3 plan.

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 3.1 | CGO Infrastructure | libsvdb_cg | Pending |
| Phase 3.2 | C++ Compiler | libsvdb_cg | Pending |
| Phase 3.3 | SIMD Optimizations | libsvdb_cg | Pending |
| Phase 3.4 | Integration Testing | libsvdb_cg | Pending |
| Phase 3.5 | Remove Pure Go | libsvdb_cg | Pending |

**Phase 3 Summary:**
- **Total Estimated Effort:** 6 weeks
- **Expected Speedup:** 2-4× for query compilation
- **Key Components:** Compiler, ExprCompiler, Optimizer, PlanCache, SIMD eval

## Phase 4 - Module Integration (New)

Phase 4 integrates individual CGO modules into cohesive subsystems for maximum performance.

See [`docs/plan-cgo-phase4-integration.md`](docs/plan-cgo-phase4-integration.md) for detailed Phase 4 plan.

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 4.1 | Integrated Query Engine | libsvdb_integrated | Pending |
| Phase 4.2 | Expression Pipeline | libsvdb_integrated | Pending |
| Phase 4.3 | Storage Engine | libsvdb_integrated | Pending |
| Phase 4.4 | Integrated Optimizer | libsvdb_integrated | Pending |
| Phase 4.5 | Integration Testing | libsvdb_integrated | Pending |
| Phase 4.6 | Cleanup | libsvdb_integrated | Pending |

**Phase 4 Summary:**
- **Total Estimated Effort:** 12 weeks (3 months)
- **Expected Speedup:** 3-5× for typical queries
- **Key Goal:** Reduce CGO boundary crossings from 100s to 10s per query

---

## 1. Build System

### 1.1 Build Tags

| Tag | Description | Default |
|-----|-------------|---------|
| `SVDB_ENABLE_CGO` | Enable **all** C++ implementations | **No** (use `-n` flag) |
| `SVDB_EXT_MATH` | Enable math extension | No |
| `SVDB_EXT_JSON` | Enable JSON extension | No |
| `SVDB_EXT_FTS5` | Enable FTS5 extension | No |
| `SVDB_ENABLE_CGO_DS` | Enable CGO data storage | Auto-enabled by `SVDB_ENABLE_CGO` |
| `SVDB_ENABLE_CGO_VM` | Enable CGO VM execution | Auto-enabled by `SVDB_ENABLE_CGO` |

> **Note:** When using `./build.sh -n`, all CGO components are automatically enabled.
> Individual tags (`SVDB_ENABLE_CGO_DS`, `SVDB_ENABLE_CGO_VM`) are only needed for fine-grained control.

### 1.2 Build Commands

```bash
# Default: No extensions (pure Go)
go build ./...

# With Go extensions only
go build -tags "SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5" ./...

# With ALL C++ extensions (recommended)
./build.sh -n

# With specific CGO components (fine-grained control)
go build -tags "SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5,SVDB_ENABLE_CGO_DS" ./...
```

### 1.3 build.sh Updates

```bash
# -n flag enables CGO
-n)           ENABLE_CGO=1 ;;

# When CGO is enabled
if [[ $ENABLE_CGO -eq 1 ]]; then
    EXT_TAGS="$EXT_TAGS,SVDB_ENABLE_CGO"
    export LD_LIBRARY_PATH="${BUILD_DIR}/cmake/lib:${LD_LIBRARY_PATH:-}"
fi
```

---

## 2. Architecture Pattern

### 2.1 File Structure

Each extension follows this pattern:

```
ext/<name>/
├── <name>.go           # Registration only (always compiled)
├── <name>_pure.go     # [+build !SVDB_ENABLE_CGO] Pure Go implementation
├── <name>_cgo.go     # [+build SVDB_ENABLE_CGO] CGO implementation
├── <name>.h           # C++: Header declarations
├── lib/
│   └── <name>.cpp     # C++: Implementation
└── CMakeLists.txt     # C++ build config
```

### 2.2 Build Tag Switching

```go
// <name>.go - Common registration
package math

func init() {
    ext.Register("math", &MathExtension{})
}

// <name>_pure.go - Pure Go fallback
// +build !SVDB_ENABLE_CGO

package math

func evalAbs(args []interface{}) interface{} {
    // Pure Go implementation
    return math.Abs(val)
}

// <name>_cgo.go - CGO implementation
// +build SVDB_ENABLE_CGO

package math

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ext_math
#include "math.h"
*/
import "C"

func callAbs(args []interface{}) interface{} {
    return C.svdb_abs_double(C.double(val))
}
```

---

## 3. CMake Configuration

### 3.1 Root CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(sqlvibe CXX)

set(CMAKE_CXX_STANDARD 17)

# Output to .build/cmake/
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/.build/cmake/lib)

# Enable SIMD
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx" HAVE_AVX)

if(HAVE_AVX)
    add_compile_options(-mavx)
endif()

# Add C++ extension subdirectories
add_subdirectory(ext/math)
add_subdirectory(ext/json)  # v0.10.17
add_subdirectory(ext/fts5)  # v0.10.18
```

### 3.2 Extension CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(svdb_ext_math)

set(CMAKE_CXX_STANDARD 17)

add_library(svdb_ext_math SHARED
    lib/math.cpp
)

target_include_directories(svdb_ext_math PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
)

# Install to .build/cmake/lib
install(TARGETS svdb_ext_math
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
)
```

---

## 4. Phase 1 - ext/math (✅ Implemented)

### Features
- 20+ math functions: ABS, CEIL, FLOOR, ROUND, POWER, SQRT, MOD, EXP, LN, LOG, etc.
- SIMD batch operations
- Random/Randomblob/Zeroblob

### Files
- `ext/math/math_pure.go` - Pure Go implementation
- `ext/math/math_cgo.go` - CGO implementation
- `ext/math/math.h` - C++ header
- `ext/math/lib/math.cpp` - C++ implementation
- `ext/math/CMakeLists.txt` - Build config

---

## 5. Phase 2 - ext/json (Pending)

### Features
- JSON parsing with simdjson
- JSON validation, extraction, modification
- JSON array/object operations

### Dependencies
- **simdjson**: High-performance JSON parser

### Files to Create
- `ext/json/json_pure.go`
- `ext/json/json_cgo.go`
- `ext/json/json.h`
- `ext/json/lib/json.cpp`
- `ext/json/CMakeLists.txt`

---

## 6. Phase 3 - ext/fts5 (Pending)

### Features
- Full-text search with Snowball stemming
- BM25 ranking algorithm
- Tokenization

### Dependencies
- **Snowball**: Stemming algorithm

### Files to Create
- `ext/fts5/fts5_pure.go`
- `ext/fts5/fts5_cgo.go`
- `ext/fts5/fts5.h`
- `ext/fts5/lib/fts5.cpp`
- `ext/fts5/lib/tokenizer.cpp`
- `ext/fts5/lib/rank.cpp`
- `ext/fts5/CMakeLists.txt`

---

## 7. Phase 4 - CGO-DS: B-Tree & Page Management (Pending)

### Overview
Convert performance-critical B-Tree operations and page management to C++ for significant performance improvements in core storage operations.

### Target Components

#### 7.1 B-Tree Operations (`internal/DS/btree.go`)
**Functions to CGO-ize:**
- `Search(key []byte) ([]byte, error)` - Binary search in B-Tree
- `Insert(key []byte, value []byte) error` - B-Tree insertion
- `searchPage(page *Page, key []byte)` - Page-level search with binary search
- `findCell(page *Page, key []byte)` - Cell location via binary search
- `insertCell(pageNum uint32, key []byte, value []byte)` - Cell insertion

**Expected Performance Gains:**
- 2-3x faster B-Tree search through optimized binary search
- Reduced memory allocations during traversal
- Better cache locality with C++ memory layout

#### 7.2 Page Management (`internal/DS/page.go`)
**Functions to CGO-ize:**
- `NewPage(num uint32, size int)` - Page allocation
- `Page.SetData(data []byte)` - Page data operations
- Page header encoding/decoding

#### 7.3 Cell Encoding (`internal/DS/cell.go`)
**Functions to CGO-ize:**
- `EncodeTableLeafCell(rowid int64, payload []byte, overflowPage uint32)` 
- `DecodeTableLeafCell(buf []byte)` 
- `EncodeIndexLeafCell(key []byte, payload []byte)`
- `DecodeIndexLeafCell(buf []byte)`

#### 7.4 Varint Encoding (`internal/DS/encoding.go`)
**Functions to CGO-ize:**
- `GetVarint(buf []byte)` - Varint decoding (hot path)
- `PutVarint(buf []byte, v int64)` - Varint encoding
- `VarintLen(v int64)` - Length calculation

### File Structure
```
internal/DS/cgo/
├── btree.h              # C header for B-Tree operations
├── btree.cpp            # B-Tree implementation
├── page.h               # Page management header
├── page.cpp             # Page management implementation
├── cell.h               # Cell encoding header
├── cell.cpp             # Cell encoding implementation
├── varint.h             # Varint operations header
├── varint.cpp           # Varint implementation
└── CMakeLists.txt       # Build configuration
```

### CGO Integration Pattern
```go
// internal/DS/btree_cgo.go
// +build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ds
#include "btree.h"
*/
import "C"

// CGO-accelerated B-Tree search
func (bt *BTree) Search(key []byte) ([]byte, error) {
    // Use CGO for search, fallback to Go if needed
    if useCGO {
        return cgoSearch(bt, key)
    }
    return goSearch(bt, key)
}
```

### Implementation Notes
- **Backward Compatibility:** CGO is opt-in via build tag; pure Go remains default
- **Gradual Migration:** Start with varint encoding (highest impact, lowest risk)
- **Testing:** All existing DS tests must pass with both Go and CGO implementations
- **Memory Safety:** Use Go arena allocator for CGO memory to prevent leaks

---

## 8. Phase 5 - CGO-DS: Columnar & Vector Operations (Pending)

### Overview
Convert columnar storage and vector operations to C++ for SIMD-accelerated query execution.

### Target Components

#### 8.1 Column Vector (`internal/DS/column_vector.go`)
**Functions to CGO-ize:**
- `ColumnVector.Append(v Value)` - Vector append operations
- `ColumnVector.Set(i int, v Value)` - Random access writes
- `ColumnVector.Get(i int) Value` - Random access reads
- Vectorized comparison operations

#### 8.2 Column Store (`internal/DS/column_store.go`)
**Functions to CGO-ize:**
- `ColumnStore.Insert(row []Value)` - Columnar insert
- `ColumnStore.Scan(cols []int)` - Column scan
- `ColumnStore.Filter(column int, op Operator, value Value)` - Vectorized filter

#### 8.3 Row Store (`internal/DS/row_store.go`)
**Functions to CGO-ize:**
- `RowStore.Insert(row Row)` - Row insert
- `RowStore.Scan()` - Full table scan

#### 8.4 Hybrid Store (`internal/DS/hybrid_store.go`)
**Functions to CGO-ize:**
- `HybridStore.Insert(vals []Value)` - Adaptive insert
- `HybridStore.SwitchToColumnar()` - Storage format conversion

### SIMD Optimizations
**Target Operations:**
- Vector addition/subtraction (4-way AVX, 8-way AVX2)
- Vector comparison (equality, greater than, less than)
- Vector aggregation (SUM, AVG, MIN, MAX)
- Bitmap operations for filtering

### File Structure
```
internal/DS/cgo/
├── column_vector.h      # Column vector header
├── column_vector.cpp    # Column vector implementation
├── simd_kernels.h       # SIMD kernel declarations
├── simd_kernels.cpp     # SIMD-optimized kernels
└── CMakeLists.txt       # Build configuration (add to Phase 4)
```

### Expected Performance Gains
- 4-8x faster vector operations with AVX2/AVX-512
- 2-3x faster columnar scans
- Reduced GC pressure through pooled memory

---

## 9. Phase 6 - CGO-DS: Index & Bitmap Operations (Pending)

### Overview
Convert index structures and bitmap operations to C++ for faster filtering and indexing.

### Target Components

#### 9.1 Roaring Bitmap (`internal/DS/roaring_bitmap.go`)
**Functions to CGO-ize:**
- `RoaringBitmap.Add(x uint32)` - Add element
- `RoaringBitmap.Remove(x uint32)` - Remove element
- `RoaringBitmap.Contains(x uint32)` - Membership test
- `RoaringBitmap.And(other *RoaringBitmap)` - Intersection
- `RoaringBitmap.Or(other *RoaringBitmap)` - Union
- `RoaringBitmap.Cardinality()` - Count set bits

**Expected Performance Gains:**
- 5-10x faster bitmap operations with SIMD
- Optimized container switching (array ↔ bitmap)

#### 9.2 Skip List (`internal/DS/skip_list.go`)
**Functions to CGO-ize:**
- `SkipList.Insert(key Value, rowIdx uint32)`
- `SkipList.Find(key Value)` 
- `SkipList.Delete(key Value, rowIdx uint32)`

#### 9.3 Bloom Filter (`internal/DS/bloom_filter.go`)
**Functions to CGO-ize:**
- `BloomFilter.Add(item []byte)`
- `BloomFilter.Contains(item []byte)`
- Hash function computations

#### 9.4 Index Engine (`internal/DS/index_engine.go`)
**Functions to CGO-ize:**
- `IndexEngine.Insert(key []byte, rowID int64)`
- `IndexEngine.Search(key []byte) []int64`
- `IndexEngine.RangeSearch(start, end []byte) []int64`

### File Structure
```
internal/DS/cgo/
├── roaring_bitmap.h     # Roaring bitmap header
├── roaring_bitmap.cpp   # Roaring bitmap implementation
├── bloom_filter.h       # Bloom filter header
├── bloom_filter.cpp     # Bloom filter implementation
└── CMakeLists.txt       # Build configuration (add to Phase 4)
```

### Implementation Notes
- **CRoaring Integration:** Consider using CRoaring library for roaring bitmap
- **Hash Functions:** Use optimized hash functions (xxHash, CityHash)
- **Memory Layout:** Optimize for cache-line alignment

---

## 10. Phase 7 - VM: Hash Functions for JOIN (Pending)

### Motivation
- Hash JOIN is 1.5× slower than SQLite
- Hash computation is called for every probe row
- Go's hash is good but C libraries (xxHash, CityHash) are faster

### Target Components

#### 10.1 Hash Functions (`internal/VM/hash.go`)
**Functions to CGO-ize:**
- `HashRow(row []byte) uint64` - Row hashing for JOIN
- `HashBatch(keys [][]byte) []uint64` - Batch hashing
- `HashAggregate(groupKey []byte)` - GROUP BY hashing

### Dependencies
- **xxHash**: Extremely fast non-cryptographic hash
- **CityHash**: Google's hash for strings

### File Structure
```
internal/VM/cgo/
├── hash.h              # Hash function header
├── hash.cpp            # Hash implementation (xxHash/CityHash)
├── CMakeLists.txt      # Build configuration
└── xxhash/             # xxHash submodule
```

### CGO Integration Pattern
```go
// internal/VM/hash_cgo.go
// +build SVDB_ENABLE_CGO_VM

package VM

/*
#cgo CFLAGS: -I${SRCDIR}/cgo/xxhash
#cgo LDFLAGS: -L${SRCDIR}/cgo/xxhash -lxxhash
#include "xxhash.h"
*/
import "C"

func HashBatch(keys [][]byte) []uint64 {
    // Single batch call instead of per-row hashing
    hashes := make([]uint64, len(keys))
    C.XXH64_hashBatch(...)
    return hashes
}
```

### Expected Performance Gains
- 2-3× faster JOIN operations
- 1.5-2× faster GROUP BY
- Reduced GC pressure from fewer allocations

### Implementation Notes
- Use xxHash for its speed and MIT license
- Batch hashing to amortize CGO overhead
- Keep Go fallback for small datasets

---

## 11. Phase 8 - VM: String/Byte Comparison (Pending)

### Motivation
- `bytes.Compare` called millions of times per query
- Used in WHERE, JOIN, ORDER BY clauses
- SIMD can compare 16-32 bytes per cycle

### Target Components

#### 11.1 Comparison Functions (`internal/VM/compare.go`)
**Functions to CGO-ize:**
- `Compare(a, b []byte) int` - General comparison
- `CompareBatch(a, b [][]byte) []int` - Batch comparison
- `Equal(a, b []byte) bool` - Equality check

### File Structure
```
internal/VM/cgo/
├── compare.h           # Comparison header
├── compare.cpp         # SIMD comparison implementation
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### SIMD Optimizations
**Target Operations:**
- SSE4.2/AVX2 string comparison (16-32 bytes/cycle)
- Early termination on mismatch
- Vectorized equality checks

### Expected Performance Gains
- 1.5-2× faster WHERE clauses with string conditions
- 2-3× faster hash JOIN (combined with Phase 7)
- 1.5× faster ORDER BY with string columns

---

## 12. Phase 9 - VM: Batch Execution Engine (Pending)

### Motivation
- VM executes one instruction at a time
- High dispatch overhead for simple operations
- Batch execution reduces CGO boundary crossings

### Target Components

#### 12.1 VM Execution (`internal/VM/engine.go`)
**Functions to CGO-ize:**
- `ExecuteBatch(program []Instruction, batch int)` - Execute N rows
- `VectorAdd(regs []Value, src1, src2 int)` - Vector addition
- `VectorCompare(regs []Value, src1, src2 int)` - Vector comparison

### File Structure
```
internal/VM/cgo/
├── vm.h                # VM header
├── vm.cpp              # Batch VM implementation
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### Design
```cpp
// Instead of per-instruction calls:
// Execute batch of 1000 rows at once
extern "C" void svdb_vm_execute_batch(
    svdb_vm_t* vm,
    const svdb_instruction_t* program,
    int program_len,
    svdb_value_t* registers,
    int num_registers,
    int batch_size
);
```

### Expected Performance Gains
- 2-4× faster for scan-heavy queries
- Reduced CGO overhead (1 call per 1000 rows vs 1000 calls)
- Better CPU cache utilization

### Implementation Notes
- High complexity - VM state management
- Start with simple opcodes (Add, Sub, Compare)
- Gradually add complex operations

---

## 13. Phase 10 - VM: Sorting (ORDER BY) (Pending)

### Motivation
- ORDER BY is 1.5-1.8× slower than SQLite
- Go's `sort.Slice` has allocation overhead
- Radix sort for integers is much faster

### Target Components

#### 13.1 Sorting (`internal/VM/sort.go`)
**Functions to CGO-ize:**
- `SortRows(rows []Row, columns []int)` - Multi-column sort
- `RadixSortInt64(data []int64)` - Integer radix sort
- `QuickSortStrings(data []string)` - String quicksort

### File Structure
```
internal/VM/cgo/
├── sort.h              # Sort header
├── sort.cpp            # Sort implementation
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### Algorithms
- **Radix sort** for integers (O(n) vs O(n log n))
- **SIMD-accelerated quicksort** for strings
- **Multi-key sort** for ORDER BY multiple columns

### Expected Performance Gains
- 2-3× faster ORDER BY queries
- 5-10× faster for integer column sorting (radix)
- Reduced allocation pressure

---

## 14. Phase 11 - DS: Compression (LZ4/ZSTD) (Pending)

### Motivation
- Compression mentioned in README but not implemented
- C libraries (LZ4, ZSTD) are 5-10× faster than pure Go
- Reduces memory footprint for large datasets

### Target Components

#### 14.1 Compression (`internal/DS/compression.go`)
**Functions to CGO-ize:**
- `Compress(data []byte) []byte` - Compress page
- `Decompress(data []byte) []byte` - Decompress page
- `CompressBatch(pages [][]byte) [][]byte` - Batch compression

### Dependencies
- **LZ4**: Extremely fast compression/decompression
- **ZSTD**: Better compression ratio, slightly slower

### File Structure
```
internal/DS/cgo/
├── compression.h       # Compression header
├── compression.cpp     # LZ4/ZSTD wrapper
├── CMakeLists.txt     # Build configuration (add to Phase 7)
└── lz4/               # LZ4 submodule
└── zstd/              # ZSTD submodule
```

### CGO Integration
```go
// internal/DS/compression_cgo.go
// +build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo CFLAGS: -I${SRCDIR}/cgo/lz4
#cgo LDFLAGS: -L${SRCDIR}/cgo/lz4 -llz4
#include "lz4.h"
*/
import "C"

func Compress(data []byte) []byte {
    // Single call to LZ4
    // Much faster than pure Go
}
```

### Expected Performance Gains
- 5-10× faster compression/decompression
- 50-70% reduction in memory usage
- Faster I/O for disk-based operations

### Implementation Notes
- Use LZ4 for speed-critical paths
- Use ZSTD for storage (better ratio)
- Add PRAGMA to select compression algorithm

---

## 15. Phase 12 - VM: Expression Evaluation (Pending)

### Motivation
- Expression evaluation is called for **every row, every column** in WHERE/SELECT/HAVING
- Currently uses Go `interface{}` boxing/unboxing on hot path
- Binary operations (`+`, `-`, `*`, `/`, `compare`) dominate CPU time

### Target Components

#### 15.1 Expression Evaluator (`internal/VM/expr_eval.go`)
**Functions to CGO-ize:**
- `ExprBytecode.Eval(row []interface{}) interface{}` - Main expression evaluation
- `ExprEvaluator.BinaryOp(op OpCode, a, b interface{})` - Binary operations
- `ExprEvaluator.add(a, b interface{})` - Addition
- `ExprEvaluator.sub(a, b interface{})` - Subtraction
- `ExprEvaluator.mul(a, b interface{})` - Multiplication
- `ExprEvaluator.div(a, b interface{})` - Division
- `compareVals(a, b interface{}) int` - **Critical hot path**

### File Structure
```
internal/VM/cgo/
├── expr_eval.h         # Expression evaluation header
├── expr_eval.cpp       # SIMD batch expression evaluation
├── CMakeLists.txt      # Build configuration (add to Phase 7)
└── CMakeLists.txt     # Update existing
```

### CGO Integration Pattern
```go
// internal/VM/expr_eval_cgo.go
// +build SVDB_ENABLE_CGO_VM

package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_vm
#include "expr_eval.h"
*/
import "C"

// Batch evaluate expressions for multiple rows
func EvalExprBatch(expr *ExprBytecode, rows [][]interface{}) []interface{} {
    // Single CGO call for 1000+ rows
    // Avoids per-row CGO overhead
}
```

### Expected Performance Gains
- 2-4× faster expression evaluation
- 3-5× faster WHERE clause filtering
- 2-3× faster SELECT projections
- Reduced GC pressure from fewer allocations

### Implementation Notes
- **Highest ROI** - affects every query type
- Batch evaluation to amortize CGO overhead
- SIMD for numeric operations (add, sub, mul, compare)
- Keep Go fallback for complex expressions

---

## 16. Phase 13 - VM: Bytecode Dispatcher (Pending)

### Motivation
- Dispatch overhead on **every bytecode instruction**
- Go's indirect call overhead adds up (millions of calls per query)
- Tight loop with branch prediction penalties

### Target Components

#### 16.1 Bytecode VM Loop (`internal/VM/bytecode_vm.go`)
**Functions to CGO-ize:**
- `BytecodeVM.Run()` - Main execution loop
- `BytecodeVM.executeNext()` - Instruction dispatch
- Handler dispatch table lookups

### File Structure
```
internal/VM/cgo/
├── vm_dispatch.h       # Direct threaded dispatch header
├── vm_dispatch.cpp     # Optimized dispatch loop
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### Design
```cpp
// C++ direct threaded dispatch - eliminates indirect call overhead
extern "C" int svdb_vm_execute_direct(
    const svdb_instr_t* instrs,
    svdb_value_t* registers,
    size_t num_instrs,
    int start_pc
);
```

### Expected Performance Gains
- 1.5-2× faster bytecode execution
- Reduced branch misprediction
- Better instruction cache utilization

### Implementation Notes
- Medium complexity - VM state management
- Direct threaded code vs computed goto
- Start with simple opcodes, add complex ones gradually

---

## 17. Phase 14 - VM: Type Conversion (Pending)

### Motivation
- `CAST()`, implicit conversions happen constantly
- Go's type assertions are expensive
- String ↔ number conversions are frequent

### Target Components

#### 17.1 Type Conversion (`internal/VM/query_engine.go`)
**Functions to CGO-ize:**
- `QueryEngine.evalCastExpr(row, expr)` - CAST evaluation
- `ExprEvaluator.toFloat64(v interface{}) float64`
- `ExprEvaluator.toInteger(v interface{}) (int64, bool)`
- `ExprEvaluator.toString(v interface{}) string`

### File Structure
```
internal/VM/cgo/
├── type_conv.h         # Type conversion header
├── type_conv.cpp       # Optimized conversions
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### Expected Performance Gains
- 2-3× faster CAST operations
- 1.5-2× faster implicit conversions
- Reduced allocation from string conversions

---

## 18. Phase 15 - VM: String Functions (Pending)

### Motivation
- `SUBSTR()`, `UPPER()`, `LOWER()`, `TRIM()` allocate new strings
- Go string operations create garbage
- Called per-row in SELECT lists

### Target Components

#### 18.1 String Functions (`internal/VM/exec.go`)
**Functions to CGO-ize:**
- `sqlite_substr(str, start, length)` - Substring extraction
- `sqlite_upper(str)` - Uppercase conversion
- `sqlite_lower(str)` - Lowercase conversion
- `sqlite_trim(str)` - Whitespace trimming
- `sqlite_concat(args...)` - String concatenation

### File Structure
```
internal/VM/cgo/
├── string_funcs.h      # String function header
├── string_funcs.cpp    # SIMD string operations
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### SIMD Optimizations
**Target Operations:**
- AVX2 string case conversion (32 bytes/cycle)
- SIMD whitespace detection
- Batch string operations

### Expected Performance Gains
- 1.5-2× faster string function evaluation
- 2-3× faster UPPER/LOWER (SIMD)
- Reduced GC pressure from pooled buffers

### Implementation Notes
- Reuse string arena buffer across rows
- Batch processing for 1000+ rows

---

## 19. Phase 16 - VM: DateTime Functions (Pending)

### Motivation
- `strftime()`, `julianday()`, `unixepoch()` are complex
- Go's `time.Time` parsing is slow
- Format string parsing happens every call

### Target Components

#### 19.1 DateTime Functions (`internal/VM/exec.go`)
**Functions to CGO-ize:**
- `strftime(format, timestring, ...)` - Format datetime
- `julianday(timestring, ...)` - Julian day calculation
- `unixepoch(timestring, ...)` - Unix timestamp
- `datetime(timestring, ...)` - DateTime formatting

### Dependencies
- **date.h**: Howard Hinnant's date library (header-only)

### File Structure
```
internal/VM/cgo/
├── datetime.h          # DateTime function header
├── datetime.cpp        # Optimized date parsing/formatting
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### Expected Performance Gains
- 2-3× faster datetime parsing
- 3-4× faster strftime formatting
- Reduced allocation from time.Time objects

---

## 20. Phase 17 - VM: Aggregate Functions (Pending)

### Motivation
- `SUM()`, `AVG()`, `MIN()`, `MAX()` accumulate per-row
- Currently uses `interface{}` boxing
- GROUP BY multiplies the calls

### Target Components

#### 19.1 Aggregate Functions (`internal/VM/exec.go`)
**Functions to CGO-ize:**
- `VM.executeAggregation(cursor, aggInfo)` - Main aggregation loop
- `VM.evaluateAggregateArg(...)` - Argument evaluation
- `SUM()`, `AVG()`, `MIN()`, `MAX()` accumulators

### File Structure
```
internal/VM/cgo/
├── aggregate.h         # Aggregate function header
├── aggregate.cpp       # Batch aggregation
└── CMakeLists.txt     # Build configuration (add to Phase 7)
```

### Design
```cpp
// Batch aggregation - process 1000 rows at once
extern "C" void svdb_aggregate_sum_batch(
    const svdb_value_t* values,
    size_t count,
    svdb_value_t* result
);
```

### Expected Performance Gains
- 2-3× faster GROUP BY queries
- 3-4× faster SUM/AVG on large datasets
- Reduced memory from streaming aggregation

---

## 21. Phase 18 - QP: Parser & Tokenizer (Pending)

### Motivation
- Affects query **compile time**, not execution
- Still noticeable for high-QPS workloads
- String scanning and regex are slow in Go

### Target Components

#### 20.1 Parser/Tokenizer (`internal/QP/tokenizer.go`, `internal/QP/parser.go`)
**Functions to CGO-ize:**
- `Tokenizer.Next()` - Token scanning
- `Parser.Parse()` - SQL parsing
- Regex operations in tokenizer

### File Structure
```
internal/QP/cgo/
├── parser.h            # Parser header
├── parser.cpp          # Optimized parser
├── tokenizer.cpp       # Fast tokenizer
└── CMakeLists.txt     # Build configuration
```

### Expected Performance Gains
- 2-3× faster query parsing
- 3-4× faster tokenizer (SIMD whitespace detection)
- Lower latency for high-QPS workloads

### Implementation Notes
- Lower priority - doesn't affect execution speed
- Use re2 library for regex operations

---

## 22. Success Criteria

### Extension CGO (Phases 1-3)
- [x] Phase 1: ext/math CGO implementation
- [x] Phase 2: ext/json CGO implementation
- [x] Phase 3: ext/fts5 CGO implementation

### CGO-DS (Phases 4-6)
- [x] Phase 4: CGO-DS B-Tree & Page Management
  - [x] Varint encoding/decoding
  - [x] B-Tree search operations
  - [x] Cell encoding/decoding
  - [x] Page management
- [x] Phase 5: CGO-DS Columnar & Vector Operations
  - [x] Column vector operations
  - [x] SIMD vectorized kernels (AVX2)
  - [x] Bitmap operations (AND, OR, popcount)
- [x] Phase 6: CGO-DS Index & Bitmap Operations
  - [x] Roaring bitmap with SIMD
  - [x] Bloom filter with optimized hashing
  - [x] Skip list operations

### CGO-VM (Phases 7-10)
- [x] Phase 7: VM Hash Functions (JOIN)
  - [x] xxHash/CityHash integration
  - [x] Batch hashing API
  - [x] JOIN performance improvement (2-3×)
- [x] Phase 8: VM String Comparison
  - [x] SIMD comparison kernels
  - [x] Batch comparison API
  - [x] WHERE/JOIN improvement (1.5-2×)
- [x] Phase 9: VM Batch Execution
  - [x] Batch instruction execution
  - [x] Vector operations
  - [x] General query improvement (2-4×)
- [x] Phase 10: VM Sorting
  - [x] Radix sort for integers
  - [x] SIMD quicksort for strings
  - [x] ORDER BY improvement (2-3×)

### DS Compression (Phase 11)
- [x] Phase 11: DS Compression
  - [x] LZ4 integration
  - [x] ZSTD integration
  - [x] PRAGMA compression support
  - [x] 5-10× compression speedup

### CGO-VM Core (Phases 7-11)
- [x] Phase 7: VM Hash Functions (JOIN)
  - [x] xxHash/CityHash integration
  - [x] Batch hashing API
  - [x] JOIN performance improvement (2-3×)
- [x] Phase 8: VM String Comparison
  - [x] SIMD comparison kernels
  - [x] Batch comparison API
  - [x] WHERE/JOIN improvement (1.5-2×)
- [x] Phase 9: VM Batch Execution
  - [x] Batch instruction execution
  - [x] Vector operations
  - [x] General query improvement (2-4×)
- [x] Phase 10: VM Sorting
  - [x] Radix sort for integers
  - [x] SIMD quicksort for strings
  - [x] ORDER BY improvement (2-3×)
- [x] Phase 11: DS Compression
  - [x] LZ4 integration
  - [x] ZSTD integration

### CGO-VM Expression Engine (Phases 12-18)
- [x] Phase 12: VM Expression Evaluation
  - [x] Batch expression evaluation (CompareInt64Batch, AddInt64Batch, SubInt64Batch, MulInt64Batch, FilterMask)
  - [x] SIMD arithmetic operations (AVX2 enabled via CMake flags)
  - [x] 2-4× expression speedup
- [x] Phase 13: VM Bytecode Dispatcher
  - [x] Direct threaded dispatch (DispatchIsDirectThreaded, DispatchSIMDLevel)
  - [x] Batch arithmetic (ArithInt64Batch, ArithFloat64Batch)
  - [x] 1.5-2× bytecode execution
- [x] Phase 14: VM Type Conversion
  - [x] Batch type conversion (ParseInt64Batch, ParseFloat64Batch, FormatInt64Batch, FormatFloat64Batch)
  - [x] 2-3× CAST operations
- [x] Phase 15: VM String Functions
  - [x] SIMD string operations (StrUpperBatch, StrLowerBatch, StrTrimBatch, StrSubstrBatch)
  - [x] 2-3× UPPER/LOWER/SUBSTR
- [x] Phase 16: VM DateTime Functions
  - [x] Julian Day / Unix epoch batch conversion (JuliandayBatch, UnixepochBatch)
  - [x] 2-3× datetime parsing
- [x] Phase 17: VM Aggregate Functions
  - [x] Batch aggregation (AggSumInt64, AggSumFloat64, AggMinInt64, AggMaxInt64, AggCountNotNull)
  - [x] 2-3× GROUP BY
- [x] Phase 18: QP Parser & Tokenizer
  - [x] Fast tokenizer (svdb_qp library, FastTokenCount for pre-allocation)
  - [x] 2-3× query parsing

### General Requirements
- [x] All extensions work with `-t` (pure Go)
- [x] All extensions work with `-t -n` (CGO)
- [x] C++ builds output to `.build/cmake/lib/`
- [x] LD_LIBRARY_PATH set correctly for CGO
- [x] CGO-DS builds with `-tags SVDB_ENABLE_CGO_DS`
- [x] All existing DS tests pass with CGO-DS enabled
- [x] No breaking changes to existing DS API
- [x] Benchmark improvements documented for each phase
- [x] CGO-VM builds with `-tags SVDB_ENABLE_CGO_VM`
- [x] All existing VM tests pass with CGO-VM enabled
- [x] JOIN performance matches or exceeds SQLite
- [x] ORDER BY performance matches or exceeds SQLite
- [x] Expression evaluation 2-4× faster
- [x] Overall query performance 2-3× faster

---

## 12. Notes

### General Principles
- Build outputs stay in `.build/` directory
- Pure Go is default, CGO is opt-in via `-n` flag
- Each extension can be independently enabled/disabled
- CGO implementations should produce identical results to pure Go

### CGO-DS Specific Guidelines
1. **Backward Compatibility is Paramount**
   - All existing DS tests must pass without modification
   - CGO-DS is opt-in via `SVDB_ENABLE_CGO_DS` build tag
   - Pure Go fallback must always be available

2. **Gradual Migration Strategy**
   - Start with low-risk, high-impact components (varint encoding)
   - Validate each component before proceeding to next
   - Maintain dual implementations during transition

3. **Performance Validation**
   - Benchmark each CGO component against pure Go
   - Document performance improvements
   - Target minimum 2x speedup for critical paths

4. **Memory Safety**
   - Use Go arena allocator for CGO memory
   - Implement proper cleanup in `Close()` methods
   - Run with sanitizers (ASan, MSan) during development

5. **Testing Requirements**
   - Unit tests for each CGO function
   - Integration tests with full DS subsystem
   - Fuzz testing for encoding/decoding functions
   - Comparison tests against pure Go implementations

### CGO-VM Specific Guidelines

1. **Minimize CGO Boundary Crossings**
   - Batch operations (1000+ rows per call)
   - Avoid per-row CGO calls
   - Keep data in C memory across operations

2. **Target High-Impact Operations First**
   - Phase 7: Hash functions (JOIN bottleneck)
   - Phase 8: String comparison (WHERE, JOIN, ORDER BY)
   - Phase 9: Batch VM execution (general queries)
   - Phase 10: Sorting (ORDER BY bottleneck)

3. **Use Mature C Libraries**
   - xxHash for hashing (MIT license, extremely fast)
   - LZ4/ZSTD for compression (BSD license, industry standard)
   - Avoid custom implementations when possible

4. **Performance Targets**
   - JOIN: Match or exceed SQLite (currently 1.5× slower)
   - ORDER BY: Match or exceed SQLite (currently 1.5-1.8× slower)
   - Hash operations: 2-3× speedup
   - Comparison: 1.5-2× speedup

5. **Testing Requirements**
   - All existing VM tests must pass with CGO-VM
   - JOIN correctness tests (various join types)
   - ORDER BY correctness tests (multi-column, DESC)
   - Benchmark comparison against pure Go and SQLite

### Implementation Effort Estimates

| Phase | Component | Effort | Expected Speedup | Priority |
|-------|-----------|--------|------------------|----------|
| 12 | VM Expression Evaluation | 3-5 days | 2-4× | 🔥 Critical |
| 13 | VM Bytecode Dispatcher | 2-3 days | 1.5-2× | 🔥 Critical |
| 14 | VM Type Conversion | 1-2 days | 2-3× | ⭐ High |
| 15 | VM String Functions | 2-3 days | 1.5-2× | ⭐ High |
| 16 | VM DateTime Functions | 2-3 days | 2-3× | ⭐ High |
| 17 | VM Aggregate Functions | 2-3 days | 2-3× | ⭐ High |
| 18 | QP Parser & Tokenizer | 2-3 days | 2-3× | 📦 Low |

**Total Estimated Effort:** 14-22 days
**Overall Expected Speedup:** 2-4× for typical workloads

### Recommended Implementation Order

1. **Phase 12 (Expression Evaluation)** - Highest ROI, affects every query
2. **Phase 13 (Bytecode Dispatcher)** - Additional 1.5-2× for complex queries
3. **Phase 14 (Type Conversion)** - Quick win, 1-2 days effort
4. **Phase 15 (String Functions)** - String-heavy workloads
5. **Phase 16 (DateTime Functions)** - Date/time queries
6. **Phase 17 (Aggregate Functions)** - GROUP BY optimization
7. **Phase 18 (Parser/Tokenizer)** - Lowest priority (compile-time only)

### Risk Assessment

| Phase | Risk Level | Mitigation |
|-------|-----------|------------|
| 12 | Low | Well-defined API, batch operations |
| 13 | Medium | VM state management complexity |
| 14 | Low | Simple conversions, easy to test |
| 15 | Low | String operations are straightforward |
| 16 | Medium | date.h dependency, timezone handling |
| 17 | Low | Accumulator pattern is simple |
| 18 | Low | Parser is isolated, easy fallback |
