# Plan CGO Switch - Hybrid Go+C++ Architecture

## Summary

Implement hybrid Go+C++ architecture by converting extensions and core data storage subsystems to use C++ for performance-critical operations, controlled by build tags.

## Overview

This plan covers converting key extensions and the DS (Data Storage) subsystem to use C++ while maintaining pure Go fallbacks:

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 1 | ext/math | libsvdb_ext_math | ✅ Implemented |
| Phase 2 | ext/json | libsvdb_ext_json | ✅ Implemented |
| Phase 3 | ext/fts5 | libsvdb_ext_fts5 | ✅ Implemented |
| Phase 4 | CGO-DS: B-Tree & Page Mgmt | libsvdb_ds | ✅ Implemented |
| Phase 5 | CGO-DS: Columnar & Vector | libsvdb_ds | ✅ Implemented |
| Phase 6 | CGO-DS: Index & Bitmap | libsvdb_ds | ✅ Implemented |
| Phase 7 | Engineering Tools | Containerfile | Pending |

---

## 1. Build System

### 1.1 Build Tags

| Tag | Description | Default |
|-----|-------------|---------|
| `SVDB_ENABLE_CGO` | Enable C++ implementations | **No** (use `-n` flag) |
| `SVDB_EXT_MATH` | Enable math extension | No |
| `SVDB_EXT_JSON` | Enable JSON extension | No |
| `SVDB_EXT_FTS5` | Enable FTS5 extension | No |
| `SVDB_ENABLE_CGO_DS` | Enable CGO data storage | No |

### 1.2 Build Commands

```bash
# Default: No extensions
go build ./...

# With Go extensions only
go build -tags "SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5" ./...

# With C++ extensions (enables SVDB_ENABLE_CGO)
./build.sh -t -n
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

## 10. Phase 7 - Engineering Tools (Pending)

### Containerfile

```dockerfile
FROM golang:1.26-bookworm

RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    ninja-build \
    clang-format \
    clang-tidy \
    cppcheck \
    lcov \
    llvm \
    ccache \
    valgrind \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

CMD ["./build.sh", "-t", "-n"]
```

### Test Framework
- Google Test integration
- Coverage: lcov + genhtml

### Directory Structure
```
.
├── Containerfile
├── cmake/
│   ├── GoogleTest.cmake
│   ├── Coverage.cmake
│   └── Sanitizers.cmake
└── tests/
    ├── CMakeLists.txt
    └── unit/
        ├── test_math.cpp
        ├── test_json.cpp
        └── test_fts5.cpp
```

---

## 11. Success Criteria

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

### Engineering (Phase 7)
- [ ] Phase 7: Engineering tools (Containerfile, test framework)

### General Requirements
- [x] All extensions work with `-t` (pure Go)
- [x] All extensions work with `-t -n` (CGO)
- [x] C++ builds output to `.build/cmake/lib/`
- [x] LD_LIBRARY_PATH set correctly for CGO
- [x] CGO-DS builds with `-tags SVDB_ENABLE_CGO_DS`
- [x] All existing DS tests pass with CGO-DS enabled
- [x] No breaking changes to existing DS API
- [x] Benchmark improvements documented for each phase

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
