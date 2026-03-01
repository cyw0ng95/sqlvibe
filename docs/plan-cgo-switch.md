# Plan CGO Switch - Hybrid Go+C++ Architecture

## Summary

Implement hybrid Go+C++ architecture by converting extensions to use C++ for performance-critical operations, controlled by build tags.

## Overview

This plan covers converting key extensions to use C++ while maintaining pure Go fallbacks:

| Phase | Module | Library | Status |
|-------|--------|---------|--------|
| Phase 1 | ext/math | libsvdb_ext_math | ✅ Implemented |
| Phase 2 | ext/json | libsvdb_ext_json | Pending |
| Phase 3 | ext/fts5 | libsvdb_ext_fts5 | Pending |
| Phase 4 | Engineering Tools | Containerfile | Pending |

---

## 1. Build System

### 1.1 Build Tags

| Tag | Description | Default |
|-----|-------------|---------|
| `SVDB_ENABLE_CGO` | Enable C++ implementations | **No** (use `-n` flag) |
| `SVDB_EXT_MATH` | Enable math extension | No |
| `SVDB_EXT_JSON` | Enable JSON extension | No |
| `SVDB_EXT_FTS5` | Enable FTS5 extension | No |

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

## 7. Phase 4 - Engineering Tools (Pending)

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

## 8. Success Criteria

- [x] Phase 1: ext/math CGO implementation
- [ ] Phase 2: ext/json CGO implementation
- [ ] Phase 3: ext/fts5 CGO implementation
- [ ] Phase 4: Engineering tools
- [ ] All extensions work with `-t` (pure Go)
- [ ] All extensions work with `-t -n` (CGO)
- [ ] C++ builds output to `.build/cmake/lib/`
- [ ] LD_LIBRARY_PATH set correctly for CGO

---

## 9. Notes

- Build outputs stay in `.build/` directory
- Pure Go is default, CGO is opt-in via `-n` flag
- Each extension can be independently enabled/disabled
- CGO implementations should produce identical results to pure Go
