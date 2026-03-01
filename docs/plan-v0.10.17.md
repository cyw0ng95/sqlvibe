# Plan v0.10.17 - C++ JSON Extension (Hybrid Go+C++)

## Summary

Refactor ext/json to use C++ for JSON parsing and operations, following the same hybrid Go+C++ pattern as v0.10.16.

## Background

### Current State
- ext/json: ~2000+ lines of Go code
- Uses standard Go JSON parsing
- 20+ JSON functions implemented

### Why C++?
- **Performance**: Faster JSON parsing/serialization
- **Consistency**: Same architecture as math extension
- **SIMD**: Can use simdjson library for speedups

### Dependencies
- **simdjson**: High-performance JSON parser (C++)

---

## 1. Architecture

### Hybrid Structure

```
ext/json/
├── json.go           # Go: Extension registration (always)
├── json_cgo.go     # [+build SVDB_ENABLE_CGO] CGO bridge
├── json_pure.go    # [-build SVDB_ENABLE_CGO] Pure Go implementation
├── json.h           # C++: Header declarations
├── json.cpp         # C++: Implementation (simdjson)
└── CMakeLists.txt   # C++ build config
```

### Library Name
- `libsvdb_ext_json.so`

---

## 2. C++ Implementation

### json.h

```cpp
#ifndef SVDB_JSON_H
#define SVDB_JSON_H

#include <cstdint>
#include <string>
#include <vector>

extern "C" {

// JSON validation
int svdb_json_valid(const char* json, int64_t len);

// JSON extraction
const char* svdb_json_extract(const char* json, const char* path);
const char* svdb_json_type(const char* json);

// JSON array/object
const char* svdb_json_array(const char** values, int64_t count);
const char* svdb_json_object(const char** keys, const char** values, int64_t count);

// JSON modification
const char* svdb_json_set(const char* json, const char* path, const char* value);
const char* svdb_json_insert(const char* json, const char* path, const char* value);
const char* svdb_json_remove(const char* json, const char* path);

// JSON table functions
void* svdb_json_each_create(const char* json, const char* path);
int svdb_json_each_next(void* ctx, char** key, char** value, int* type);
void svdb_json_each_destroy(void* ctx);

// simdjson-based parsing (fast path)
int svdb_json_parse_simdjson(const char* json, int64_t len, void** out_parsed);

} // extern "C"

#endif
```

### json.cpp (using simdjson)

```cpp
#include "json.h"
#include "simdjson.h"

using namespace simdjson;

int svdb_json_valid(const char* json, int64_t len) {
    ondemand::parser parser;
    try {
        parser.parse(json, len);
        return 1; // valid
    } catch (...) {
        return 0; // invalid
    }
}

const char* svdb_json_extract(const char* json, const char* path) {
    ondemand::parser parser;
    try {
        auto doc = parser.parse(json);
        // Navigate path and extract value
        // Return as string
    } catch (...) {
        return nullptr;
    }
}
```

---

## 3. Build System

### 3.1 build.sh Update

Same pattern as v0.10.16:

```bash
# -n flag already enables SVDB_ENABLE_CGO for all CGO modules
./build.sh -t -n
```

### 3.2 Root CMakeLists.txt

Update to include JSON:

```cmake
# Add to existing CMakeLists.txt
add_subdirectory(ext/json)
```

### 3.3 ext/json/CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(svdb_ext_json)

set(CMAKE_CXX_STANDARD 17)

# Fetch simdjson if not found
find_package(simdjson QUIET)
if(NOT simdjson_FOUND)
    # Use bundled simdjson or fetch
    include(FetchContent)
    FetchContent_Declare(simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG v3.6.0
    )
    FetchContent_MakeAvailable(simdjson)
endif()

add_library(svdb_ext_json SHARED
    json.cpp
)

target_link_libraries(svdb_ext_json PUBLIC
    simdjson
)

target_include_directories(svdb_ext_json PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Install to .build/cmake/lib
install(TARGETS svdb_ext_json
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
)
```

---

## 4. Go Integration

### json_cgo.go

```go
// +build SVDB_ENABLE_CGO

package json

/*
#cgo pkg-config: simdjson
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ext_json
#cgo CFLAGS: -I${SRCDIR}/include
#include "json.h"
#include <stdlib.h>
*/
import "C"

func evalJSONCGO(args []interface{}) interface{} {
    jsonStr := toString(args[0])
    if jsonStr == "" {
        return nil
    }
    
    cStr := C.CString(jsonStr)
    defer C.free(unsafe.Pointer(cStr))
    
    result := C.svdb_json_valid(cStr, C.int64_t(len(jsonStr)))
    return result == 1
}

func evalJSONExtractCGO(json, path string) string {
    cJson := C.CString(json)
    cPath := C.CString(path)
    defer C.free(unsafe.Pointer(cJson))
    defer C.free(unsafe.Pointer(cPath))
    
    result := C.svdb_json_extract(cJson, cPath)
    if result == nil {
        return ""
    }
    defer C.free(unsafe.Pointer(result))
    
    return C.GoString(result)
}
```

---

## 5. Performance Comparison

### Expected Performance

| Function | Go (ns) | C++ simdjson (ns) | Speedup |
|----------|---------|-------------------|---------|
| JSON_VALID | 500 | 50 | 10x |
| JSON_EXTRACT | 1000 | 100 | 10x |
| JSON_ARRAY | 300 | 50 | 6x |
| JSON_OBJECT | 400 | 60 | 6x |

### simdjson Benefits
- Parses gigabytes of JSON per second
- Uses SIMD instructions
- Zero-copy parsing on modern CPUs

---

## 6. Dependencies

### External Libraries

| Library | Purpose | License |
|---------|---------|---------|
| simdjson | JSON parsing | Apache 2.0 |

### Fetch Strategy
- Use CMake FetchContent to download
- Cache in .build/ directory
- Fall back to system library if available

---

## 7. Implementation Order

1. Create C++ header (json.h)
2. Add simdjson dependency to CMakeLists.txt
3. Implement C++ JSON functions (json.cpp)
4. Create json_cgo.go with build tags
5. Ensure json_pure.go works without CGO
6. Build and test both variants
7. Benchmark comparison

---

## 8. Success Criteria

- [ ] C++ JSON library builds with simdjson
- [ ] All 20+ JSON functions work via CGO
- [ ] Pure Go fallback works without CGO
- [ ] Performance improvement demonstrated (10x)
- [ ] Build system works with -n flag

---

## 8. Test Cases (Go with -t flag)

### 8.1 Test Files to Add

| Test File | Description | Test Cases |
|-----------|-------------|------------|
| ext/json/json_test.go | Already exists | ~30 tests |
| ext/json/json_cgo_test.go | [+build SVDB_ENABLE_CGO] CGO-specific | ~15 tests |
| ext/json/json_bench_test.go | Benchmark CGO vs pure Go | ~5 benchmarks |

### 8.2 CGO Test Example

```go
// ext/json/json_cgo_test.go
// +build SVDB_ENABLE_CGO

package json

import (
    "testing"
)

func TestJSONValidCGO(t *testing.T) {
    tests := []struct {
        input  string
        expect bool
    }{
        {`{"a":1}`, true},
        {`[1,2,3]`, true},
        {`invalid`, false},
        {``, false},
    }
    
    for _, tt := range tests {
        result := callJSONValidCGO(tt.input)
        if result != tt.expect {
            t.Errorf("JSONValid(%q) = %v, want %v", tt.input, result, tt.expect)
        }
    }
}

func TestJSONExtractCGO(t *testing.T) {
    tests := []struct {
        json  string
        path  string
        expect string
    }{
        {`{"a":{"b":1}}`, `$.a.b`, `1`},
        {`[1,2,3]`, `$[0]`, `1`},
        {`{"a":1}`, `$.missing`, ``},
    }
    
    for _, tt := range tests {
        result := callJSONExtractCGO(tt.json, tt.path)
        if result != tt.expect {
            t.Errorf("JSONExtract(%q, %q) = %q, want %q", tt.json, tt.path, result, tt.expect)
        }
    }
}

func BenchmarkJSONValidCGO(b *testing.B) {
    for i := 0; i < b.N; i++ {
        callJSONValidCGO(`{"test":123}`)
    }
}
```

### 8.3 Run Tests

```bash
# Test pure Go (default)
./build.sh -t

# Test CGO version
./build.sh -t -n

# Compare results
```

---

## 9. Success Criteria

- [ ] C++ JSON library builds with simdjson
- [ ] All 20+ JSON functions work via CGO
- [ ] Pure Go fallback works without CGO
- [ ] Performance improvement demonstrated (10x)
- [ ] Build system works with -n flag
- [ ] Go tests pass with -t flag
- [ ] Go tests pass with -t -n flag (CGO)
