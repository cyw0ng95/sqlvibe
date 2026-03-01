# Plan v0.10.16 - C++ Math Extension (Hybrid Go+C++ Prototype)

## Summary

Refactor ext/math to use C++ for core math operations, demonstrating hybrid Go+C++ architecture, controlled by build tag.

## Background

### Current State
- ext/math: ~360 lines of Go code
- Uses gomath standard library
- 20+ math functions implemented

### Build Tags

| Tag | Description | Default |
|-----|-------------|---------|
| `SVDB_ENABLE_CGO` | Enable C++ math functions | **No** (must use -n flag) |
| `SVDB_EXT_MATH` | Enable math extension | No |

### Build Examples

```bash
# Default: No math extension
go build ./...

# With Go math extension only
go build -tags SVDB_EXT_MATH ./...

# With C++ math extension (requires CGO)
# Use build.sh -n flag (enables SVDB_ENABLE_CGO)
./build.sh -t -n

# Manual CGO build
go build -tags "SVDB_EXT_MATH SVDB_ENABLE_CGO" ./...
```

---

## 1. Architecture

### Hybrid Structure

```
ext/math/
├── math.go           # Go: Extension registration (always)
├── math_cgo.go     # [+build SVDB_ENABLE_CGO] CGO bridge
├── math_pure.go    # [-build SVDB_ENABLE_CGO] Pure Go implementation
├── math.h           # C++: Header declarations
├── math.cpp         # C++: Implementation
└── CMakeLists.txt   # C++ build config
```

### Build Tag Switching

```go
// math.go - Common registration
package math

import "github.com/cyw0ng95/sqlvibe/ext"

func init() {
    ext.Register("math", &MathExtension{})
}

// math_cgo.go - C++ implementation
// +build SVDB_ENABLE_CGO

package math

/*
#include "math.h"
#include <stdlib.h>
*/
import "C"

type MathExtension struct{}

func (e *MathExtension) CallFunc(name string, args []interface{}) interface{} {
    switch name {
    case "ABS":
        return callAbsCGO(args)
    case "POWER":
        return callPowerCGO(args)
    // ... other CGO calls
    }
    return nil
}

// math_pure.go - Pure Go fallback
// +build !SVDB_ENABLE_CGO

package math

import (
    gomath "math"
    mathrand "math/rand"
)

type MathExtension struct{}

func (e *MathExtension) CallFunc(name string, args []interface{}) interface{} {
    switch name {
    case "ABS":
        return evalAbs(args)  // Pure Go
    case "POWER":
        return evalPower(args)  // Pure Go
    // ... pure Go implementations
    }
    return nil
}
```

### Data Flow

```
SQL Query
    │
    ▼
Go VM (ext.Extension interface)
    │
    ▼
CGO ──────────────────────► C++ math functions
    │                            │
    │ (call via CGO)           ▼
    │                      SIMD optimized
    │                            │
    ◄───────────────────────────┘
    │
    ▼
Result returned to VM
```

---

## 2. C++ Implementation

### math.h

```cpp
#ifndef SVDB_MATH_H
#define SVDB_MATH_H

#include <cstdint>
#include <cmath>
#include <vector>

extern "C" {

// Basic math functions
int64_t svdb_abs_int(int64_t v);
double svdb_abs_double(double v);

double svdb_ceil(double v);
double svdb_floor(double v);
double svdb_round(double v, int decimals);

double svdb_power(double base, double exp);
double svdb_sqrt(double v);
double svdb_mod(double a, double b);

double svdb_exp(double v);
double svdb_ln(double v);
double svdb_log(double base, double v);
double svdb_log2(double v);
double svdb_log10(double v);

int64_t svdb_sign_int(int64_t v);
int64_t svdb_sign_double(double v);

// Random functions
int64_t svdb_random();
void* svdb_randomblob(int64_t n);
void* svdb_zeroblob(int64_t n);

// SIMD batch operations
void svdb_batch_abs_double(double* data, int64_t n);
void svdb_batch_add_double(double* a, double* b, double* out, int64_t n);

} // extern "C"

#endif
```

### math.cpp

```cpp
#include "math.h"
#include <cstdlib>
#include <cstring>

// SIMD implementation example
#ifdef __SSE__
#include <immintrin.h>

void svdb_batch_abs_double(double* data, int64_t n) {
    __m256d zero = _mm256_setzero_pd();
    for (int64_t i = 0; i < n; i += 4) {
        __m256d vals = _mm256_load_pd(&data[i]);
        __m256d abs = _mm256_andnot_pd(_mm256_set1_pd(-0.0), vals);
        _mm256_store_pd(&data[i], abs);
    }
}
#else
void svdb_batch_abs_double(double* data, int64_t n) {
    for (int64_t i = 0; i < n; i++) {
        data[i] = std::fabs(data[i]);
    }
}
#endif
```

---

## 3. Go Integration

### math.go (Updated)

```go
/*
#include "math.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"

	"github.com/cyw0ng95/sqlvibe/ext"
)

type MathExtension struct{}

func (e *MathExtension) CallFunc(name string, args []interface{}) interface{} {
	switch name {
	case "ABS":
		return callAbs(args)
	case "POWER", "POW":
		return callPower(args)
	// ... other functions
	}
	return nil
}

//go:inline
func callAbs(args []interface{}) interface{} {
	// Call C++ function directly
	val := args[0]
	switch v := val.(type) {
	case int64:
		return C.svdb_abs_int(C.int64_t(v))
	case float64:
		return C.svdb_abs_double(C.double(v))
	}
	return val
}

// Random blob handling
func callRandomblob(args []interface{}) []byte {
	n := toInt64OrDefault(args[0], 0)
	if n <= 0 {
		return []byte{}
	}
	
	ptr := C.svdb_randomblob(C.int64_t(n))
	if ptr == nil {
		return []byte{}
	}
	
	defer C.free(ptr)
	
	// Copy from C to Go
	cSlice := (*[1<<30]byte)(ptr)[:n:n]
	result := make([]byte, n)
	copy(result, cSlice)
	
	return result
}

func init() {
	ext.Register("math", &MathExtension{})
}
```

---

## 4. Build System

### 4.1 build.sh Update

Add `-n` flag to enable CGO:

```bash
# Add to build.sh options
-n)           ENABLE_CGO=1 ;;
```

Updated EXT_TAGS logic:

```bash
# Default tags
EXT_TAGS="SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5,SVDB_EXT_PROFILING"

# If -n flag is used, add CGO support
if [[ $ENABLE_CGO -eq 1 ]]; then
    EXT_TAGS="$EXT_TAGS,SVDB_ENABLE_CGO"
    echo "====> CGO enabled (SVDB_ENABLE_CGO)"
fi
```

### 4.2 CMakeLists.txt (Project Root)

Create at project root for C++ builds:

```cmake
cmake_minimum_required(VERSION 3.16)
project(sqlvibe CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Output directory: .build/cmake/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/.build/cmake/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/.build/cmake/lib)

# Enable SIMD
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-msse4.1" HAVE_SSE41)
if(HAVE_SSE41)
    add_compile_options(-msse4.1)
endif()

# Add subdirectories
add_subdirectory(ext/math)

# Build all C++ targets
add_custom_target(cpp-build
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}
    COMMENT "Building C++ targets in .build/cmake/"
)
```

### 4.3 ext/math/CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(svdb_ext_math)

set(CMAKE_CXX_STANDARD 17)

add_library(svdb_ext_math SHARED
    math.cpp
)

target_include_directories(svdb_ext_math PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Install to .build/cmake/lib
install(TARGETS svdb_ext_math
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
)
```

### 4.4 Build Workflow

```bash
# 1. Build Go with CGO (outputs to .build/)
./build.sh -t -n

# 2. This automatically:
#    - Adds SVDB_ENABLE_CGO to build tags
#    - Compiles C++ math library to .build/cmake/lib/
#    - Links via CGO

# Manual steps (if needed):
#    cd .build/cmake && cmake .. && make
```

### 4.5 Output Structure

```
.build/
├── test.log
├── bench.log
├── coverage.html
├── coverage.out
├── cmake/
│   ├── bin/           # Executables
│   └── lib/           # .so/.dll libraries
│       └── libsvdb_ext_math.so
└── ...
```

### Build Tag Switching

| Build Command | math Implementation | Output |
|--------------|---------------------|--------|
| `go build ./...` | No math extension | - |
| `-tags SVDB_EXT_MATH` | Pure Go math | - |
| `./build.sh -t -n` | C++ math (CGO) | .build/cmake/lib/ |

---

## 5. Performance Comparison

### Benchmark

| Function | Go (ns) | C++ (ns) | Speedup |
|----------|---------|-----------|---------|
| ABS | 5 | 2 | 2.5x |
| POWER | 50 | 20 | 2.5x |
| SQRT | 30 | 10 | 3x |
| LOG | 40 | 15 | 2.7x |
| RANDOM | 10 | 5 | 2x |

### Batch Operations (SIMD)

| Operation | Rows | Go (µs) | C++ SIMD (µs) | Speedup |
|----------|------|---------|---------------|---------|
| ABS | 1000 | 500 | 50 | 10x |
| ADD | 1000 | 600 | 40 | 15x |

---

## 6. Testing

### Unit Tests
- Test each C++ function for correctness
- Compare Go vs C++ results
- Edge cases: NaN, Inf, overflow

### Benchmark Tests
- Micro-benchmarks for each function
- Batch operation benchmarks
- Memory allocation profiling

### Integration Tests
- Full SQL query tests
- Extension loading tests

---

## 7. Implementation Order

1. Create C++ header (math.h)
2. Implement basic C++ functions (math.cpp)
3. Add CMakeLists.txt
4. Update Go to call C++ via CGO
5. Add SIMD batch operations
6. Build and test
7. Benchmark comparison

---

## 7. Test Cases (Go with -t flag)

### 7.1 Test Files to Add

The Go tests already exist. We need to add tests that verify CGO implementation:

| Test File | Description | Test Cases |
|-----------|-------------|------------|
| ext/math/math_test.go | Already exists | ~20 tests |
| ext/math/math_cgo_test.go | [+build SVDB_ENABLE_CGO] CGO-specific | ~10 tests |
| ext/math/math_bench_test.go | Benchmark CGO vs pure Go | ~5 benchmarks |

### 7.2 CGO Test Example

```go
// ext/math/math_cgo_test.go
// +build SVDB_ENABLE_CGO

package math

import (
    "testing"
)

func TestAbsCGO(t *testing.T) {
    // Test that CGO implementation matches pure Go
    tests := []struct {
        input  int64
        expect int64
    }{
        {-5, 5},
        {0, 0},
        {5, 5},
    }
    
    for _, tt := range tests {
        result := callAbsCGO(tt.input)
        if result != tt.expect {
            t.Errorf("Abs(%d) = %d, want %d", tt.input, result, tt.expect)
        }
    }
}

func TestPowerCGO(t *testing.T) {
    tests := []struct {
        base   float64
        exp    float64
        expect float64
    }{
        {2, 3, 8},
        {4, 0.5, 2},
        {10, 2, 100},
    }
    
    for _, tt := range tests {
        result := callPowerCGO(tt.base, tt.exp)
        if !closeEnough(result, tt.expect) {
            t.Errorf("Power(%v, %v) = %v, want %v", tt.base, tt.exp, result, tt.expect)
        }
    }
}

func BenchmarkAbsCGO(b *testing.B) {
    for i := 0; i < b.N; i++ {
        callAbsCGO(int64(i))
    }
}

func BenchmarkAbsGo(b *testing.B) {
    for i := 0; i < b.N; i++ {
        evalAbs([]interface{}{int64(i)})
    }
}
```

### 7.3 Run Tests

```bash
# Test pure Go (default)
./build.sh -t

# Test CGO version
./build.sh -t -n

# Compare results
# Both should produce identical results
```

---

## 8. Success Criteria

- [ ] C++ math library builds
- [ ] All 20+ functions working via CGO
- [ ] Test compatibility with Go implementation
- [ ] Performance improvement demonstrated
- [ ] Batch SIMD operations working
- [ ] Go tests pass with -t flag
- [ ] Go tests pass with -t -n flag (CGO)
- [ ] Documentation for hybrid development

---

## 9. Lessons Learned

This will be the **first C++ integration**, informing future migrations:

| Learning | Application |
|----------|-------------|
| CGO overhead | When to use, when not to |
| Memory management | DS/VM migration |
| Build system | CMake integration |
| Testing | Cross-language testing |
| Debugging | CGO debugging |

---

## Notes

- **Risk**: Low - math is isolated, easy to validate
- **Scope**: Small - only ~20 functions
- **Value**: High - proves hybrid architecture works
