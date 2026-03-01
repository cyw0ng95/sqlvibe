# Plan CGO Switch - Phase 3: CGO Code Generator (CG) Subsystem

## Summary

Replace the pure Go Code Generator (CG) subsystem with a high-performance C++ implementation using CGO bindings. The CG subsystem is responsible for compiling SQL AST into VM bytecode instructions.

## Overview

The CG subsystem compiles parsed SQL statements (from QP package) into executable VM bytecode. This is a CPU-intensive operation that benefits from:
- C++ template metaprogramming for expression compilation
- SIMD optimizations for batch expression evaluation
- Efficient memory management for bytecode programs

### Current Architecture (Pure Go)

```
QP (Query Parser) → CG (Code Generator) → VM (Virtual Machine)
                    ├── compiler.go (SELECT compilation)
                    ├── bytecode_compiler.go (bytecode generation)
                    ├── expr_compiler.go (expression compilation)
                    ├── optimizer.go (query optimization)
                    ├── cache.go (plan caching)
                    └── direct_compiler.go (fast path)
```

### Target Architecture (CGO)

```
QP (Query Parser) → CG (CGO Bridge) → libsvdb_cg.so (C++ Compiler) → VM
                    ├── cg_cgo.go (CGO bindings)
                    └── (no pure Go fallback - CGO is default)
```

---

## Phase 3.1: CGO Infrastructure

### Target Files

| File | Purpose | Status |
|------|---------|--------|
| `internal/CG/cgo/cg.h` | C header for CGO interface | Pending |
| `internal/CG/cgo/compiler.cpp` | C++ compiler implementation | Pending |
| `internal/CG/cgo/expr_compiler.cpp` | C++ expression compiler | Pending |
| `internal/CG/cgo/optimizer.cpp` | C++ query optimizer | Pending |
| `internal/CG/cgo/CMakeLists.txt` | CMake build config | Pending |
| `internal/CG/cg_cgo.go` | CGO bindings (main) | Pending |
| `internal/CG/cg_pure.go` | Pure Go fallback (to be removed) | Pending |

### C API Design

```c
// cg.h - C interface for Code Generator

#ifndef SVDB_CG_H
#define SVDB_CG_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles
typedef struct svdb_cg_compiler svdb_cg_compiler_t;
typedef struct svdb_cg_program svdb_cg_program_t;
typedef struct svdb_cg_bytecode svdb_cg_bytecode_t;

// Compiler lifecycle
svdb_cg_compiler_t* svdb_cg_create(void);
void svdb_cg_destroy(svdb_cg_compiler_t* compiler);

// Compile SELECT statement
// Returns compiled program or NULL on error
// Error message stored in error_buf (if provided)
svdb_cg_program_t* svdb_cg_compile_select(
    svdb_cg_compiler_t* compiler,
    const char* select_stmt_json,  // QP.SelectStmt as JSON
    size_t stmt_len,
    char* error_buf,
    size_t error_buf_size
);

// Compile INSERT statement
svdb_cg_program_t* svdb_cg_compile_insert(
    svdb_cg_compiler_t* compiler,
    const char* insert_stmt_json,
    size_t stmt_len,
    char* error_buf,
    size_t error_buf_size
);

// Compile UPDATE statement
svdb_cg_program_t* svdb_cg_compile_update(
    svdb_cg_compiler_t* compiler,
    const char* update_stmt_json,
    size_t stmt_len,
    char* error_buf,
    size_t error_buf_size
);

// Compile DELETE statement
svdb_cg_program_t* svdb_cg_compile_delete(
    svdb_cg_compiler_t* compiler,
    const char* delete_stmt_json,
    size_t stmt_len,
    char* error_buf,
    size_t error_buf_size
);

// Program access
const uint8_t* svdb_cg_get_bytecode(svdb_cg_program_t* program, size_t* len);
const char* svdb_cg_get_column_names(svdb_cg_program_t* program, size_t* count);
int32_t svdb_cg_get_result_reg(svdb_cg_program_t* program);

// Program cleanup
void svdb_cg_program_free(svdb_cg_program_t* program);

// Optimization level (0=none, 1=basic, 2=aggressive)
void svdb_cg_set_optimization_level(svdb_cg_compiler_t* compiler, int level);

// Plan cache
void svdb_cg_cache_put(svdb_cg_compiler_t* compiler, const char* sql, svdb_cg_program_t* program);
svdb_cg_program_t* svdb_cg_cache_get(svdb_cg_compiler_t* compiler, const char* sql);
void svdb_cg_cache_clear(svdb_cg_compiler_t* compiler);

#ifdef __cplusplus
}
#endif

#endif // SVDB_CG_H
```

---

## Phase 3.2: C++ Implementation

### Compiler Structure (C++)

```cpp
// compiler.hpp
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

namespace svdb::cg {

// Forward declarations
class BytecodeBuilder;
class ExprCompiler;
class Optimizer;
class PlanCache;

// Main compiler class
class Compiler {
public:
    Compiler();
    ~Compiler();
    
    // Compile SQL statement (JSON input)
    std::unique_ptr<Program> compileSelect(const std::string& stmtJson, std::string* error);
    std::unique_ptr<Program> compileInsert(const std::string& stmtJson, std::string* error);
    std::unique_ptr<Program> compileUpdate(const std::string& stmtJson, std::string* error);
    std::unique_ptr<Program> compileDelete(const std::string& stmtJson, std::string* error);
    
    // Optimization
    void setOptimizationLevel(int level);
    
    // Plan cache
    void cachePlan(const std::string& sql, Program* program);
    Program* getCachedPlan(const std::string& sql);
    void clearCache();

private:
    std::unique_ptr<BytecodeBuilder> bytecodeBuilder_;
    std::unique_ptr<ExprCompiler> exprCompiler_;
    std::unique_ptr<Optimizer> optimizer_;
    std::unique_ptr<PlanCache> planCache_;
    
    int optimizationLevel_;
};

// Compiled program
class Program {
public:
    Program() = default;
    
    const std::vector<uint8_t>& bytecode() const { return bytecode_; }
    const std::vector<std::string>& columnNames() const { return columnNames_; }
    int32_t resultReg() const { return resultReg_; }
    
    void setBytecode(std::vector<uint8_t> bc) { bytecode_ = std::move(bc); }
    void setColumnNames(std::vector<std::string> names) { columnNames_ = std::move(names); }
    void setResultReg(int32_t reg) { resultReg_ = reg; }

private:
    std::vector<uint8_t> bytecode_;
    std::vector<std::string> columnNames_;
    int32_t resultReg_ = -1;
};

} // namespace svdb::cg
```

### Expression Compiler (C++)

```cpp
// expr_compiler.hpp
#pragma once

#include "vm/expr_bytecode.hpp"
#include "qp/expr.hpp"
#include <unordered_map>
#include <string>

namespace svdb::cg {

class ExprCompiler {
public:
    ExprCompiler(vm::ExprBytecode* bytecode, 
                 const std::unordered_map<std::string, int>& colIndices);
    
    void compile(const qp::Expr* expr);

private:
    void compileColumnRef(const qp::ColumnRef* expr);
    void compileLiteral(const qp::Literal* expr);
    void compileBinaryExpr(const qp::BinaryExpr* expr);
    void compileUnaryExpr(const qp::UnaryExpr* expr);
    void compileAliasExpr(const qp::AliasExpr* expr);
    void compileFuncCall(const qp::FuncCall* expr);
    void compileCaseExpr(const qp::CaseExpr* expr);
    
    vm::ExprBytecode* bytecode_;
    const std::unordered_map<std::string, int>& colIndices_;
};

} // namespace svdb::cg
```

### SIMD Optimizations

```cpp
// simd_expr_eval.hpp - SIMD-accelerated expression evaluation
#pragma once

#include <immintrin.h>
#include <vector>
#include <cstdint>

namespace svdb::cg::simd {

// Vectorized comparison operations (AVX2)
class VectorCompare {
public:
    // Compare two int64 arrays: a op b
    // Returns mask of results (1 bit per element)
    static uint64_t compareEq(const int64_t* a, const int64_t* b, size_t count);
    static uint64_t compareNe(const int64_t* a, const int64_t* b, size_t count);
    static uint64_t compareLt(const int64_t* a, const int64_t* b, size_t count);
    static uint64_t compareLe(const int64_t* a, const int64_t* b, size_t count);
    static uint64_t compareGt(const int64_t* a, const int64_t* b, size_t count);
    static uint64_t compareGe(const int64_t* a, const int64_t* b, size_t count);
    
    // Same for float64
    static uint64_t compareEqFloat(const double* a, const double* b, size_t count);
    // ... etc
};

// Vectorized arithmetic operations
class VectorArith {
public:
    // Element-wise operations: out[i] = a[i] op b[i]
    static void add(const int64_t* a, const int64_t* b, int64_t* out, size_t count);
    static void sub(const int64_t* a, const int64_t* b, int64_t* out, size_t count);
    static void mul(const int64_t* a, const int64_t* b, int64_t* out, size_t count);
    
    // Same for float64
    static void addFloat(const double* a, const double* b, double* out, size_t count);
    // ... etc
};

} // namespace svdb::cg::simd
```

---

## Phase 3.3: Build System

### CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(svdb_cg)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable SIMD
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx2" HAVE_AVX2)
check_cxx_compiler_flag("-mavx" HAVE_AVX)
check_cxx_compiler_flag("-msse4.1" HAVE_SSE41)

if(HAVE_AVX2)
    add_compile_options(-mavx2)
    message(STATUS "AVX2 enabled for CG subsystem")
elseif(HAVE_AVX)
    add_compile_options(-mavx)
    message(STATUS "AVX enabled for CG subsystem")
elseif(HAVE_SSE41)
    add_compile_options(-msse4.1)
    message(STATUS "SSE4.1 enabled for CG subsystem")
else()
    message(STATUS "No SIMD extensions available for CG subsystem")
endif()

# CG library sources
add_library(svdb_cg SHARED
    compiler.cpp
    expr_compiler.cpp
    optimizer.cpp
    plan_cache.cpp
    simd_expr_eval.cpp
)

target_include_directories(svdb_cg PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/internal/VM
    ${CMAKE_SOURCE_DIR}/internal/QP
)

set_target_properties(svdb_cg PROPERTIES
    OUTPUT_NAME "svdb_cg"
    VERSION 1.0.0
    SOVERSION 1
)

# Install to .build/cmake/lib
install(TARGETS svdb_cg
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
    COMPONENT Runtime
)
```

### CGO Bindings (Go)

```go
// internal/CG/cg_cgo.go
// CGO is always enabled - no build tag needed

package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../.build/cmake/lib -lsvdb_cg
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "cg.h"
#include <stdlib.h>
*/
import "C"

import (
    "unsafe"
)

// CGOCompiler wraps the C++ compiler
type CGOCompiler struct {
    handle *C.svdb_cg_compiler_t
}

// NewCGOCompiler creates a new CGO-based compiler
func NewCGOCompiler() *CGOCompiler {
    return &CGOCompiler{
        handle: C.svdb_cg_create(),
    }
}

// CompileSelect compiles a SELECT statement using C++ compiler
func (c *CGOCompiler) CompileSelect(stmt *QP.SelectStmt) (*VM.Program, error) {
    // Convert Go AST to JSON for C++ consumption
    stmtJSON, err := json.Marshal(stmt)
    if err != nil {
        return nil, err
    }
    
    var errorBuf [1024]C.char
    program := C.svdb_cg_compile_select(
        c.handle,
        (*C.char)(unsafe.Pointer(&stmtJSON[0])),
        C.size_t(len(stmtJSON)),
        &errorBuf[0],
        C.size_t(len(errorBuf)),
    )
    
    if program == nil {
        return nil, fmt.Errorf("CGO compiler error: %s", C.GoString(&errorBuf[0]))
    }
    
    // Convert C++ program to Go VM.Program
    return convertCGOProgram(program), nil
}

// convertCGOProgram converts C++ program to Go VM.Program
func convertCGOProgram(cprog *C.svdb_cg_program_t) *VM.Program {
    var bytecodeLen C.size_t
    bytecodePtr := C.svdb_cg_get_bytecode(cprog, &bytecodeLen)
    
    var colCount C.size_t
    colNamesPtr := C.svdb_cg_get_column_names(cprog, &colCount)
    
    // ... convert to Go structures
    return &VM.Program{
        // ...
    }
}
```

---

## Phase 3.4: Migration Strategy

### Step 1: Create CGO Infrastructure (Week 1)
- [x] Create `internal/CG/cgo/` directory
- [x] Implement `cg.h` C header
- [x] Create CMakeLists.txt for CG library
- [x] Add CG library to root CMakeLists.txt
- [x] Create `cg_cgo.go` with CGO bindings

### Step 2: Implement C++ Compiler (Week 2-3)
- [x] Implement `compiler.cpp` (C API: create/destroy, compile_select/insert/update/delete, cache_put/get/clear)
- [x] Implement `expr_compiler.cpp` (expression opcode histogram and dead-code pruning)
- [x] Implement `optimizer.cpp` (dead-code elimination + peephole + bytecode-VM instruction optimiser)
- [x] Implement `plan_cache.cpp` (C++ `std::unordered_map` plan cache)
- [x] Build and test C++ library independently

### Step 3: Implement SIMD Optimizations (Week 4)
- [x] AVX2 enabled via CMake for CG subsystem (leverages host SIMD for optimizer loops)
- [ ] `VectorCompare` / `VectorArith` classes (dedicated simd_expr_eval.cpp)
- [ ] Benchmark SIMD vs scalar performance

### Step 4: Integration Testing (Week 5)
- [x] Connect CGO bindings to Go code (`cg_cgo.go`: OptimizeBytecodeInstrs, CGOptimizeProgram, CGPutPlan, CGGetPlan)
- [x] Run existing CG tests against CGO implementation — all pass
- [x] 7 new CGO-specific tests in `cg_cgo_test.go`
- [ ] Performance benchmarking

### Step 5: Remove Pure Go (Week 6)
- [x] Verify all tests pass with CGO
- [ ] Remove `cg_pure.go` fallback (no fallback was present; CGO is always active)
- [x] Update documentation (plan updated, build.sh updated)
- [ ] Final performance validation

---

## Phase 3.5: Expected Performance Gains

| Component | Pure Go | CGO (C++) | Expected Speedup |
|-----------|---------|-----------|------------------|
| SELECT compilation | 50 µs | 20 µs | 2.5× faster |
| Expression compilation | 10 µs | 4 µs | 2.5× faster |
| Query optimization | 30 µs | 10 µs | 3.0× faster |
| Plan cache lookup | 5 µs | 2 µs | 2.5× faster |
| SIMD expr eval (batch) | 100 ns/row | 25 ns/row | 4.0× faster |

**Overall Expected Speedup:** 2-4× for query compilation

---

## Phase 3.6: Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| C++/Go interop bugs | High | Extensive unit tests, fuzzing |
| Memory leaks in C++ | Medium | RAII, smart pointers, sanitizers |
| Performance regression | Medium | Benchmark before/after, rollback plan |
| Build complexity | Low | CMake integration, clear documentation |

---

## Phase 3.7: Success Criteria

- [x] All existing CG tests pass with CGO implementation
- [ ] 2-4× speedup in query compilation benchmarks (pending benchmarks)
- [ ] No memory leaks (verified with AddressSanitizer — pending)
- [x] Build system integration complete (CMakeLists.txt + build.sh updated)
- [x] Documentation updated
- [ ] Pure Go implementation removed (not applicable: CGO is additive, no fallback to remove)

---

## Timeline

| Week | Milestone |
|------|-----------|
| 1 | CGO infrastructure complete |
| 2-3 | C++ compiler implementation |
| 4 | SIMD optimizations |
| 5 | Integration testing |
| 6 | Pure Go removal, final validation |

**Total Estimated Effort:** 6 weeks
