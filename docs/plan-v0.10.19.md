# Plan v0.10.19 - C++ Engineering Tools

## Summary

Add engineering infrastructure for C++ development: Containerfile with build environment, test framework, and coverage framework.

## Background

### v0.10.16-18 Summary
- v0.10.16: C++ math extension (libsvdb_ext_math)
- v0.10.17: C++ JSON extension (libsvdb_ext_json)
- v0.10.18: C++ FTS5 extension (libsvdb_ext_fts5)

### Need for Engineering Tools
- **Reproducibility**: Docker builds for consistent environments
- **Testing**: C++ unit testing framework
- **Coverage**: Code coverage for C++ code
- **Go 1.26**: Latest Go version

---

## 1. Containerfile

### 1.1 Build Environment Container (Single)

```dockerfile
# Containerfile
FROM golang:1.26-bookworm

# Install C++ build dependencies
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
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install Go tools
RUN go install github.com/securego/gosec/cmd/gosec@latest

# Set up workspace
WORKDIR /workspace

# Default build command (builds Go + C++)
CMD ["./build.sh", "-t", "-n"]
```

---

## 2. Test Framework

### 2.1 Google Test Integration

```cmake
# cmake/GoogleTest.cmake
include(GoogleTest)

include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()
```

### 2.2 C++ Test Structure

```
tests/
├── CMakeLists.txt
├── unit/
│   ├── test_math.cpp
│   ├── test_json.cpp
│   └── test_fts5.cpp
├── integration/
│   └── test_extension_load.cpp
└── benchmark/
    └── bench_math.cpp
```

### 2.3 Test Example

```cpp
// tests/unit/test_math.cpp
#include <gtest/gtest.h>
#include "math.h"

extern "C" {
    #include "math.h"
}

class MathTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MathTest, AbsInt) {
    EXPECT_EQ(svdb_abs_int(-5), 5);
    EXPECT_EQ(svdb_abs_int(0), 0);
    EXPECT_EQ(svdb_abs_int(5), 5);
}

TEST_F(MathTest, AbsDouble) {
    EXPECT_DOUBLE_EQ(svdb_abs_double(-3.14), 3.14);
    EXPECT_DOUBLE_EQ(svdb_abs_double(0.0), 0.0);
}

TEST_F(MathTest, Power) {
    EXPECT_DOUBLE_EQ(svdb_power(2.0, 3.0), 8.0);
    EXPECT_DOUBLE_EQ(svdb_power(4.0, 0.5), 2.0);
}

TEST_F(MathTest, Sqrt) {
    EXPECT_DOUBLE_EQ(svdb_sqrt(4.0), 2.0);
    EXPECT_DOUBLE_EQ(svdb_sqrt(2.0), sqrt(2.0));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
```

### 2.4 CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(sqlvibe_tests)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CTest)
include(GoogleTest)

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(test_math
    unit/test_math.cpp
)

target_link_libraries(test_math
    svdb_ext_math
    GTest::gtest_main
)

gtest_discover_tests(test_math)
```

---

## 3. Coverage Framework

### 3.1 Coverage CMake

```cmake
# cmake/Coverage.cmake

option(ENABLE_COVERAGE "Enable coverage reporting" OFF)

if(ENABLE_COVERAGE)
    find_program(LCOV_EXECUTABLE lcov)
    find_program(GENHTML_EXECUTABLE genhtml)
    
    if(NOT LCOV_EXECUTABLE)
        message(WARNING "lcov not found, coverage disabled")
        return()
    endif()
    
    set(CMAKE_CXX_FLAGS_COVERAGE
        "-g -O0 --coverage -fprofile-arcs -ftest-coverage"
        CACHE STRING "Flags used by C++ compiler during coverage builds"
    )
    
    set(CMAKE_EXE_LINKER_FLAGS_COVERAGE
        "--coverage -fprofile-arcs -ftest-coverage"
        CACHE STRING "Flags used by linker during coverage builds"
    )
    
    mark_as_advanced(CMAKE_CXX_FLAGS_COVERAGE CMAKE_EXE_LINKER_FLAGS_COVERAGE)
    
    add_custom_target(coverage
        COMMAND ${LCOV_EXECUTABLE} --capture --directory . 
            --output-file coverage.info
            --exclude '*/googletest/*'
            --exclude '*/test*'
        COMMAND ${GENHTML_EXECUTABLE} coverage.info 
            --output-directory coverage_html
            --title "SQLVIBE Coverage Report"
        COMMENT "Generating coverage report..."
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
endif()
```

### 3.2 Usage

```bash
# Build with coverage
cd .build/cmake
cmake -DENABLE_COVERAGE=ON ..
make -j4

# Run tests
ctest

# Generate coverage report
make coverage

# View report
open coverage_html/index.html
```

---

## 4. Directory Structure

```
.
├── Containerfile              # Build environment (Go 1.26 + C++ tools)
├── cmake/
│   ├── GoogleTest.cmake     # GTest integration
│   ├── Coverage.cmake       # Coverage setup
│   └── Sanitizers.cmake    # ASAN/TSAN/UBSAN
├── tests/
│   ├── CMakeLists.txt
│   ├── unit/
│   │   ├── test_math.cpp
│   │   ├── test_json.cpp
│   │   └── test_fts5.cpp
│   ├── integration/
│   │   └── test_extension_load.cpp
│   └── benchmark/
│       └── bench_math.cpp
└── build.sh                 # Updated with C++ test support
```

---

## 5. build.sh Update

### Add C++ test support

```bash
# Add to build.sh
RUN_CPP_TESTS=0

# New flag
-n)           RUN_CPP_TESTS=1 ;;
# ... existing flags

# C++ test execution
if [[ $RUN_CPP_TESTS -eq 1 ]]; then
    echo "====> Building C++ tests..."
    
    # Build C++ with CMake
    mkdir -p "$BUILD_DIR/cmake"
    cd "$BUILD_DIR/cmake"
    cmake "$SCRIPT_DIR" -DENABLE_TESTS=ON
    cmake --build . -- -j4
    
    # Run C++ tests
    ctest --output-on-failure
    
    # Coverage if requested
    if [[ $COVERAGE -eq 1 ]]; then
        cmake "$SCRIPT_DIR" -DENABLE_COVERAGE=ON
        cmake --build .
        make coverage
    fi
fi
```

---

## 6. Sanitizers

### 6.1 Sanitizer CMake

```cmake
# cmake/Sanitizers.cmake
option(ENABLE_SANITIZERS "Enable sanitizers" OFF)

if(ENABLE_SANITIZERS)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()
```

### Usage

```bash
cmake -DENABLE_SANITIZERS=ON ..
make
ctest
```

---

## 7. Success Criteria

- [ ] Containerfile with Go 1.26 + C++ build tools + coverage tools
- [ ] Google Test integration
- [ ] C++ unit tests for math/json/fts5
- [ ] Coverage framework (lcov + genhtml)
- [ ] build.sh updated with -n flag for CGO
- [ ] Sanitizer support (ASAN)
- [ ] Tests can run with ./build.sh -t -n

---

## 8. Notes

- Single Containerfile for simplicity
- All builds stay in .build/ directory
- Mirrors Go testing patterns for C++
