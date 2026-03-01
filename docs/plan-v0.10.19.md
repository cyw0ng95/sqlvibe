# Plan v0.10.19 - C++ Engineering Tools

## Summary

Add engineering infrastructure for C++ development: Containerfile, test framework, and coverage framework.

## Background

### v0.10.16-18 Summary
- v0.10.16: C++ math extension (libsvdb_ext_math)
- v0.10.17: C++ JSON extension (libsvdb_ext_json)
- v0.10.18: C++ FTS5 extension (libsvdb_ext_fts5)

### Need for Engineering Tools
- **Reproducibility**: Docker builds for consistent environments
- **Testing**: C++ unit testing framework
- **Coverage**: Code coverage for C++ code
- **CI/CD**: Automated builds and tests

---

## 1. Containerfile

### 1.1 Development Container

```dockerfile
# Containerfile
FROM golang:1.22-bookworm

# Install C++ build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    ninja-build \
    clang-format \
    clang-tidy \
    cppcheck \
    lcov \
    ccache \
    && rm -rf /var/lib/apt/lists/*

# Install Go dependencies
RUN go install github.com/securego/gosec/cmd/gosec@latest

# Set up Go workspace
WORKDIR /workspace

# Copy project
COPY . .

# Default build command
CMD ["./build.sh", "-t", "-n"]
```

### 1.2 Build Container (Minimal)

```dockerfile
# Containerfile.build (minimal for CI)
FROM golang:1.22-bookworm-slim

RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .
CMD ["go", "test", "-tags", "SVDB_EXT_MATH,SVDB_ENABLE_CGO", "./..."]
```

### 1.3 Test Container

```dockerfile
# Containerfile.test
FROM golang:1.22-bookworm

RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    lcov \
    llvm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Coverage output volume
VOLUME /coverage

CMD ["./build.sh", "-t", "-c"]
```

---

## 2. Test Framework

### 2.1 Google Test Integration

```cmake
# cmake/GoogleTest.cmake
include(GoogleTest)

# Find or fetch GoogleTest
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
│   ├── test_extension_load.cpp
│   └── test_sql_queries.cpp
└── benchmark/
    ├── bench_math.cpp
    └── bench_json.cpp
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

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable testing
include(CTest)
include(GoogleTest)

# Find or fetch GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Add test executable
add_executable(test_math
    unit/test_math.cpp
)

target_link_libraries(test_math
    svdb_ext_math
    GTest::gtest_main
)

gtest_discover_tests(test_math)

# Add more tests...
```

---

## 3. Coverage Framework

### 3.1 Coverage CMake

```cmake
# cmake/Coverage.cmake

# Enable coverage
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)

if(ENABLE_COVERAGE)
    # Find lcov
    find_program(LCOV_EXECUTABLE lcov)
    find_program(GENHTML_EXECUTABLE genhtml)
    
    if(NOT LCOV_EXECUTABLE)
        message(WARNING "lcov not found, coverage disabled")
        return()
    endif()
    
    # Coverage flags
    set(CMAKE_CXX_FLAGS_COVERAGE
        "-g -O0 --coverage -fprofile-arcs -ftest-coverage"
        CACHE STRING "Flags used by C++ compiler during coverage builds"
    )
    
    set(CMAKE_EXE_LINKER_FLAGS_COVERAGE
        "--coverage -fprofile-arcs -ftest-coverage"
        CACHE STRING "Flags used by linker during coverage builds"
    )
    
    mark_as_advanced(CMAKE_CXX_FLAGS_COVERAGE CMAKE_EXE_LINKER_FLAGS_COVERAGE)
    
    # Coverage report target
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

### 3.3 Coverage Dashboard

```yaml
# .github/workflows/coverage.yml
name: Coverage

on:
  push:
    branches: [main, develop]
  pull_request:

jobs:
  coverage:
    runs-on: ubuntu-latest
    container:
      image: golang:1.22-bookworm
    steps:
      - uses: actions/checkout@v4
      
      - name: Install dependencies
        run: |
          apt-get update
          apt-get install -y cmake g++ lcov llvm
      
      - name: Build with coverage
        run: |
          mkdir -p build
          cd build
          cmake -DENABLE_COVERAGE=ON ..
          make -j4
      
      - name: Run tests
        run: ctest --output-on-failure
      
      - name: Generate coverage
        run: |
          cd build
          make coverage
      
      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./build/coverage.info
          directory: ./build/coverage_html
```

---

## 4. CI/CD Integration

### 4.1 GitHub Actions

```yaml
# .github/workflows/ci-cpp.yml
name: C++ CI

on:
  push:
    branches: [develop]
    paths:
      - 'ext/**/*.cpp'
      - 'ext/**/*.h'
      - 'ext/**/CMakeLists.txt'
  pull_request:

jobs:
  build-cpp:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Build C++ extensions
        run: |
          mkdir -p build
          cd build
          cmake ..
          make -j4
      
      - name: Run C++ tests
        run: |
          cd build
          ctest --output-on-failure
      
      - name: Coverage
        if: github.event_name == 'pull_request'
        run: |
          cmake -DENABLE_COVERAGE=ON ..
          make
          make coverage
      
      - name: Upload coverage
        if: github.event_name == 'pull_request'
        uses: codecov/codecov-action@v3
```

---

## 5. Directory Structure

```
.
├── Containerfile              # Development container
├── Containerfile.build        # Minimal build container
├── Containerfile.test        # Test container
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
├── .github/
│   └── workflows/
│       ├── ci-cpp.yml
│       └── coverage.yml
└── build.sh                 # Updated with C++ test support
```

---

## 6. build.sh Update

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

## 7. Sanitizers

### 7.1 Sanitizer CMake

```cmake
# cmake/Sanitizers.cmake
option(ENABLE_SANITIZERS "Enable sanitizers" OFF)

if(ENABLE_SANITIZERS)
    # Address Sanitizer
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
    
    # Optional: Undefined Behavior
    # add_compile_options(-fsanitize=undefined)
    # add_link_options(-fsanitize=undefined)
    
    # Optional: Memory
    # add_compile_options(-fsanitize=memory -fno-omit-frame-pointer)
    # add_link_options(-fsanitize=memory)
endif()
```

### Usage

```bash
cmake -DENABLE_SANITIZERS=ON ..
make
ctest
```

---

## 8. Success Criteria

- [ ] Containerfile for development
- [ ] Containerfile.build for CI
- [ ] Containerfile.test for testing
- [ ] Google Test integration
- [ ] C++ unit tests for math/json/fts5
- [ ] Coverage framework (lcov + genhtml)
- [ ] CI/CD workflow for C++
- [ ] Sanitizer support (ASAN/UBSAN)
- [ ] build.sh updated with -n flag

---

## 9. Notes

- Mirrors Go testing patterns but for C++
- Coverage output in .build/coverage/
- Container builds stay in .build/docker/
