cmake_minimum_required(VERSION 3.16)
project(sqlvibe CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Output directory: .build/cmake/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/.build/cmake/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/.build/cmake/lib)

# Enable SIMD
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx" HAVE_AVX)
check_cxx_compiler_flag("-msse4.1" HAVE_SSE41)

if(HAVE_AVX)
    add_compile_options(-mavx)
    message(STATUS "AVX enabled")
elseif(HAVE_SSE41)
    add_compile_options(-msse4.1)
    message(STATUS "SSE4.1 enabled")
endif()

# Add libsvdb - unified core library
if(EXISTS ${CMAKE_SOURCE_DIR}/src/CMakeLists.txt)
    add_subdirectory(src)
endif()

# Add extensions
if(EXISTS ${CMAKE_SOURCE_DIR}/src/ext/math/CMakeLists.txt)
    add_subdirectory(src/ext/math)
endif()

if(EXISTS ${CMAKE_SOURCE_DIR}/src/ext/json/CMakeLists.txt)
    add_subdirectory(src/ext/json)
endif()

if(EXISTS ${CMAKE_SOURCE_DIR}/src/ext/fts5/CMakeLists.txt)
    add_subdirectory(src/ext/fts5)
endif()

# Build all C++ targets
add_custom_target(cpp-build
    COMMENT "Building C++ targets in .build/cmake/"
)

# v0.11.3: C++ Unit Tests (GoogleTest)
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# Foundation classes test
add_executable(tests_cpp_unit
    tests/CppUnit/page_manager_v2_test.cpp
    tests/CppUnit/test_vtab_registry.cpp
)
target_link_libraries(tests_cpp_unit
    PRIVATE
    svdb
    GTest::gtest_main
)
target_include_directories(tests_cpp_unit PRIVATE
    ${CMAKE_SOURCE_DIR}/src/core
    ${CMAKE_SOURCE_DIR}/src/core/DS
    ${CMAKE_SOURCE_DIR}/src/core/IS
)

add_test(NAME CppUnitTests COMMAND tests_cpp_unit)

# v0.11.5: C++ Benchmarks (Google Benchmark)
# Fetch Google Benchmark
include(FetchContent)
FetchContent_Declare(
    google_benchmark
    URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
# Manually set benchmark options before making available
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(google_benchmark)

# Find SQLite3
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

# Create BenchmarkCpp directory
file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/tests/BenchmarkCpp)

# BenchmarkCpp executable
add_executable(BenchmarkCpp
    tests/BenchmarkCpp/main.cpp
)
target_link_libraries(BenchmarkCpp
    PRIVATE
    svdb
    benchmark::benchmark
    ${SQLITE3_LIBRARIES}
)
target_include_directories(BenchmarkCpp PRIVATE
    ${CMAKE_SOURCE_DIR}/src/core
    ${CMAKE_SOURCE_DIR}/src/core/SC
    ${SQLITE3_INCLUDE_DIRS}
)
