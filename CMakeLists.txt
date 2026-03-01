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

# Add subdirectories for C++ extensions
if(EXISTS ${CMAKE_SOURCE_DIR}/ext/math/CMakeLists.txt)
    add_subdirectory(ext/math)
endif()

if(EXISTS ${CMAKE_SOURCE_DIR}/ext/json/CMakeLists.txt)
    add_subdirectory(ext/json)
endif()

if(EXISTS ${CMAKE_SOURCE_DIR}/ext/fts5/CMakeLists.txt)
    add_subdirectory(ext/fts5)
endif()

# Add DS CGO library
if(EXISTS ${CMAKE_SOURCE_DIR}/internal/DS/cgo/CMakeLists.txt)
    add_subdirectory(internal/DS/cgo)
endif()

# Add VM CGO library
if(EXISTS ${CMAKE_SOURCE_DIR}/internal/VM/cgo/CMakeLists.txt)
    add_subdirectory(internal/VM/cgo)
endif()

# Add QP CGO library
if(EXISTS ${CMAKE_SOURCE_DIR}/internal/QP/cgo/CMakeLists.txt)
    add_subdirectory(internal/QP/cgo)
endif()

# Add CG CGO library (Phase 3)
if(EXISTS ${CMAKE_SOURCE_DIR}/internal/CG/cgo/CMakeLists.txt)
    add_subdirectory(internal/CG/cgo)
endif()

# Add pkg/sqlvibe CGO library (Phase 2)
if(EXISTS ${CMAKE_SOURCE_DIR}/pkg/sqlvibe/cgo/CMakeLists.txt)
    add_subdirectory(pkg/sqlvibe/cgo)
endif()

# Build all C++ targets
add_custom_target(cpp-build
    COMMENT "Building C++ targets in .build/cmake/"
)
