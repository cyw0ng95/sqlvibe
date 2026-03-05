cmake_minimum_required(VERSION 3.16)
project(svdb_vm_phase2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable SIMD if available
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx2" HAVE_AVX2)
check_cxx_compiler_flag("-mavx" HAVE_AVX)
check_cxx_compiler_flag("-msse4.1" HAVE_SSE41)

if(HAVE_AVX2)
    add_compile_options(-mavx2)
    message(STATUS "AVX2 enabled for VM Phase 2")
elseif(HAVE_AVX)
    add_compile_options(-mavx)
    message(STATUS "AVX enabled for VM Phase 2")
elseif(HAVE_SSE41)
    add_compile_options(-msse4.1)
    message(STATUS "SSE4.1 enabled for VM Phase 2")
else()
    message(STATUS "No SIMD extensions available for VM Phase 2")
endif()

# VM Phase 2 library sources (hash join)
add_library(svdb_vm_phase2 SHARED
    hash_join.cpp
)

target_include_directories(svdb_vm_phase2 PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(svdb_vm_phase2 PROPERTIES
    OUTPUT_NAME "svdb_vm_phase2"
    VERSION 1.0.0
    SOVERSION 1
)

# Install to .build/cmake/lib
install(TARGETS svdb_vm_phase2
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
    COMPONENT Runtime
)
