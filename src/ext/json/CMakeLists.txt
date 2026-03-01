# svdb_ext_json - JSON extension for libsvdb

cmake_minimum_required(VERSION 3.16)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx" HAVE_AVX)
check_cxx_compiler_flag("-msse4.1" HAVE_SSE41)

if(HAVE_AVX)
    add_compile_options(-mavx)
    message(STATUS "AVX enabled for JSON extension")
elseif(HAVE_SSE41)
    add_compile_options(-msse4.1)
    message(STATUS "SSE4.1 enabled for JSON extension")
else()
    message(STATUS "No SIMD extensions available for JSON extension")
endif()

add_library(svdb_ext_json SHARED
    lib/json.cpp
)

target_include_directories(svdb_ext_json PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
)

set_target_properties(svdb_ext_json PROPERTIES
    OUTPUT_NAME "svdb_ext_json"
    VERSION 1.0.0
    SOVERSION 1
)

install(TARGETS svdb_ext_json
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
    COMPONENT Runtime
)
