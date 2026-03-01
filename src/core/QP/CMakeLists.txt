cmake_minimum_required(VERSION 3.16)
project(svdb_qp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# QP library sources
add_library(svdb_qp SHARED
    tokenizer.cpp
)

target_include_directories(svdb_qp PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(svdb_qp PROPERTIES
    OUTPUT_NAME "svdb_qp"
    VERSION 1.0.0
    SOVERSION 1
)

# Install to .build/cmake/lib
install(TARGETS svdb_qp
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
    COMPONENT Runtime
)
