# libsvdb - Unified database library
# Compiles all core subsystems into a single shared library

cmake_minimum_required(VERSION 3.16)

# Enable SIMD
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx2" HAVE_AVX2)
check_cxx_compiler_flag("-mavx" HAVE_AVX)
check_cxx_compiler_flag("-msse4.1" HAVE_SSE41)

# Base compile options
set(SVDB_COMPILE_OPTS "")
set(SVDB_STATUS_MSG "")

if(HAVE_AVX2)
    list(APPEND SVDB_COMPILE_OPTS -mavx2)
    set(SVDB_STATUS_MSG "AVX2")
elseif(HAVE_AVX)
    list(APPEND SVDB_COMPILE_OPTS -mavx)
    set(SVDB_STATUS_MSG "AVX")
elseif(HAVE_SSE41)
    list(APPEND SVDB_COMPILE_OPTS -msse4.1)
    set(SVDB_STATUS_MSG "SSE4.1")
endif()

if(SVDB_STATUS_MSG)
    message(STATUS "${SVDB_STATUS_MSG} enabled for libsvdb")
else()
    message(STATUS "No SIMD extensions available for libsvdb")
endif()

# Collect all source files from core subsystems
set(SVDB_CORE_SOURCES
    # DS - Data Storage
    core/DS/varint.cpp
    core/DS/cell.cpp
    core/DS/btree.cpp
    core/DS/btree_cursor.cpp
    core/DS/simd.cpp
    core/DS/roaring.cpp
    core/DS/compression.cpp
    core/DS/value.cpp
    core/DS/page.cpp
    core/DS/freelist.cpp
    core/DS/balance.cpp
    core/DS/manager.cpp
    core/DS/wal.cpp
    core/DS/skip_list.cpp
    core/DS/overflow.cpp
    core/DS/cache.cpp
    core/DS/columnar.cpp
    core/DS/row_store.cpp
    core/DS/bloom_filter.cpp
    core/DS/column_vector.cpp
    core/DS/hybrid_store.cpp
    core/DS/arena.cpp
    core/DS/worker_pool.cpp
    core/DS/prefetch.cpp
    core/DS/slab.cpp
    core/DS/row.cpp
    core/DS/metrics.cpp

    # VM - Virtual Machine
    core/VM/hash.cpp
    core/VM/compare.cpp
    core/VM/sort.cpp
    core/VM/expr_eval.cpp
    core/VM/expr_engine.cpp
    core/VM/aggregate_engine.cpp
    core/VM/vm_dispatch.cpp
    core/VM/dispatch.cpp
    core/VM/type_conv.cpp
    core/VM/string_funcs.cpp
    core/VM/datetime.cpp
    core/VM/aggregate.cpp
    core/VM/string_pool.cpp
    core/VM/registers.cpp
    core/VM/opcodes.cpp
    core/VM/instruction.cpp
    core/VM/cursor.cpp
    core/VM/program.cpp
    core/VM/query_engine.cpp
    core/VM/exec.cpp
    core/VM/bytecode_vm.cpp
    core/VM/vm_opcode.cpp
    core/VM/vm_execute.cpp

    # QP - Query Processing
    core/QP/tokenizer.cpp
    core/QP/analyzer.cpp
    core/QP/normalize.cpp
    core/QP/type_infer.cpp
    core/QP/dag.cpp
    core/QP/binder.cpp
    core/QP/parser.cpp
    core/QP/parser_select.cpp
    core/QP/parser_expr.cpp
    core/QP/parser_dml.cpp
    core/QP/parser_ddl.cpp
    core/QP/parser_alter.cpp
    core/QP/parser_txn.cpp

    # CG - Code Generation
    core/CG/compiler.cpp
    core/CG/expr_compiler.cpp
    core/CG/optimizer.cpp
    core/CG/plan_cache.cpp
    core/CG/register.cpp
    core/CG/bytecode_compiler.cpp
    core/CG/direct_compiler.cpp

    # TM - Transaction Management
    core/TM/transaction.cpp

    # PB - Platform Bridges (VFS)
    core/PB/vfs.cpp

    # SF - Standard Functions (optimization)
    core/SF/opt.cpp

    # IS - Information Schema
    core/IS/schema.cpp

    # Wrapper - Invoke chain
    core/wrapper/invoke_chain_wrapper.cpp

    # CGO - Hash Join
    core/cgo/hash_join.cpp

    # VM Engine - Query orchestration
    core/VM/engine/engine.cpp

    # SVDB — Unified C Public API
    core/svdb/database.cpp
    core/svdb/exec.cpp
    core/svdb/query.cpp
    core/svdb/result.cpp
    core/svdb/transaction.cpp
    core/svdb/statement.cpp
    core/svdb/pragma.cpp
    core/svdb/window.cpp
    core/svdb/hash_join.cpp
    core/svdb/setops.cpp
    core/svdb/fk_trigger.cpp
    core/svdb/schema.cpp
    core/svdb/explain.cpp
    core/svdb/backup.cpp
    core/svdb/io.cpp
    core/svdb/vacuum.cpp
    core/svdb/extensions.cpp
    core/svdb/pools.cpp
)

# Build libsvdb
add_library(svdb SHARED ${SVDB_CORE_SOURCES})

target_include_directories(svdb PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${CMAKE_CURRENT_SOURCE_DIR}/core/DS
    ${CMAKE_CURRENT_SOURCE_DIR}/core/VM
    ${CMAKE_CURRENT_SOURCE_DIR}/core/QP
    ${CMAKE_CURRENT_SOURCE_DIR}/core/CG
    ${CMAKE_CURRENT_SOURCE_DIR}/core/TM
    ${CMAKE_CURRENT_SOURCE_DIR}/core/PB
    ${CMAKE_CURRENT_SOURCE_DIR}/core/SF
    ${CMAKE_CURRENT_SOURCE_DIR}/core/IS
    ${CMAKE_CURRENT_SOURCE_DIR}/core/wrapper
    ${CMAKE_CURRENT_SOURCE_DIR}/core/cgo
    ${CMAKE_CURRENT_SOURCE_DIR}/core/VM/engine
    ${CMAKE_CURRENT_SOURCE_DIR}/core/svdb
)

if(SVDB_COMPILE_OPTS)
    target_compile_options(svdb PRIVATE ${SVDB_COMPILE_OPTS})
endif()

set_target_properties(svdb PROPERTIES
    OUTPUT_NAME "svdb"
    VERSION 1.0.0
    SOVERSION 1
)

# Install to .build/cmake/lib
install(TARGETS svdb
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
    COMPONENT Runtime
)
