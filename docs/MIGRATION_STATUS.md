# CGO Migration Status: internal/ → src/core/

**Last Updated**: 2026-03-01  
**Goal**: Migrate performance-critical Go code from `internal/` to C++ in `src/core/`

---

## Legend

| Status | Meaning |
|--------|---------|
| ✅ **Complete** | C++ implementation exists and is used by Go wrapper |
| 🟡 **Partial** | C++ implementation exists but is incomplete |
| ❌ **Not Started** | No C++ implementation yet |
| 📋 **Go-Only** | Should remain in Go (orchestration, tests, Go-specific) |

---

## DS (Data Storage) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `arena.go` | — | 📋 | Go arena allocator, keep in Go |
| `backup.go` | — | 📋 | High-level orchestration, keep in Go |
| `balance.go` | `balance.cpp` | ✅ | B-Tree balancing: split, merge, redistribute |
| `bloom_filter.go` | — | 📋 | Can use C++ roaring instead |
| `btree.go` | `btree.cpp` | ✅ | Search, insert (with page split), delete complete |
| `btree_cursor.go` | `btree_cursor.cpp` | ✅ | Cursor traversal, page cache helpers |
| `cache.go` | `cache.cpp` | ❌ | LRU/ARC cache |
| `cell.go` | `cell.cpp` | ✅ | Cell encoding/decoding complete |
| `column_store.go` | `columnar.cpp` | ❌ | Columnar storage engine |
| `column_vector.go` | — | 📋 | Go column vectors, keep |
| `compact.go` | — | 📋 | Compaction orchestration |
| `compression.go` | `compression.cpp` | ✅ | LZ4/ZSTD use C++ |
| `encoding.go` | `varint.cpp` | ✅ | Varint uses C++ |
| `exec_columnar.go` | — | 📋 | Columnar execution (Go-specific) |
| `freelist.go` | `freelist.cpp` | ✅ | Free list trunk/leaf management |
| `hybrid_store.go` | — | 📋 | Row/columnar adapter (Go-specific) |
| `index_engine.go` | — | 📋 | Index orchestration |
| `manager.go` | `manager.cpp` | ✅ | Page offset, header read/write helpers |
| `metrics.go` | — | 📋 | Go telemetry |
| `mmap.go` | — | 📋 | Go memory mapping |
| `overflow.go` | `overflow.cpp` | ❌ | Overflow page chains |
| `page.go` | `page.cpp` | ✅ | Page header management, compaction |
| `parallel.go` | — | 📋 | Go parallel utilities |
| `persistence.go` | — | 📋 | File I/O orchestration |
| `prefetch.go` | — | 📋 | Go prefetch utilities |
| `roaring_bitmap.go` | `roaring.cpp` | ✅ | Uses C++ roaring |
| `row.go` | — | 📋 | Go row handling |
| `row_store.go` | `row_store.cpp` | ❌ | Row storage engine |
| `skip_list.go` | `skip_list.cpp` | ✅ | 16-level skip list (int64+string keys) |
| `slab.go` | — | 📋 | Go slab allocator |
| `value.go` | `value.cpp` | ✅ | Value type uses C++ |
| `vtab.go` | — | 📋 | Virtual table interface (Go) |
| `vtab_cursor.go` | — | 📋 | Virtual table cursor (Go) |
| `vtab_module.go` | — | 📋 | Virtual table module (Go) |
| `wal.go` | `wal.cpp` | ✅ | Length-prefixed WAL record encode/decode |
| `worker_pool.go` | — | 📋 | Go worker pool |

**DS Summary**: 13/36 complete, 0/36 partial, 2/36 need migration (cache.cpp, overflow.cpp, columnar.cpp, row_store.cpp)

---

## VM (Virtual Machine) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `aggregate_funcs.go` | `aggregate.cpp` | ✅ | Aggregate functions use C++ |
| `bc_opcodes.go` | — | 📋 | Opcode constants |
| `bytecode_handlers.go` | `opcodes.cpp` | ✅ | Opcode names, metadata, classification |
| `bytecode_prog.go` | `program.cpp` | ✅ | Bytecode program container (instrs, consts, col names) |
| `bytecode_vm.go` | `bytecode_vm.cpp` | ❌ | Main VM loop |
| `compare.go` | `compare.cpp` | ✅ | Comparison functions use C++ |
| `compiler.go` | — | 📋 | High-level compiler orchestration |
| `cursor.go` | `cursor.cpp` | ✅ | Cursor array management (256 slots) |
| `datetime.go` | `datetime.cpp` | ✅ | DateTime functions use C++ |
| `dispatch.go` | `dispatch.cpp` | ✅ | Dispatch uses C++ |
| `engine.go` | `query_engine.cpp` | ✅ | Query classification, table extraction, comment stripping |
| `engine/aggregate.go` | `aggregate_engine.cpp` | ✅ | Aggregate engine uses C++ |
| `engine/join.go` | `hash_join.cpp` | ✅ | Hash JOIN uses C++ |
| `engine/select.go` | — | 📋 | SELECT orchestration |
| `engine/sort.go` | `sort.cpp` | ✅ | Sort uses C++ |
| `engine/subquery.go` | — | 📋 | Subquery orchestration |
| `engine/window.go` | — | 📋 | Window orchestration |
| `exec.go` | `exec.cpp` | ✅ | Cache eligibility, FNV-1a hash, columnar threshold |
| `expr_bytecode.go` | `expr_engine.cpp` | ✅ | Expression bytecode uses C++ |
| `expr_eval.go` | `expr_eval.cpp` | ✅ | Expression evaluation uses C++ |
| `hash.go` | `hash.cpp` | ✅ | Hash functions use C++ |
| `instr.go` | `instruction.cpp` | ✅ | 16-byte instruction struct, flag helpers |
| `instruction.go` | — | 📋 | Instruction constants |
| `opcodes.go` | — | 📋 | Opcode definitions |
| `program.go` | — | 📋 | Program representation |
| `query_engine.go` | — | 📋 | Query engine orchestration |
| `query_expr.go` | — | 📋 | Query expression handling |
| `query_operators.go` | — | 📋 | Query operators |
| `registers.go` | `registers.cpp` | ✅ | Register allocator uses C++ |
| `result_cache.go` | — | 📋 | Go result cache |
| `row_eval.go` | — | 📋 | Row evaluation |
| `sort.go` | `sort.cpp` | ✅ | Sort uses C++ |
| `string_funcs.go` | `string_funcs.cpp` | ✅ | String functions use C++ |
| `string_pool.go` | `string_pool.cpp` | ✅ | String pool uses C++ |
| `subquery_cache.go` | — | 📋 | Go subquery cache |
| `type_conv.go` | `type_conv.cpp` | ✅ | Type conversion uses C++ |
| `value.go` | — | 📋 | Go value wrapper |
| `wrapper/invoke_chain.go` | `invoke_chain_wrapper.cpp` | ✅ | Invoke chain uses C++ |
| `wrapper/types.go` | — | 📋 | Wrapper type definitions |

**VM Summary**: 24/40 complete, 0/40 partial, 1/40 need migration (bytecode_vm.cpp), 15/40 Go-only

---

## QP (Query Processing) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `analyzer.go` | `analyzer.cpp` | ✅ | Column analysis, aggregate/subquery detection |
| `binder.go` | `binder.cpp` | ✅ | Placeholder counting, named param extraction |
| `dag.go` | `dag.cpp` | ✅ | DAG nodes/edges, topological sort |
| `normalize.go` | `normalize.cpp` | ✅ | Query normalization (lowercase, trim, literal→?) |
| `optimizer.go` | `optimizer.cpp` | ✅ | Optimizer uses C++ |
| `parse_cache.go` | `plan_cache.cpp` | ✅ | Parse cache uses C++ |
| `parser.go` | `parser.cpp` | ❌ | Main SQL parser |
| `parser_alter.go` | `parser_alter.cpp` | ❌ | ALTER TABLE parser |
| `parser_create.go` | `parser_ddl.cpp` | ❌ | CREATE statement parser |
| `parser_dml.go` | `parser_dml.cpp` | ❌ | DML parser |
| `parser_expr.go` | `parser_expr.cpp` | ❌ | Expression parser |
| `parser_select.go` | `parser_select.cpp` | ❌ | SELECT parser |
| `parser_txn.go` | `parser_txn.cpp` | ❌ | Transaction parser |
| `schema_parser.go` | — | 📋 | Schema parsing (Go-specific) |
| `tokenizer.go` | `tokenizer.cpp` | ✅ | FastTokenCount uses C++ |
| `tokenizer_count.go` | `tokenizer.cpp` | ✅ | Token count uses C++ |
| `topn.go` | — | 📋 | TOP-N optimization (Go) |
| `type_infer.go` | `type_infer.cpp` | ✅ | Literal type inference, type promotion, func return types |

**QP Summary**: 9/18 complete, 0/18 partial, 7/18 need migration (parser files), 2/18 Go-only

---

## CG (Code Generation) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `bytecode_compiler.go` | `bytecode_compiler.cpp` | ✅ | Fast-path detection, aggregate/sort/limit/window analysis |
| `bytecode_expr.go` | `expr_compiler.cpp` | ✅ | Expression compilation uses C++ |
| `compiler.go` | `compiler.cpp` | ✅ | Compiler uses C++ |
| `compiler/aggregate.go` | — | 📋 | Aggregate compilation |
| `compiler/cte.go` | — | 📋 | CTE compilation |
| `compiler/dml.go` | — | 📋 | DML compilation |
| `compiler/select.go` | — | 📋 | SELECT compilation |
| `compiler/subquery.go` | — | 📋 | Subquery compilation |
| `compiler/window.go` | — | 📋 | Window compilation |
| `direct_compiler.go` | `direct_compiler.cpp` | ✅ | Simple-select detection, table/LIMIT/OFFSET extraction |
| `expr.go` | — | 📋 | Expression handling |
| `expr_compiler.go` | `expr_compiler.cpp` | ✅ | Expression compiler uses C++ |
| `optimizer.go` | `optimizer.cpp` | ✅ | Optimizer uses C++ |
| `plan_cache.go` | `plan_cache.cpp` | ✅ | Plan cache uses C++ |
| `stmt_cache.go` | — | 📋 | Statement cache (Go) |

**CG Summary**: 8/15 complete, 0/15 partial, 0/15 need migration, 7/15 Go-only

---

## TM (Transaction Management) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `isolation.go` | — | 📋 | Isolation level logic (Go) |
| `lock.go` | — | 📋 | Lock manager (Go) |
| `mvcc.go` | — | 📋 | MVCC (Go) |
| `transaction.go` | `transaction.cpp` | ✅ | Transaction uses C++ |
| `wal.go` | — | 📋 | WAL orchestration (Go) |

**TM Summary**: 1/5 complete, 0/5 partial, 0/5 need migration, 4/5 Go-only

---

## PB (Platform Bridges) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `file.go` | `vfs.cpp` | ✅ | VFS uses C++ |
| `vfs_memory.go` | — | 📋 | Memory VFS (Go) |
| `vfs_unix.go` | — | 📋 | Unix VFS (Go) |

**PB Summary**: 1/3 complete, 0/3 partial, 0/3 need migration, 2/3 Go-only

---

## IS (Information Schema) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `columns_view.go` | — | 📋 | Schema views (Go) |
| `constraints_view.go` | — | 📋 | Schema views (Go) |
| `information_schema.go` | — | 📋 | Schema orchestration (Go) |
| `referential_view.go` | — | 📋 | Schema views (Go) |
| `registry.go` | — | 📋 | Schema registry (Go) |
| `schema_cache.go` | — | 📋 | Schema cache (Go) |
| `schema_extractor.go` | — | 📋 | Schema extraction (Go) |
| `schema_parser.go` | — | 📋 | Schema parsing (Go) |
| `tables_view.go` | — | 📋 | Schema views (Go) |
| `views_view.go` | — | 📋 | Schema views (Go) |
| `vtab_registry.go` | — | 📋 | Virtual table registry (Go) |

**IS Note**: `schema.cpp` exists but appears to be a placeholder

**IS Summary**: 0/11 complete, 0/11 partial, 0/11 need migration, 11/11 Go-only

---

## SF (System Framework) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `errors/error_code.go` | — | 📋 | Error codes (Go) |
| `errors/error.go` | — | 📋 | Error handling (Go) |
| `errors/error_map.go` | — | 📋 | Error mappings (Go) |
| `errors/sqlstate.go` | — | 📋 | SQLSTATE codes (Go) |
| `errors/version.go` | — | 📋 | Version info (Go) |
| `log.go` | — | 📋 | Logging (Go) |
| `opt/cpu_opt.go` | — | 📋 | CPU optimization hints |
| `opt/simd.go` | `opt.cpp` | ✅ | SIMD detection uses C++ |
| `util/assert.go` | — | 📋 | Assertions (Go) |
| `util/pool.go` | — | 📋 | Sync pool utilities |
| `vfs/vfs.go` | — | 📋 | VFS interface (Go) |

**SF Summary**: 1/11 complete, 0/11 partial, 0/11 need migration, 10/11 Go-only

---

## Overall Summary

| Subsystem | Complete | Partial | Need Migration | Go-Only | Total |
|-----------|----------|---------|----------------|---------|-------|
| **DS** | 13 | 0 | 4 | 19 | 36 |
| **VM** | 24 | 0 | 1 | 15 | 40 |
| **QP** | 9 | 0 | 7 | 2 | 18 |
| **CG** | 8 | 0 | 0 | 7 | 15 |
| **TM** | 1 | 0 | 0 | 4 | 5 |
| **PB** | 1 | 0 | 0 | 2 | 3 |
| **IS** | 0 | 0 | 0 | 11 | 11 |
| **SF** | 1 | 0 | 0 | 10 | 11 |
| **TOTAL** | **57** | **0** | **12** | **70** | **139** |

**Migration Progress**: 57/87 migratable items complete (**66%**)

---

## Remaining Migration Tasks

### DS (4 items)
1. **`cache.cpp`** - LRU/ARC page cache
2. **`overflow.cpp`** - Overflow page chains for large payloads
3. **`columnar.cpp`** - Columnar storage engine
4. **`row_store.cpp`** - Row storage engine

### VM (1 item)
5. **`bytecode_vm.cpp`** - Main VM execution loop (complex, Go-native performance may be sufficient)

### QP (7 items - parser files)
6. **`parser.cpp`** - Main SQL parser
7. **`parser_alter.cpp`** - ALTER TABLE parser
8. **`parser_ddl.cpp`** - CREATE/DROP parser
9. **`parser_dml.cpp`** - DML parser
10. **`parser_expr.cpp`** - Expression parser
11. **`parser_select.cpp`** - SELECT parser
12. **`parser_txn.cpp`** - Transaction parser

---

## Files That Should Remain in Go

The following files implement Go-specific patterns or orchestration logic that should **NOT** be migrated:

### Orchestration Layer
- `hybrid_store.go` - Adapts row/columnar storage
- `engine.go` - High-level query orchestration
- `compiler.go` - Compiler orchestration
- `transaction.go` - Transaction lifecycle (Go TM)

### Go Concurrency Patterns
- `worker_pool.go` - Go goroutine pools
- `parallel.go` - Go parallel utilities
- `prefetch.go` - Go async prefetch

### Memory Management (Go-optimized)
- `arena.go` - Go arena allocator
- `slab.go` - Go slab allocator

### Virtual Tables
- `vtab.go`, `vtab_cursor.go`, `vtab_module.go` - Go virtual table interface

### Information Schema
- All `IS/*.go` files - Schema metadata is Go-specific

### Error Handling & Logging
- `SF/errors/*.go` - Go error types
- `SF/log.go` - Go logging

### Utilities
- `SF/util/assert.go` - Go assertions
- `SF/util/pool.go` - Go sync.Pool

---

## Migration Pattern

For each file to migrate:

1. **Create C++ header** (`src/core/<SUBSYS>/<module>.h`)
   - Define C-compatible API with `extern "C"`
   - Use opaque pointers for C++ classes
   - Document function contracts

2. **Create C++ implementation** (`src/core/<SUBSYS>/<module>.cpp`)
   - Implement core logic in C++
   - Use SIMD intrinsics where applicable
   - Manual memory management (RAII, smart pointers)
   - Add assertions for defensive programming

3. **Update CMakeLists.txt**
   - Add source to `src/CMakeLists.txt`
   - Ensure correct include paths
   - Update library dependencies

4. **Convert Go file to CGO wrapper**
   - Remove pure-Go implementation
   - Keep Go API for backward compatibility
   - Call C++ via CGO
   - Handle memory management (C.free, C.CString)

5. **Test**
   - Run unit tests: `./build.sh -t`
   - Run benchmarks: `./build.sh -b`
   - Verify no regressions
   - Check coverage: `./build.sh -t -c`

6. **Clean up**
   - Remove obsolete Go functions
   - Update comments
   - Mark file as migrated in this document

---

## Next Steps

1. **QP parser migration** - Migrate the 7 SQL parser files to C++ for faster tokenization/parsing
2. **DS cache.cpp** - LRU/ARC page cache for buffer pool
3. **DS overflow.cpp** - Overflow page chain management
4. **VM bytecode_vm.cpp** - Main VM execution loop (highest complexity)
