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
| `balance.go` | `balance.cpp` | ❌ | B-Tree balancing logic |
| `bloom_filter.go` | — | 📋 | Can use C++ roaring instead |
| `btree.go` | `btree.cpp` | 🟡 | **Search works, insert/delete are placeholders** |
| `btree_cursor.go` | `btree_cursor.cpp` | ❌ | Cursor traversal |
| `cache.go` | `cache.cpp` | ❌ | LRU/ARC cache |
| `cell.go` | `cell.cpp` | ✅ | Cell encoding/decoding complete |
| `column_store.go` | `columnar.cpp` | ❌ | Columnar storage engine |
| `column_vector.go` | — | 📋 | Go column vectors, keep |
| `compact.go` | — | 📋 | Compaction orchestration |
| `compression.go` | `compression.cpp` | ✅ | LZ4/ZSTD use C++ |
| `encoding.go` | `varint.cpp` | ✅ | Varint uses C++ |
| `exec_columnar.go` | — | 📋 | Columnar execution (Go-specific) |
| `freelist.go` | `freelist.cpp` | ❌ | Free list management |
| `hybrid_store.go` | — | 📋 | Row/columnar adapter (Go-specific) |
| `index_engine.go` | — | 📋 | Index orchestration |
| `manager.go` | `manager.cpp` | ❌ | Page manager |
| `metrics.go` | — | 📋 | Go telemetry |
| `mmap.go` | — | 📋 | Go memory mapping |
| `overflow.go` | `overflow.cpp` | ❌ | Overflow page chains |
| `page.go` | `page.cpp` | ❌ | Page management |
| `parallel.go` | — | 📋 | Go parallel utilities |
| `persistence.go` | — | 📋 | File I/O orchestration |
| `prefetch.go` | — | 📋 | Go prefetch utilities |
| `roaring_bitmap.go` | `roaring.cpp` | ✅ | Uses C++ roaring |
| `row.go` | — | 📋 | Go row handling |
| `row_store.go` | `row_store.cpp` | ❌ | Row storage engine |
| `skip_list.go` | `skip_list.cpp` | ❌ | Skip list data structure |
| `slab.go` | — | 📋 | Go slab allocator |
| `value.go` | `value.cpp` | ✅ | Value type uses C++ |
| `vtab.go` | — | 📋 | Virtual table interface (Go) |
| `vtab_cursor.go` | — | 📋 | Virtual table cursor (Go) |
| `vtab_module.go` | — | 📋 | Virtual table module (Go) |
| `wal.go` | `wal.cpp` | ❌ | Write-ahead logging |
| `worker_pool.go` | — | 📋 | Go worker pool |

**DS Summary**: 6/36 complete, 1/36 partial, 29/36 need migration

---

## VM (Virtual Machine) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `aggregate_funcs.go` | `aggregate.cpp` | ✅ | Aggregate functions use C++ |
| `bc_opcodes.go` | — | 📋 | Opcode constants |
| `bytecode_handlers.go` | `opcodes.cpp` | ❌ | Bytecode opcode implementations |
| `bytecode_prog.go` | `program.cpp` | ❌ | Bytecode program representation |
| `bytecode_vm.go` | `bytecode_vm.cpp` | ❌ | Main VM loop |
| `compare.go` | `compare.cpp` | ✅ | Comparison functions use C++ |
| `compiler.go` | — | 📋 | High-level compiler orchestration |
| `cursor.go` | `cursor.cpp` | ❌ | Cursor management |
| `datetime.go` | `datetime.cpp` | ✅ | DateTime functions use C++ |
| `dispatch.go` | `dispatch.cpp` | ✅ | Dispatch uses C++ |
| `engine.go` | `query_engine.cpp` | ❌ | Query execution engine |
| `engine/aggregate.go` | `aggregate_engine.cpp` | ✅ | Aggregate engine uses C++ |
| `engine/join.go` | `hash_join.cpp` | ✅ | Hash JOIN uses C++ |
| `engine/select.go` | — | 📋 | SELECT orchestration |
| `engine/sort.go` | `sort.cpp` | ✅ | Sort uses C++ |
| `engine/subquery.go` | — | 📋 | Subquery orchestration |
| `engine/window.go` | — | 📋 | Window orchestration |
| `exec.go` | `exec.cpp` | ❌ | Execution engine |
| `expr_bytecode.go` | `expr_engine.cpp` | ✅ | Expression bytecode uses C++ |
| `expr_eval.go` | `expr_eval.cpp` | ✅ | Expression evaluation uses C++ |
| `hash.go` | `hash.cpp` | ✅ | Hash functions use C++ |
| `instr.go` | `instruction.cpp` | ❌ | Instruction format |
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

**VM Summary**: 17/40 complete, 0/40 partial, 8/40 need migration, 15/40 Go-only

---

## QP (Query Processing) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `analyzer.go` | `analyzer.cpp` | ❌ | Semantic analysis |
| `binder.go` | `binder.cpp` | ❌ | Name resolution |
| `dag.go` | `dag.cpp` | ❌ | Query DAG representation |
| `normalize.go` | `normalize.cpp` | ❌ | Query normalization |
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
| `type_infer.go` | `type_infer.cpp` | ❌ | Type inference |

**QP Summary**: 4/18 complete, 0/18 partial, 12/18 need migration, 2/18 Go-only

---

## CG (Code Generation) Subsystem

| Go File | C++ Target | Status | Notes |
|---------|-----------|--------|-------|
| `bytecode_compiler.go` | `bytecode_compiler.cpp` | ❌ | Bytecode generation |
| `bytecode_expr.go` | `expr_compiler.cpp` | ✅ | Expression compilation uses C++ |
| `compiler.go` | `compiler.cpp` | ✅ | Compiler uses C++ |
| `compiler/aggregate.go` | — | 📋 | Aggregate compilation |
| `compiler/cte.go` | — | 📋 | CTE compilation |
| `compiler/dml.go` | — | 📋 | DML compilation |
| `compiler/select.go` | — | 📋 | SELECT compilation |
| `compiler/subquery.go` | — | 📋 | Subquery compilation |
| `compiler/window.go` | — | 📋 | Window compilation |
| `direct_compiler.go` | `direct_compiler.cpp` | ❌ | Direct compilation |
| `expr.go` | — | 📋 | Expression handling |
| `expr_compiler.go` | `expr_compiler.cpp` | ✅ | Expression compiler uses C++ |
| `optimizer.go` | `optimizer.cpp` | ✅ | Optimizer uses C++ |
| `plan_cache.go` | `plan_cache.cpp` | ✅ | Plan cache uses C++ |
| `stmt_cache.go` | — | 📋 | Statement cache (Go) |

**CG Summary**: 6/15 complete, 0/15 partial, 2/15 need migration, 7/15 Go-only

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
| **DS** | 6 | 1 | 29 | 0 | 36 |
| **VM** | 17 | 0 | 8 | 15 | 40 |
| **QP** | 4 | 0 | 12 | 2 | 18 |
| **CG** | 6 | 0 | 2 | 7 | 15 |
| **TM** | 1 | 0 | 0 | 4 | 5 |
| **PB** | 1 | 0 | 0 | 2 | 3 |
| **IS** | 0 | 0 | 0 | 11 | 11 |
| **SF** | 1 | 0 | 0 | 10 | 11 |
| **TOTAL** | **36** | **1** | **51** | **51** | **139** |

---

## Priority Migration Tasks

### High Priority (Performance Critical)

1. **Complete `btree.cpp`** (DS)
   - Implement `svdb_btree_insert()` - currently a placeholder
   - Implement `svdb_btree_delete()` - currently a placeholder
   - Add page split logic
   - Add page merge logic
   - **Impact**: Enables migration of `btree.go` (806 lines)

2. **Create `page.cpp`** (DS)
   - Page allocation/deallocation
   - Page header management
   - Cell pointer management
   - **Impact**: Foundation for storage layer

3. **Create `freelist.cpp`** (DS)
   - Free list management
   - Page recycling
   - Trunk page management
   - **Impact**: Storage efficiency

4. **Create `manager.cpp`** (DS)
   - PageManager implementation
   - Buffer pool integration
   - I/O scheduling
   - **Impact**: Core storage orchestration

5. **Create `wal.cpp`** (DS)
   - WAL write-ahead logging
   - Checkpoint logic
   - Recovery
   - **Impact**: Durability and ACID

### Medium Priority

6. **Create `balance.cpp`** (DS)
   - B-Tree page balancing
   - Split/merge operations
   - **Impact**: B-Tree performance

7. **Create `btree_cursor.cpp`** (DS)
   - Cursor traversal
   - Seek operations
   - **Impact**: Query iteration

8. **Create `parser.cpp`** (QP)
   - SQL parser implementation
   - AST construction
   - **Impact**: Query parsing performance

9. **Create `binder.cpp`** (QP)
   - Name resolution
   - Type checking
   - **Impact**: Semantic analysis

10. **Create `analyzer.cpp`** (QP)
    - Query analysis
    - Statistics collection
    - **Impact**: Optimization

### Low Priority

11. **Create `bytecode_compiler.cpp`** (CG)
    - Bytecode generation
    - Instruction scheduling
    - **Impact**: Compilation speed

12. **Create `direct_compiler.cpp`** (CG)
    - Fast path compilation
    - **Impact**: Simple query performance

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
- `cache.go` - Go LRU cache (unless C++ version needed)

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

1. **Complete `btree.cpp`** - Finish insert/delete with page split logic
2. **Create `page.cpp`** - Page management foundation
3. **Create `freelist.cpp`** - Free list management
4. **Create `manager.cpp`** - PageManager implementation
5. **Create `wal.cpp`** - WAL implementation

After DS layer is complete, proceed with QP parser migration.
