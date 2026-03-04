# Go to C++ Migration Analysis

**Date**: 2026-03-03  
**Project**: sqlvibe - High-performance in-memory database engine  
**Goal**: Migrate all Go business logic to C++ while maintaining Go as a thin binding layer

---

## Executive Summary

The sqlvibe project has made significant progress in migrating performance-critical components from Go to C++. The current architecture uses a **unified C public API** (`svdb.h`) that allows pure C/C++ callers to use the engine without Go runtime overhead.

### Current State
- **249 Go files** remain in `internal/`
- **37 CGO wrapper files** (`*_cgo.go`) - thin bindings to C++
- **C++ implementation** complete in `src/core/` for most subsystems
- **Go orchestration layer** still contains business logic that should move to C++

### Target State
- Go becomes a **pure type-mapping layer** (~400 LOC in `internal/cgo/`)
- All business logic, query processing, and execution in C++
- Zero Go callback overhead for inner loops

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Go Application Layer                      │
│  (pkg/sqlvibe, cmd/, tests/)                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Go Binding Layer (internal/cgo/)                │
│  Pure type mapping, error conversion, memory management      │
│  Target: ~400 LOC, zero business logic                       │
└─────────────────────────────────────────────────────────────┘
                            ↓ (CGO, ~5ns overhead)
┌─────────────────────────────────────────────────────────────┐
│            C Public API (src/core/svdb/svdb.h)              │
│  svdb_open, svdb_exec, svdb_query, svdb_prepare, etc.       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              C++ Engine Module (src/core/)                   │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │
│  │    DS    │    VM    │    QP    │    CG    │    TM    │  │
│  │  Storage │ Execution│  Parser  │ Compiler │ Transact │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │
│  │    PB    │    SF    │    IS    │  wrapper │   cgo    │  │
│  │   VFS    │   Opt    │  Schema  │  Invoke  │Hash Join │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Migration Status by Subsystem

### ✅ Complete (C++ Native, Go is Thin Wrapper)

| Subsystem | Location | Status | Notes |
|-----------|----------|--------|-------|
| **SVDB Public API** | `src/core/svdb/` | ✅ Complete | C API: `svdb_open/exec/query/prepare` |
| **VM Core** | `src/core/VM/` | ✅ Complete | 46 opcodes, bytecode VM, dispatch |
| **VM Engine** | `src/core/VM/engine/` | ✅ Complete | FilterRows, InnerJoin, GroupRows, SortRows |
| **DS Core** | `src/core/DS/` | ✅ Complete | B-Tree, columnar, row store, overflow |
| **QP Parser** | `src/core/QP/` | ✅ Complete | Tokenizer, parser, binder, analyzer |
| **CG Compiler** | `src/core/CG/` | ✅ Complete | Bytecode compiler, optimizer, plan cache |
| **TM Transaction** | `src/core/TM/` | ✅ Complete | Transaction management |
| **PB VFS** | `src/core/PB/` | ✅ Complete | Virtual file system |
| **IS Schema** | `src/core/IS/` | ✅ Complete | Information schema, schema cache |
| **SF Opt** | `src/core/SF/` | ✅ Complete | Standard functions optimization |

---

### ⚠️ Partial (Go Still Has Business Logic)

These subsystems have C++ implementations but Go still contains duplicate logic:

#### 1. **VM - Virtual Machine Orchestration** 
- **C++**: `src/core/VM/bytecode_vm.cpp`, `vm_dispatch.cpp`, `engine.cpp`
- **Go**: `internal/VM/engine.go`, `internal/VM/exec.go`, `internal/VM/cursor.go`
- **Issue**: Go `VM` struct (278 LOC) still handles:
  - Program execution loop
  - Register management
  - Cursor array operations
  - Branch prediction
  - Result collection
  - Ephemeral tables for SetOps
  - Subquery cache management

**Migration Priority**: HIGH  
**Estimated Effort**: 2-3 days  
**Action**: Move VM orchestration to C++, Go becomes pure handle wrapper

---

#### 2. **DS - Storage Manager**
- **C++**: `src/core/DS/manager.cpp`, `btree.cpp`, `hybrid_store.cpp`
- **Go**: `internal/DS/manager.go` (182 LOC), `internal/DS/btree.go` (843 LOC)
- **Issue**: Go `PageManager` and `BTree` still contain:
  - Page allocation/deallocation logic
  - Header read/write
  - Free list management
  - B-Tree search/insert (table trees use C++, index trees still pure Go)
  - Prefetch worker pool
  - Balance logic coordination

**Migration Priority**: HIGH  
**Estimated Effort**: 3-4 days  
**Action**: Complete C++ B-Tree for index trees, move manager orchestration

---

#### 3. **QP - Query Processing**
- **C++**: `src/core/QP/parser.cpp`, `tokenizer.cpp`, `binder.cpp`
- **Go**: `internal/QP/parser.go` (585 LOC), `internal/QP/analyzer.go`
- **Issue**: Go AST types still defined in Go:
  - `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`
  - `Expr`, `TableRef`, `Join`, `CTEClause`, `WindowDef`
  - Type inference logic
  - Normalization

**Migration Priority**: MEDIUM  
**Estimated Effort**: 4-5 days  
**Action**: Expose C++ AST via CGO, or keep Go types as pure data structures

---

#### 4. **CG - Code Generation**
- **C++**: `src/core/CG/bytecode_compiler.cpp`, `optimizer.cpp`
- **Go**: `internal/CG/compiler.go`, `internal/CG/bytecode_compiler.go`
- **Issue**: Go still has:
  - Compiler orchestration
  - Expression compilation
  - Plan cache coordination
  - Optimizer pipeline

**Migration Priority**: MEDIUM  
**Estimated Effort**: 2-3 days  

---

#### 5. **TM - Transaction Management**
- **C++**: `src/core/TM/transaction.cpp`
- **Go**: `internal/TM/transaction.go`, `internal/TM/mvcc.go`, `internal/TM/lock.go`
- **Issue**: Go still handles:
  - MVCC snapshot isolation
  - Lock table management
  - Deadlock detection
  - WAL coordination

**Migration Priority**: MEDIUM  
**Estimated Effort**: 2-3 days  

---

#### 6. **IS - Information Schema**
- **C++**: `src/core/IS/schema.cpp`
- **Go**: `internal/IS/information_schema.go`, `internal/IS/schema_cache.go`
- **Issue**: Go virtual table implementation for:
  - `information_schema.tables`
  - `information_schema.columns`
  - Schema extraction and parsing

**Migration Priority**: LOW  
**Estimated Effort**: 1-2 days  

---

### 📋 Go-Only Files (Need C++ Implementation)

These files have no C++ counterpart yet:

#### VM Layer (`internal/VM/`)
```
instruction.go          - Instruction type (CGO wrapper exists)
opcodes.go              - Opcode definitions
program.go              - Program structure
registers.go            - Register array
cursor.go               - Cursor management
compare.go              - Comparison functions
datetime.go             - Date/time functions
string_funcs.go         - String functions
type_conv.go            - Type conversion
hash.go                 - Hash functions
aggregate_funcs.go      - Aggregate functions
string_pool.go          - String pooling
result_cache.go         - Query result cache
row_eval.go             - Row evaluation
subquery_cache.go       - Subquery caching
```

#### DS Layer (`internal/DS/`)
```
arena.go                - Memory arena allocator
backup.go               - Incremental backup
balance.go              - Page balancing (C++ exists, Go coord)
bloom_filter.go         - Bloom filter
cache.go                - Page cache
cell.go                 - B-Tree cell encoding
column_vector.go        - Columnar vectors
compression.go          - Compression (LZ4/ZSTD)
encoding.go             - Varint, encoding utils
freelist.go             - Free list manager
index_engine.go         - Index management
mmap.go                 - Memory-mapped I/O
parallel.go             - Parallel query execution
prefetch.go             - Async prefetch
roaring_bitmap.go       - Roaring bitmap indexes
skip_list.go            - Skip list index
slab.go                 - Slab allocator
value.go                - SQL value handling
vtab.go                 - Virtual tables
worker_pool.go          - Worker pool
```

#### QP Layer (`internal/QP/`)
```
analyzer.go             - Query analyzer
binder.go               - Name binding
dag.go                  - DAG optimization
normalize.go            - Query normalization
optimizer.go            - Query optimizer
parse_cache.go          - Parse result cache
parser_alter.go         - ALTER TABLE parser
parser_create.go        - CREATE parser
parser_dml.go           - DML parser
parser_expr.go          - Expression parser
parser_select.go        - SELECT parser
parser_txn.go           - Transaction parser
tokenizer.go            - SQL tokenizer
topn.go                 - Top-N optimization
type_infer.go           - Type inference
```

#### CG Layer (`internal/CG/`)
```
compiler.go             - Main compiler
bytecode_compiler.go    - Bytecode generation
expr_compiler.go        - Expression compilation
direct_compiler.go      - Direct execution path
optimizer.go            - Bytecode optimizer
plan_cache.go           - Query plan cache
stmt_cache.go           - Prepared statement cache
expr.go                 - Expression types
```

#### TM Layer (`internal/TM/`)
```
transaction.go          - Transaction coordinator
mvcc.go                 - MVCC engine
lock.go                 - Lock manager
isolation.go            - Isolation levels
wal.go                  - WAL coordination
```

#### IS Layer (`internal/IS/`)
```
information_schema.go   - INFORMATION_SCHEMA views
schema_cache.go         - Schema caching
schema_extractor.go     - Schema extraction
schema_parser.go        - Schema parsing
registry.go             - Virtual table registry
```

#### PB Layer (`internal/PB/`)
```
file.go                 - File abstraction
vfs_memory.go           - In-memory VFS
vfs_unix.go             - Unix VFS
```

#### SF Layer (`internal/SF/`)
```
log.go                  - Logging
errors/                 - Error handling
opt/                    - Optimizations
util/                   - Utilities
vfs/                    - VFS utilities
```

#### cgo/ Layer (`internal/cgo/`) - ✅ Target Architecture
```
db_cgo.go               - ✅ Model: DB handle wrapper
exec_cgo.go             - ✅ Model: Exec binding
rows_cgo.go             - ✅ Model: Row iteration
stmt_cgo.go             - ✅ Model: Prepared statements
tx_cgo.go               - ✅ Model: Transaction binding
schema_cgo.go           - ✅ Model: Schema introspection
backup_cgo.go           - ✅ Model: Backup operations
errors.go               - ✅ Model: Error conversion
```

---

## Migration Strategy

### Phase 1: Complete VM Migration (Week 1-2)
**Goal**: Move all VM orchestration to C++

1. **VM Execution Loop** (`internal/VM/engine.go`)
   - Move to `src/core/VM/vm_execute.cpp`
   - Expose `svdb_vm_execute(stmt, result)` in C API
   - Go becomes: `C.svdb_vm_execute(h)` wrapper

2. **Register Management** (`internal/VM/registers.go`)
   - Already in C++: `src/core/VM/registers.cpp`
   - Remove Go duplicate, use C++ via CGO

3. **Cursor Array** (`internal/VM/cursor.go`)
   - Already in C++: `src/core/VM/cursor.cpp`
   - Remove Go duplicate

4. **Result Collection**
   - Move to `src/core/VM/result_cache.cpp`
   - Expose via C API

**Deliverable**: Go `VM` struct reduced to handle + error wrapper

---

### Phase 2: Complete DS Migration (Week 3-4)
**Goal**: Move all storage logic to C++

1. **PageManager** (`internal/DS/manager.go`)
   - Already in C++: `src/core/DS/manager.cpp`
   - Move header read/write, free list coordination

2. **BTree Index Trees** (`internal/DS/btree.go`)
   - Extend `src/core/DS/btree.cpp` for index page types
   - Fix page type mismatch (Go: 0x02/0x0a, C++: 0x0a/0x02)
   - Remove Go B-Tree entirely

3. **Prefetch/Worker Pool** (`internal/DS/prefetch.go`)
   - Move to `src/core/DS/async_prefetch.cpp`
   - Use std::async or thread pool

4. **Value Encoding** (`internal/DS/value.go`)
   - Already in C++: `src/core/DS/value.cpp`
   - Remove Go duplicate

**Deliverable**: Go `PageManager` and `BTree` become C++ handle wrappers

---

### Phase 3: Complete QP Migration (Week 5-6)
**Goal**: Move query processing to C++

1. **AST Types** (`internal/QP/parser.go`)
   - Option A: Expose C++ AST via CGO (complex)
   - Option B: Keep Go types as pure data (simpler)
   - Recommendation: Option B for now

2. **Tokenizer** (`internal/QP/tokenizer.go`)
   - Already in C++: `src/core/QP/tokenizer.cpp`
   - Go wrapper calls C++ tokenizer

3. **Parser/Binder** (`internal/QP/*.go`)
   - Already in C++: `src/core/QP/parser.cpp`, `binder.cpp`
   - Go orchestrates C++ calls

4. **Optimizer** (`internal/QP/optimizer.go`)
   - Move to `src/core/QP/optimizer.cpp`
   - Expose `svdb_optimize(query)` in C API

**Deliverable**: Go QP becomes orchestration layer calling C++

---

### Phase 4: Complete CG Migration (Week 7)
**Goal**: Move code generation to C++

1. **Compiler** (`internal/CG/compiler.go`)
   - Already in C++: `src/core/CG/compiler.cpp`
   - Go calls `svdb_compile(query)` → returns bytecode

2. **Optimizer** (`internal/CG/optimizer.go`)
   - Already in C++: `src/core/CG/optimizer.cpp`
   - Peephole optimization, dead-code elimination

3. **Plan Cache** (`internal/CG/plan_cache.go`)
   - Already in C++: `src/core/CG/plan_cache.cpp`
   - Expose via C API

**Deliverable**: Go CG becomes thin wrapper

---

### Phase 5: Complete TM Migration (Week 8)
**Goal**: Move transaction management to C++

1. **MVCC** (`internal/TM/mvcc.go`)
   - Move to `src/core/TM/mvcc.cpp`
   - Snapshot isolation, version chains

2. **Lock Manager** (`internal/TM/lock.go`)
   - Move to `src/core/TM/lock_table.cpp`
   - Deadlock detection, wait-for graph

3. **WAL Coordination** (`internal/TM/wal.go`)
   - Already in C++: `src/core/DS/wal.cpp`
   - Coordinate via C API

**Deliverable**: Go TM becomes handle wrapper

---

### Phase 6: Cleanup & Optimization (Week 9-10)
**Goal**: Final cleanup, performance tuning

1. **Remove Go Duplicates**
   - Delete Go files with C++ equivalents
   - Update imports

2. **CGO Optimization**
   - Minimize CGO calls (batch operations)
   - Zero-copy data transfer
   - Pin memory for CGO

3. **Testing**
   - Verify all tests pass
   - Benchmark performance
   - Fix regressions

**Deliverable**: Production-ready C++ engine with Go bindings

---

## File-by-File Migration Checklist

### VM Layer
- [ ] `internal/VM/engine.go` → `src/core/VM/vm_execute.cpp`
- [ ] `internal/VM/exec.go` → `src/core/VM/exec.cpp` (already exists, remove Go)
- [ ] `internal/VM/cursor.go` → `src/core/VM/cursor.cpp` (already exists, remove Go)
- [ ] `internal/VM/registers.go` → `src/core/VM/registers.cpp` (already exists, remove Go)
- [ ] `internal/VM/program.go` → `src/core/VM/program.cpp` (already exists, remove Go)
- [ ] `internal/VM/opcodes.go` → `src/core/VM/opcodes.cpp` (already exists, remove Go)
- [ ] `internal/VM/instruction.go` → `src/core/VM/instruction.cpp` (already exists, remove Go)
- [ ] `internal/VM/compare.go` → `src/core/VM/compare.cpp` (already exists, remove Go)
- [ ] `internal/VM/datetime.go` → `src/core/VM/datetime.cpp` (already exists, remove Go)
- [ ] `internal/VM/string_funcs.go` → `src/core/VM/string_funcs.cpp` (already exists, remove Go)
- [ ] `internal/VM/type_conv.go` → `src/core/VM/type_conv.cpp` (already exists, remove Go)
- [ ] `internal/VM/hash.go` → `src/core/VM/hash.cpp` (already exists, remove Go)
- [ ] `internal/VM/aggregate_funcs.go` → `src/core/VM/aggregate.cpp` (already exists, remove Go)
- [ ] `internal/VM/string_pool.go` → `src/core/VM/string_pool.cpp` (already exists, remove Go)
- [ ] `internal/VM/result_cache.go` → `src/core/VM/result_cache.cpp`
- [ ] `internal/VM/row_eval.go` → `src/core/VM/row_eval.cpp`
- [ ] `internal/VM/subquery_cache.go` → `src/core/VM/subquery_cache.cpp`

### DS Layer
- [ ] `internal/DS/manager.go` → `src/core/DS/manager.cpp` (already exists, remove Go)
- [ ] `internal/DS/btree.go` → `src/core/DS/btree.cpp` (already exists, remove Go)
- [ ] `internal/DS/btree_cursor.go` → `src/core/DS/btree_cursor.cpp` (already exists, remove Go)
- [ ] `internal/DS/page.go` → `src/core/DS/page.cpp` (already exists, remove Go)
- [ ] `internal/DS/freelist.go` → `src/core/DS/freelist.cpp` (already exists, remove Go)
- [ ] `internal/DS/balance.go` → `src/core/DS/balance.cpp` (already exists, remove Go)
- [ ] `internal/DS/wal.go` → `src/core/DS/wal.cpp` (already exists, remove Go)
- [ ] `internal/DS/overflow.go` → `src/core/DS/overflow.cpp` (already exists, remove Go)
- [ ] `internal/DS/cache.go` → `src/core/DS/cache.cpp` (already exists, remove Go)
- [ ] `internal/DS/value.go` → `src/core/DS/value.cpp` (already exists, remove Go)
- [ ] `internal/DS/cell.go` → `src/core/DS/cell.cpp` (already exists, remove Go)
- [ ] `internal/DS/encoding.go` → `src/core/DS/varint.cpp` (already exists, remove Go)
- [ ] `internal/DS/columnar.go` → `src/core/DS/columnar.cpp` (already exists, remove Go)
- [ ] `internal/DS/row_store.go` → `src/core/DS/row_store.cpp` (already exists, remove Go)
- [ ] `internal/DS/hybrid_store.go` → `src/core/DS/hybrid_store.cpp` (already exists, remove Go)
- [ ] `internal/DS/compression.go` → `src/core/DS/compression.cpp` (already exists, remove Go)
- [ ] `internal/DS/bloom_filter.go` → `src/core/DS/bloom_filter.cpp` (already exists, remove Go)
- [ ] `internal/DS/roaring_bitmap.go` → `src/core/DS/roaring.cpp` (already exists, remove Go)
- [ ] `internal/DS/skip_list.go` → `src/core/DS/skip_list.cpp` (already exists, remove Go)
- [ ] `internal/DS/column_vector.go` → `src/core/DS/column_vector.cpp` (already exists, remove Go)
- [ ] `internal/DS/arena.go` → `src/core/DS/arena.cpp` (NEW)
- [ ] `internal/DS/backup.go` → `src/core/DS/backup.cpp` (NEW)
- [ ] `internal/DS/mmap.go` → `src/core/DS/mmap.cpp` (NEW)
- [ ] `internal/DS/parallel.go` → `src/core/DS/parallel.cpp` (NEW)
- [ ] `internal/DS/prefetch.go` → `src/core/DS/prefetch.cpp` (NEW)
- [ ] `internal/DS/slab.go` → `src/core/DS/slab.cpp` (NEW)
- [ ] `internal/DS/vtab.go` → `src/core/DS/vtab.cpp` (NEW)
- [ ] `internal/DS/worker_pool.go` → `src/core/DS/worker_pool.cpp` (NEW)
- [ ] `internal/DS/index_engine.go` → `src/core/DS/index_engine.cpp` (NEW)

### QP Layer
- [ ] `internal/QP/parser.go` → `src/core/QP/parser.cpp` (already exists, keep Go types)
- [ ] `internal/QP/tokenizer.go` → `src/core/QP/tokenizer.cpp` (already exists, remove Go)
- [ ] `internal/QP/analyzer.go` → `src/core/QP/analyzer.cpp` (already exists, remove Go)
- [ ] `internal/QP/binder.go` → `src/core/QP/binder.cpp` (already exists, remove Go)
- [ ] `internal/QP/normalize.go` → `src/core/QP/normalize.cpp` (already exists, remove Go)
- [ ] `internal/QP/type_infer.go` → `src/core/QP/type_infer.cpp` (already exists, remove Go)
- [ ] `internal/QP/dag.go` → `src/core/QP/dag.cpp` (already exists, remove Go)
- [ ] `internal/QP/optimizer.go` → `src/core/QP/optimizer.cpp` (NEW)
- [ ] `internal/QP/parse_cache.go` → `src/core/QP/parse_cache.cpp` (NEW)
- [ ] `internal/QP/parser_*.go` → `src/core/QP/parser_*.cpp` (already exists, remove Go)
- [ ] `internal/QP/topn.go` → `src/core/QP/topn.cpp` (NEW)

### CG Layer
- [ ] `internal/CG/compiler.go` → `src/core/CG/compiler.cpp` (already exists, remove Go)
- [ ] `internal/CG/bytecode_compiler.go` → `src/core/CG/bytecode_compiler.cpp` (already exists, remove Go)
- [ ] `internal/CG/expr_compiler.go` → `src/core/CG/expr_compiler.cpp` (already exists, remove Go)
- [ ] `internal/CG/direct_compiler.go` → `src/core/CG/direct_compiler.cpp` (already exists, remove Go)
- [ ] `internal/CG/optimizer.go` → `src/core/CG/optimizer.cpp` (already exists, remove Go)
- [ ] `internal/CG/plan_cache.go` → `src/core/CG/plan_cache.cpp` (already exists, remove Go)
- [ ] `internal/CG/stmt_cache.go` → `src/core/CG/stmt_cache.cpp` (NEW)
- [ ] `internal/CG/expr.go` → `src/core/CG/expr.h` (already exists, remove Go)

### TM Layer
- [ ] `internal/TM/transaction.go` → `src/core/TM/transaction.cpp` (already exists, remove Go)
- [ ] `internal/TM/mvcc.go` → `src/core/TM/mvcc.cpp` (NEW)
- [ ] `internal/TM/lock.go` → `src/core/TM/lock_table.cpp` (NEW)
- [ ] `internal/TM/isolation.go` → `src/core/TM/isolation.cpp` (NEW)
- [ ] `internal/TM/wal.go` → `src/core/DS/wal.cpp` (coordinate, remove Go)

### IS Layer
- [ ] `internal/IS/information_schema.go` → `src/core/IS/information_schema.cpp` (NEW)
- [ ] `internal/IS/schema_cache.go` → `src/core/IS/schema_cache.cpp` (NEW)
- [ ] `internal/IS/schema_extractor.go` → `src/core/IS/schema_extractor.cpp` (NEW)
- [ ] `internal/IS/schema_parser.go` → `src/core/IS/schema_parser.cpp` (NEW)
- [ ] `internal/IS/registry.go` → `src/core/IS/registry.cpp` (NEW)

### PB Layer
- [ ] `internal/PB/file.go` → `src/core/PB/file.cpp` (already exists, remove Go)
- [ ] `internal/PB/vfs_memory.go` → `src/core/PB/vfs_memory.cpp` (already exists, remove Go)
- [ ] `internal/PB/vfs_unix.go` → `src/core/PB/vfs_unix.cpp` (already exists, remove Go)

### SF Layer
- [ ] `internal/SF/log.go` → `src/core/SF/log.cpp` (NEW)
- [ ] `internal/SF/errors/` → `src/core/SF/errors/` (NEW)
- [ ] `internal/SF/opt/` → `src/core/SF/opt.cpp` (already exists, remove Go)
- [ ] `internal/SF/util/` → `src/core/SF/util.cpp` (NEW)
- [ ] `internal/SF/vfs/` → `src/core/PB/` (merge)

---

## C API Extensions Needed

Extend `src/core/svdb/svdb.h` with:

```c
/* VM Execution */
svdb_code_t svdb_vm_execute(svdb_stmt_t *stmt, svdb_rows_t **rows);

/* Query Optimization */
svdb_code_t svdb_optimize(svdb_db_t *db, const char *sql, svdb_rows_t **plan);

/* Transaction Management */
svdb_code_t svdb_mvcc_snapshot(svdb_tx_t *tx);
svdb_code_t svdb_lock_table(svdb_tx_t *tx, const char *table, int mode);

/* Schema Introspection */
svdb_code_t svdb_schema_info(svdb_db_t *db, const char *table, svdb_rows_t **rows);

/* Backup */
svdb_code_t svdb_backup_incremental(svdb_db_t *src, const char *dest_path);

/* Performance */
svdb_code_t svdb_stats(svdb_db_t *db, svdb_rows_t **stats);
```

---

## Performance Goals

### Current Bottlenecks
1. **Go callback overhead**: ~260ns per CGO call (target: <10ns)
2. **Memory allocation**: Go GC pauses (target: zero-GC query execution)
3. **Data copying**: Go ↔ C++ string/bytes copy (target: zero-copy)

### Target Metrics
- **SELECT**: Match SQLite performance (within 10%)
- **INSERT**: 2× faster than SQLite (batch INSERT)
- **AGGREGATE**: 3× faster than SQLite (C++ vectorized)
- **JOIN**: Match SQLite (hash JOIN in C++)

---

## Testing Strategy

### Unit Tests
- Port Go tests to C++ (GoogleTest)
- Keep Go tests as integration tests

### Integration Tests
- `tests/Benchmark/` - Performance regression tests
- `tests/Compatibility/` - SQL compatibility tests
- `tests/Fuzz/` - Fuzz testing (Go + C++)

### Benchmark Suite
```bash
# Current benchmarks (keep)
./build.sh -b

# Add C++ micro-benchmarks
cd src/core && cmake --build . --target benchmarks
```

---

## Risk Assessment

### High Risk
1. **Index B-Tree page type mismatch** - Go and C++ use different byte values
   - Mitigation: Standardize on SQLite canonical format in C++
   
2. **MVCC concurrency** - Complex interaction with Go runtime
   - Mitigation: Implement entirely in C++, expose via C API

3. **Memory management** - Go GC vs C++ manual
   - Mitigation: Use arena allocators, RAII in C++

### Medium Risk
1. **AST type exposure** - Complex C++ → Go type mapping
   - Mitigation: Keep Go types as pure data structures

2. **Error handling** - Go errors vs C++ exceptions
   - Mitigation: C API returns error codes, no exceptions across boundary

### Low Risk
1. **Build system** - CMake + Go build
   - Mitigation: `build.sh` already handles this

2. **Testing coverage** - Ensure C++ code is tested
   - Mitigation: Port Go tests to C++

---

## Timeline Summary

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| **Phase 1: VM** | Week 1-2 | C++ VM execution, Go handle wrapper |
| **Phase 2: DS** | Week 3-4 | C++ storage, no Go B-Tree/Manager |
| **Phase 3: QP** | Week 5-6 | C++ parser/optimizer, Go orchestration |
| **Phase 4: CG** | Week 7 | C++ compiler, Go wrapper |
| **Phase 5: TM** | Week 8 | C++ MVCC/locks, Go handle |
| **Phase 6: Cleanup** | Week 9-10 | Testing, optimization, docs |

**Total**: 10 weeks (2.5 months)

---

## Conclusion

The sqlvibe project is **70% complete** in migrating from Go to C++. The C++ infrastructure is solid, with a unified C public API that enables zero-overhead integration.

### Immediate Next Steps
1. **Week 1**: Move VM execution loop to C++ (`vm_execute.cpp`)
2. **Week 2**: Remove Go VM duplicate code
3. **Week 3**: Fix index B-Tree page type mismatch
4. **Week 4**: Complete DS migration

### Long-term Vision
- **Go**: Pure type-mapping layer (~400 LOC)
- **C++**: Full engine implementation
- **C API**: Stable ABI for language bindings (Python, Rust, Java)
- **Performance**: 3-5× faster than SQLite for analytical workloads

---

**Document Version**: 1.0  
**Last Updated**: 2026-03-03  
**Maintainer**: sqlvibe team
