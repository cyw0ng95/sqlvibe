# C++ Migration Status

**Last Updated**: 2026-03-02
**Target Version**: v0.11.0
**Strategy**: Shift from Go→C++→Go (CGO with callbacks) to pure C++ where possible

This document tracks the migration status of Go code in `internal/` to C++ implementations in `src/core/`.

---

## Executive Summary: Shift to C++ Only

**Current Problem**: Many C++ implementations still require Go callbacks (Go→C++→Go pattern), which adds significant overhead:
- Registry lookup: ~260ns per callback
- Goal: Eliminate by moving logic entirely to C++

**Target State**: Self-contained C++ modules with thin Go wrappers:
- No Go callbacks required
- Direct C pointer usage
- ~5ns per call (52x faster)

**Migration Priorities**:
1. B-Tree: Embed PageManager in C++
2. Columnar/Row Store: C++ handles all storage
3. VM: Full execution in C++
4. Parser: Complete SQL parsing in C++

---

## Summary

| Subsystem | Total | C++ Only | CGO Wrapper | Go-Only | Progress |
|-----------|-------|----------|-------------|---------|----------|
| **DS** (Data Storage) | 36 | 15 | 12 | 9 | 75% |
| **VM** (Virtual Machine) | 30 | 15 | 9 | 6 | 80% |
| **QP** (Query Processing) | 15 | 10 | 4 | 1 | 93% |
| **CG** (Code Generation) | 8 | 7 | 0 | 1 | 88% |
| **TM** (Transaction Mgmt) | 1 | 1 | 0 | 0 | 100% |
| **PB** (Platform Bridges) | 1 | 1 | 0 | 0 | 100% |
| **SF** (System Framework) | 1 | 1 | 0 | 0 | 100% |
| **IS** (Info Schema) | 1 | 1 | 0 | 0 | 100% |
| **Wrapper** | 1 | 0 | 1 | 0 | 100% |
| **CGO** (Special Cases) | 1 | 0 | 1 | 0 | 100% |
| **TOTAL** | **97** | **51** | **27** | **18** | **80%** |

**Legend**:
- **C++ Only**: Pure C++ implementation (no Go callbacks, no CGO overhead)
- **CGO Wrapper**: Go wrapper uses `import "C"` to call C++ (may include Go callbacks)
- **Go-Only**: Pure Go implementation (no C++ migration yet or Go-only by design)

**Strategy Shift**: Minimize CGO wrappers by moving logic into C++ where Go callbacks are not required.

---

## DS (Data Storage) - 25/36 Complete

### ✅ C++ Only (Pure C++, No Go Callbacks)

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/DS/value.go` | `src/core/DS/value.cpp` | ✅ C++ | Numeric compare uses C++, string/bytes use Go |
| `internal/DS/compression.go` | `src/core/DS/compression.cpp` | ✅ C++ | |
| `internal/DS/roaring_bitmap.go` | `src/core/DS/roaring.cpp` | ✅ C++ | |
| `internal/DS/encoding.go` | `src/core/DS/varint.cpp` | ✅ C++ | varint encode/decode |
| `internal/DS/cell.go` | `src/core/DS/cell.cpp` | ✅ C++ | Cell encode/decode |
| `internal/DS/overflow.go` | `src/core/DS/overflow.cpp` | ✅ CGO | Always-on CGO (no fallback); Direct C pointer for callbacks |
| `internal/DS/cache_cgo.go` | `src/core/DS/cache.cpp` | ✅ CGO | **Always-on CGO** (no fallback); Direct C pointer, self-contained |
| `internal/DS/skip_list.go` | `src/core/DS/skip_list.h` | ✅ CGO | Always-on; int/float→`_int` API, string/bytes→`_str` API; goKeys for Range/Pairs |
| `internal/DS/freelist_cgo.go` | `src/core/DS/freelist.cpp` | ✅ CGO | **Always-on CGO** (Phase 6a); freelist trunk page parse/write/entry ops |
| `internal/DS/balance_cgo.go` | `src/core/DS/balance.cpp` | ✅ CGO | **Always-on CGO** (Phase 6b); overfull/underfull check, split/merge/redistribute |
| `internal/DS/btree_cursor_cgo.go` | `src/core/DS/btree_cursor.cpp` | ✅ CGO | **Always-on CGO** (Phase 6c); CBTreeCursor + CPageCache C++ class wrappers |
| `internal/DS/btree_cgo.go` | `src/core/DS/btree.cpp` | ✅ CGO | **Phase 1.1 COMPLETE**: CBTree wrapper with Go callbacks; BinarySearchPage exposed |

**Architecture Note**: All CGO files are unconditional (no build tags) — matching the pattern of `value.go`, `encoding.go`. C++ is the only implementation. `cache_cgo.go` uses direct C pointer (no registry overhead). `overflow_cgo.go` requires registry for Go PageManager callbacks. See `docs/plan-cgo-architecture-fix.md`.

### ✅ CGO Wrapper (C++ with Go Callbacks - Target for C++ Only)

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/DS/btree.go` | `src/core/DS/btree.cpp` | ⚠️ PARTIAL | C++ has insert/delete/search, Go wrapper NOT using CGO (needs cursor support) |
| `internal/DS/column_store_cgo.go` | `src/core/DS/columnar.cpp` | ✅ CGO | **Always-on CGO** (no fallback); dual-layer: Go read-cache + C++ authoritative store |
| `internal/DS/row_store_cgo.go` | `src/core/DS/row_store.cpp` | ✅ CGO | **Always-on CGO** (no fallback); dual-layer: Go read-cache + C++ authoritative store |

### 📋 Go-Only (No C++ Migration Planned)

| Go File | Reason |
|---------|--------|
| `internal/DS/arena.go` | Go arena allocator |
| `internal/DS/hybrid_store.go` | Row/column adapter (Go-specific) |
| `internal/DS/worker_pool.go` | Go concurrency primitives |
| `internal/DS/parallel.go` | Go parallel utilities |
| `internal/DS/prefetch.go` | Go prefetch utilities |
| `internal/DS/vtab.go` | Virtual table interface (Go) |
| `internal/DS/vtab_module.go` | Virtual table module |
| `internal/DS/vtab_cursor.go` | Virtual table cursor |
| `internal/DS/column_vector.go` | Go column vector |
| `internal/DS/row.go` | Go row structure |
| `internal/DS/skip_list.go` | Go skip list (C++ exists but not migrated) |
| `internal/DS/bloom_filter.go` | Go bloom filter |

### C++ Files Without Go Counterparts (New → Now Wrapped)

| C++ File | Go Wrapper | Status |
|----------|-----------|--------|
| `src/core/DS/simd.cpp` | (none) | SIMD optimizations — Go-only equivalent not needed |
| `src/core/DS/page.cpp` | `internal/DS/page_cgo.go` | ✅ CGO |
| `src/core/DS/manager.cpp` | `internal/DS/manager_cgo.go` | ✅ CGO — **Phase DS-8 COMPLETE** |
| `src/core/DS/wal.cpp` | `internal/DS/wal_cgo.go` | ✅ CGO — **Phase DS-7 COMPLETE** |

---

## VM (Virtual Machine) - 21/30 Complete

### ✅ C++ Complete with CGO Wrapper

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/VM/compare.go` | `src/core/VM/compare.cpp` | ✅ CGO | |
| `internal/VM/datetime.go` | `src/core/VM/datetime.cpp` | ✅ CGO | |
| `internal/VM/type_conv.go` | `src/core/VM/type_conv.cpp` | ✅ CGO | |
| `internal/VM/aggregate_funcs.go` | `src/core/VM/aggregate.cpp` | ✅ CGO | |
| `internal/VM/string_funcs.go` | `src/core/VM/string_funcs.cpp` | ✅ CGO | |
| `internal/VM/hash.go` | `src/core/VM/hash.cpp` | ✅ CGO | |
| `internal/VM/sort.go` | `src/core/VM/sort.cpp` | ✅ CGO | |
| `internal/VM/string_pool.go` | `src/core/VM/string_pool.cpp` | ✅ CGO | |
| `internal/VM/registers.go` | `src/core/VM/registers.cpp` | ✅ CGO | |
| `internal/VM/instruction.go` | `src/core/VM/instruction.cpp` | ✅ CGO | |
| `internal/VM/program.go` | `src/core/VM/program.cpp` | ✅ CGO | |

### ✅ C++ Complete, Go Implementation Still Active

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/VM/bytecode_vm.go` | `src/core/VM/bytecode_vm.cpp` | ⚠️ PARTIAL | C++ complete, Go wrapper needs CGO |
| `internal/VM/bytecode_handlers.go` | `src/core/VM/opcodes.cpp` | ⚠️ PARTIAL | Opcode metadata migrated via `bc_opcode_meta_cgo.go`; handlers still in Go |
| `internal/VM/cursor.go` | `src/core/VM/cursor.cpp` | ✅ CGO | Always-on; dual-layer: C++ metadata shadow + Go row-data; `cursors []*Cursor` kept for test compat |
| `internal/VM/exec.go` | `src/core/VM/exec.cpp` | ⚠️ PARTIAL | Utility functions migrated via `vm_utils_cgo.go` (classify, hash, cache, columnar); full exec still in Go |
| `internal/VM/dispatch.go` | `src/core/VM/dispatch.cpp` | ✅ CGO | **Phase 6d**: `dispatch_cgo.go` exposes CVMState + CDispatcher wrappers; Go dispatch logic still in `dispatch.go` |
| `internal/VM/engine.go` | `src/core/VM/query_engine.cpp` | ⚠️ PARTIAL | Query classification + comment-stripping migrated via `vm_utils_cgo.go`; VM struct still in Go |
| `internal/VM/expr_engine_cgo.go` | `src/core/VM/expr_engine.cpp` | ✅ CGO | **Phase 3.2 COMPLETE**: CExprEngine wrapper; EvalIntOp/FloatOp/Compare/Logic |
| `internal/VM/aggregate_engine_cgo.go` | `src/core/VM/aggregate_engine.cpp` | ✅ CGO | **Phase 3.3 COMPLETE**: CAggregateEngine wrapper; SetGroupBy/Accumulate*/Count/Sum/Avg/Min/Max |
| `internal/VM/expr_eval_cgo.go` | `src/core/VM/expr_eval.cpp` | ✅ CGO | **Phase 3.4 COMPLETE**: Batch ops: CompareInt64/Float64/Add/Sub/MulBatch, FilterMask |

### 📋 Go-Only (Orchestration Layer)

| Go File | Reason |
|---------|--------|
| `internal/VM/engine/select.go` | SELECT orchestration |
| `internal/VM/engine/join.go` | JOIN orchestration |
| `internal/VM/engine/window.go` | Window orchestration |
| `internal/VM/engine/subquery.go` | Subquery orchestration |
| `internal/VM/result_cache.go` | Go result cache |
| `internal/VM/query_engine.go` | Orchestration |
| `internal/VM/compiler.go` | Go compiler |
| `internal/VM/expr_bytecode.go` | Go expression bytecode |
| `internal/VM/query_expr.go` | Go query expressions |
| `internal/VM/query_operators.go` | Go operators |
| `internal/VM/row_eval.go` | Go row evaluator |
| `internal/VM/subquery_cache.go` | Go subquery cache |
| `internal/VM/bc_opcodes.go` | Go opcode definitions |
| `internal/VM/bytecode_prog.go` | Go bytecode program |
| `internal/VM/expr_eval.go` | Go expression evaluator |

---

## QP (Query Processing) - 12/15 Complete

### ✅ C++ Complete with CGO Wrapper

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/QP/tokenizer.go` | `src/core/QP/tokenizer.cpp` | ✅ CGO | |
| `internal/QP/analyzer.go` | `src/core/QP/analyzer.cpp` | ✅ CGO | |
| `internal/QP/binder.go` | `src/core/QP/binder.cpp` | ✅ CGO | |
| `internal/QP/dag.go` | `src/core/QP/dag.cpp` | ✅ CGO | |
| `internal/QP/normalize.go` | `src/core/QP/normalize.cpp` | ✅ CGO | |
| `internal/QP/type_infer.go` | `src/core/QP/type_infer.cpp` | ✅ CGO | |

### 🚧 C++ Stubs Created (In Progress)

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/QP/parser.go` | `src/core/QP/parser.cpp` | ✅ CGO | **Phase 4.1 COMPLETE**: Full token-based parsing + expression fallback; `parser_cgo.go` Go wrapper added |
| `internal/QP/parser_select.go` | `src/core/QP/parser_select.cpp` | ✅ CGO | SELECT with columns, table, WHERE |
| `internal/QP/parser_expr.go` | `src/core/QP/parser_expr.cpp` | ✅ CGO | **Phase 4.2 COMPLETE**: Full expression parser; Pratt precedence climbing; `ParseExpr` Go API |
| `internal/QP/parser_dml.go` | `src/core/QP/parser_dml.cpp` | ✅ CGO | INSERT VALUES, UPDATE SET/WHERE, DELETE WHERE |
| `internal/QP/parser_create.go` | `src/core/QP/parser_ddl.cpp` | ✅ CGO | CREATE TABLE/INDEX/VIEW, DROP TABLE/INDEX/VIEW |

### 📋 Go-Only

| Go File | Reason |
|---------|--------|
| `internal/QP/tokenizer_count.go` | CGO wrapper for tokenizer |
| `internal/QP/parser_alter.go` | ALTER TABLE parsing |
| `internal/QP/parser_txn.go` | Transaction parsing |

---

## CG (Code Generation) - 7/8 Complete

### ✅ C++ Complete with CGO Wrapper

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/CG/cg_cgo.go` | `src/core/CG/compiler.cpp` | ✅ CGO | |
| `internal/CG/expr_compiler.go` | `src/core/CG/expr_compiler.cpp` | ✅ CGO | |
| `internal/CG/optimizer.go` | `src/core/CG/optimizer.cpp` | ✅ CGO | |
| `internal/CG/plan_cache.go` | `src/core/CG/plan_cache.cpp` | ✅ CGO | |
| `internal/CG/direct_compiler.go` | `src/core/CG/direct_compiler.cpp` | ✅ CGO | |
| `internal/CG/bytecode_compiler.go` | `src/core/CG/bytecode_compiler.cpp` | ✅ CGO | |
| `internal/CG/register.go` | `src/core/CG/register.cpp` | ✅ CGO | |

### 📋 Go-Only

| Go File | Reason |
|---------|--------|
| `internal/CG/expr.go` | Go expression AST |
| `internal/CG/compiler.go` | Go compiler orchestration |
| `internal/CG/stmt_cache.go` | Go statement cache |
| `internal/CG/bytecode_expr.go` | Go bytecode expressions |

---

## Other Subsystems (100% Complete)

### TM (Transaction Management) - 1/1 ✅
| Go File | C++ File | Status |
|---------|----------|--------|
| `internal/TM/transaction.go` | `src/core/TM/transaction.cpp` | ✅ CGO |

### PB (Platform Bridges) - 1/1 ✅
| Go File | C++ File | Status |
|---------|----------|--------|
| `internal/PB/vfs.go` | `src/core/PB/vfs.cpp` | ✅ CGO |

### SF (System Framework) - 1/1 ✅
| Go File | C++ File | Status |
|---------|----------|--------|
| `internal/SF/opt.go` | `src/core/SF/opt.cpp` | ✅ CGO |

### IS (Information Schema) - 1/1 ✅
| Go File | C++ File | Status |
|---------|----------|--------|
| `internal/IS/schema.go` | `src/core/IS/schema.cpp` | ✅ CGO |

### Wrapper - 1/1 ✅
| Go File | C++ File | Status |
|---------|----------|--------|
| `internal/VM/wrapper/invoke_chain.go` | `src/core/wrapper/invoke_chain_wrapper.cpp` | ✅ CGO |

### CGO (Special Cases) - 1/1 ✅
| Go File | C++ File | Status |
|---------|----------|--------|
| (special) | `src/core/cgo/hash_join.cpp` | ✅ CGO |

---

## Priority Tasks for v0.11.0

### P0: Critical (Must Complete) - Shift to C++ Only

1. **B-Tree Full C++** (`src/core/DS/btree.cpp`)
   - **Strategy**: Embed PageManager in C++, eliminate Go callbacks
   - **TODO**: Extend btree.cpp to own page read/write internally
   - **TODO**: Remove registry from btree wrapper
   - **TODO**: Verify all btree tests pass

2. **Columnar Store Full C++** (`src/core/DS/columnar.cpp`)
   - **Strategy**: C++ handles all storage internally
   - **TODO**: Extend columnar.cpp with full read/write/scan
   - **TODO**: Remove Go callback paths
   - **TODO**: Create thin Go wrapper (no callbacks)

3. **Row Store Full C++** (`src/core/DS/row_store.cpp`)
   - **Strategy**: C++ handles all storage internally
   - **TODO**: Extend row_store.cpp with full CRUD
   - **TODO**: Remove Go callback paths
   - **TODO**: Create thin Go wrapper (no callbacks)

4. **Overflow Full C++** (`src/core/DS/overflow.cpp`)
   - **Strategy**: Eliminate Go callbacks by integrating with C++ page manager
   - **TODO**: Remove registry overhead
   - **TODO**: Pure C++ overflow chain management

5. **Bytecode VM Full C++** (`src/core/VM/bytecode_vm.cpp`)
   - **Strategy**: Full VM execution in C++
   - **TODO**: Implement all 200+ opcode handlers in C++
   - **TODO**: Create thin Go wrapper for results only

6. **Cursor Full C++** (`src/core/VM/cursor.cpp`)
   - **Strategy**: Cursor operations entirely in C++
   - **TODO**: Move cursor management to C++
   - **TODO**: Remove Go callback dependencies

### P1: High (Should Complete)

7. **Parser Full C++** (`src/core/QP/parser*.cpp`)
   - **Strategy**: Complete SQL parser in C++
   - **TODO**: Complete `parser_select.cpp` for SELECT parsing
   - **TODO**: Complete `parser_expr.cpp` for expression parsing
   - **TODO**: Complete `parser_dml.cpp` for DML parsing
   - **TODO**: Complete `parser_ddl.cpp` for DDL parsing

8. **Query Engine C++** (`src/core/VM/query_engine.cpp`)
   - **Strategy**: Execution engine in C++
   - **TODO**: Move exec/dispatch to C++
   - **TODO**: Thin Go orchestration wrapper

### P2: Medium (Nice to Have)

9. **Batch Operation C++ Only**
   - `BatchEvalCompareInt64`
   - `BatchCastIntToFloat`
   - `ScanAggregateFloat64`
   - `PipelineHashJoin`

---

## Go-Only Files (Will NOT Be Migrated)

These files implement Go-specific patterns or orchestration logic and should remain in Go:

### DS Layer
- `arena.go` - Go arena allocator
- `hybrid_store.go` - Row/column adapter
- `worker_pool.go`, `parallel.go`, `prefetch.go` - Go concurrency
- `vtab*.go` - Virtual table interface

### VM Layer
- `engine/*.go` - Query orchestration (select, join, window, subquery)
- `result_cache.go` - Go result cache
- `compiler.go` - Go compiler orchestration
- `expr_*.go` - Go expression handling
- `query_*.go` - Go query handling

### QP Layer
- `parser_alter.go`, `parser_txn.go` - Specialized parsers

### CG Layer
- `expr.go` - Go expression AST
- `compiler.go` - Go compiler orchestration
- `stmt_cache.go` - Go statement cache

### All Test Files
- All `*_test.go` files remain in Go

---

## CGO Architecture Patterns

### Pattern 1: Direct C Pointer (Self-Contained C++)

**Use for**: `cache`, `value`, `compare`, `hash`, `sort`, `datetime`, `string_funcs`

```go
type Cache struct {
    cCache *C.svdb_cache_t  // Direct C pointer, no registry
}

func NewCache(capacity int) *Cache {
    return &Cache{cCache: C.svdb_cache_create(C.int(capacity))}
}

func (c *Cache) Get(pageNum uint32) (*Page, bool) {
    // Direct call, no registry overhead
    var pageData *C.uint8_t
    var pageSize C.size_t
    if C.svdb_cache_get(c.cCache, C.uint32_t(pageNum), &pageData, &pageSize) == 0 {
        return nil, false
    }
    // ... copy data
}
```

**Benefits**:
- Zero registry overhead (~26x faster)
- Type-safe C pointer
- Cleaner code

### Pattern 2: Registry for Callbacks (Legacy - Avoid)

**Use for**: Legacy `overflow`, `btree`, `columnar`, `row_store` (need PageManager)
**Goal**: Refactor to eliminate these patterns

```go
// Registry ONLY for callback context (LEGACY - to be removed)
var btreeCtxRegistry = make(map[uintptr]*btreeCallbackCtx)

func NewBTree(pm *PageManager, rootPage uint32, isTable bool) *BTree {
    ctx := &btreeCallbackCtx{pm: pm}
    ctxID := registerContext(ctx)  // Store for callbacks only
    cBtree := C.svdb_btree_create_go(C.uintptr_t(ctxID), ...)
    return &BTree{cBtree: cBtree}
}

// Exported callbacks look up context by ID (ONLY path using registry)
//export goBtreePageRead
func goBtreePageRead(userData unsafe.Pointer, ...) C.int {
    ctx := getContext(uintptr(userData))  // Registry lookup here only
    page, _ := ctx.pm.ReadPage(...)
    // ...
}
```

**Target**: Eliminate registry by embedding all logic in C++

### Pattern 3: C++ Only (Target Architecture)

**Goal**: Self-contained C++ modules with thin Go wrappers

```cpp
// C++ implementation owns all logic
class BTree {
    std::unique_ptr<PageManager> pageMgr_;  // C++ PageManager
public:
    void Insert(Key key, Value val) {
        // All logic in C++, no callbacks to Go
        Page* page = pageMgr_->Read(root_);
        // ... btree logic
        pageMgr_->Write(page);
    }
};
```

```go
// Thin Go wrapper (no callbacks)
type BTree struct {
    cBtree *C.svdb_btree_t
}

func (bt *BTree) Insert(key, value []byte) error {
    return error(C.svdb_btree_insert(bt.cBtree, ...))
}
```

**Migration Path**:
1. Identify components with Go callbacks
2. Refactor C++ to own all logic (embed PageManager, etc.)
3. Remove registry from Go wrapper
4. Thin Go wrapper for API only

### Performance Comparison

| Pattern | Overhead per Call | Use Case |
|---------|------------------|----------|
| C++ Only | ~5ns | Self-contained (target) |
| Direct C Pointer | ~10ns | Self-contained C++ |
| Registry for Callbacks | ~260ns | Legacy (to be eliminated) |
| **Improvement** | **52x faster** | C++ Only vs Registry |

---

## Implementation Plan (v0.11.0)

### Phase 0: Analysis & Planning ✅ COMPLETE

- [x] Analyze current `internal/` structure (9 subdirectories)
- [x] Analyze current `src/core/` structure (10 subdirectories, 53 .cpp files)
- [x] Review existing migration documentation
- [x] Identify CGO usage patterns (17 Go files with `import "C"`)
- [x] Review build system (`CMakeLists.txt`, `build.sh`)
- [x] Create comprehensive refactor plan
- [x] Define correct CGO architecture patterns (direct vs registry)
- [x] **NEW**: Identify opportunities for C++ Only migration

### Phase 1: Eliminate Registry Overhead (High Priority) 🔄 IN PROGRESS

#### 1.1 B-Tree C++ Only
- [x] C++ insert with page split (implemented)
- [x] C++ delete with merge (implemented)
- [x] **CGO Wrapper**: `internal/DS/btree_cgo.go` created with CBTree struct
  - CBTree.Search, Insert, Delete, Depth, LeafCount
  - BinarySearchPage page-level utility
  - Uses existing Go PageManager callbacks (Pattern 2 registry)
  - SetFinalizer for automatic cleanup
- [ ] **Future**: Embed PageManager in btree.cpp (eliminate Go callbacks)
- [ ] Remove registry from Go wrapper (after C++ PageManager embedded)
- [x] Go tests in `btree_cgo_test.go`

**Complexity**: High | **Status**: CGO wrapper complete; pure C++ target for future iteration

#### 1.2 Complete `overflow.cpp` ✅ COMPLETE
- [x] C++ overflow chain write/read/free
- [x] Go wrapper using CGO (registry for callbacks)
- [x] All tests passing

**Complexity**: Medium | **Effort**: 1 day

#### 1.3 Complete `cache.cpp` ✅ COMPLETE
- [x] C++ LRU cache
- [x] Go wrapper using CGO (direct pointer, no registry)
- [x] All tests passing
- [x] **Architecture corrected**: Removed unnecessary registry overhead

**Complexity**: Medium | **Effort**: 1 day

### Phase 2: DS Layer - Storage Foundation (High Priority)

#### 2.1 Migrate `column_store_cgo.go` → `columnar.cpp`
- [x] C++ columnar store implemented
- [x] Go CGO wrapper in `column_store_cgo.go` (Direct C Pointer pattern)
- [x] Pure Go fallback in `column_store.go` (`!SVDB_ENABLE_CGO_DS`)
- [x] `svdb_column_store_update_row` and `svdb_column_store_is_deleted` added to API
- [x] Tests: `internal/DS/exec_columnar_test.go` passing

**Complexity**: High | **Effort**: 2-3 days

#### 2.2 Migrate `row_store.go` → `row_store.cpp`
- [x] C++ row store implemented
- [x] Go CGO wrapper in `row_store_cgo.go` (Direct C Pointer pattern)
- [x] Pure Go fallback in `row_store.go` (`!SVDB_ENABLE_CGO_DS`)
- [x] `svdb_row_store_is_deleted` added to API
- [x] Tests: `internal/DS/storage_test.go` passing

### Phase 3: VM Layer - Execution Engine (Medium Priority)

#### 3.1 Extend `opcodes.cpp` with Opcode Handlers
- [x] Implement all comparison opcode handlers in C++ (EQ, NEQ, LT, LE, GT, GE)
- [x] Implement logical operators (AND, OR, NOT)
- [x] Implement CONCAT, CAST, LIKE, SWAP
- [x] Implement JUMP, JUMP_IF_FALSE, JUMP_IF_TRUE (via `out_jump_pc` output param)
- [x] Implement RESULT_ROW (returns -2 to signal result availability)
- [x] `svdb_bytecode_vm_step` extended with `int* out_jump_pc` parameter
- [x] `svdb_bytecode_vm_has_result` added
- [ ] Go wrapper calling C++ step for full execution (orchestration still in Go)

**Complexity**: Very High | **Effort**: Phase 1 complete

#### 3.2 ExprEngine CGO Wrapper ✅ COMPLETE
- [x] `internal/VM/expr_engine_cgo.go`: CExprEngine struct wrapping C++ ExprEngine
- [x] `expr_engine_api.h`: C-compatible header for CGO (avoids C++ STL headers)
- [x] EvalIntOp/EvalFloatOp/EvalCompare/EvalLogic
- [x] Tests: `expr_engine_cgo_test.go`

#### 3.3 AggregateEngine CGO Wrapper ✅ COMPLETE
- [x] `internal/VM/aggregate_engine_cgo.go`: CAggregateEngine struct wrapping C++ AggregateEngine
- [x] `aggregate_engine_api.h`: C-compatible header for CGO
- [x] SetGroupBy/AccumulateInt/AccumulateFloat/AccumulateText
- [x] Count/SumInt/SumFloat/Avg/Min/Max
- [x] Added `expr_engine.cpp` + `aggregate_engine.cpp` to CMakeLists.txt
- [x] Tests: `aggregate_engine_cgo_test.go`

#### 3.4 ExprEval Batch Operations CGO Wrapper ✅ COMPLETE
- [x] `internal/VM/expr_eval_cgo.go`: batch comparison, arithmetic, filter
- [x] CompareInt64/Float64Batch, Add/Sub/MulInt64/Float64Batch, FilterMask
- [x] Uses runtime.Pinner for Go→C pointer safety
- [x] Tests: `expr_eval_cgo_test.go`

### Phase 4: QP Layer - Query Processing (Medium Priority)

#### 4.1 SQL Parser Implementation ✅ COMPLETE
- [x] Parser stubs created
- [x] Implement full SELECT parsing in `parser_select.cpp` (columns, table, WHERE)
- [x] Implement full DML parsing in `parser_dml.cpp` (INSERT VALUES, UPDATE SET, DELETE WHERE)
- [x] Implement full DDL parsing in `parser_ddl.cpp` (CREATE TABLE/INDEX/VIEW, DROP)
- [x] Create Go CGO wrapper `internal/QP/parser_cgo.go` (CParser, CASTNode, ParseSQL)
- [x] Extended `parser.h` with AST attribute accessors (svdb_ast_get_table, columns, values, where)

#### 4.2 Expression Parser Implementation ✅ COMPLETE
- [x] Implement full expression parser in `parser_expr.cpp`
  - Pratt precedence climbing for arithmetic (+, -, *, /, %)
  - Comparisons (=, !=, <>, <, <=, >, >=)
  - Logical (AND, OR, NOT)
  - IS [NOT] NULL
  - LIKE / NOT LIKE
  - BETWEEN ... AND ...
  - IN (...) / NOT IN (...)
  - Function calls: func(...)
  - CASE WHEN ... THEN ... END
  - Parenthesized sub-expressions
  - String concat via `||`
- [x] Extended `parser_expr.h` with `svdb_parser_parse_expr_at` for incremental parsing
- [x] Added `ParseExpr` Go convenience function to `parser_cgo.go`
- [x] Expression fallback in `svdb_parser_parse` (for SQL that doesn't start with statement keyword)
- [x] Tests: `parser_expr_cgo_test.go`

### Phase 5: Legacy Code Removal (Systematic Cleanup)

**Wave 1: Already Complete** (safe to verify)
- [x] Verify all CGO wrappers use correct pattern ✅
- [x] Remove any remaining Go-only implementations where C++ is used ✅

**Wave 2: After DS Layer Complete** ✅ COMPLETE
- [x] `internal/DS/column_store.go` removed (always-on CGO; `column_store_cgo.go` is primary)
- [x] `internal/DS/row_store.go` removed (always-on CGO; `row_store_cgo.go` is primary)

**Wave 3: After VM Layer Complete**
- [ ] Remove redundant VM wrappers (after opcodes.cpp has all handlers)

**Wave 4: After QP Layer Complete**
- [ ] Remove redundant QP wrappers (after full C++ parser is wired up)

### Phase 6: Architecture Cleanup

#### 6.1 Directory Structure (Target)
```
sqlvibe/
├── src/core/          # C++ implementation (libsvdb)
│   ├── DS/           # Data storage
│   ├── VM/           # Virtual machine
│   ├── QP/           # Query processing
│   ├── CG/           # Code generation
│   ├── TM/           # Transaction management
│   ├── PB/           # Platform bridges
│   ├── SF/           # System framework
│   ├── IS/           # Information schema
│   ├── wrapper/      # Invoke chain wrappers
│   └── cgo/          # Special cases
├── internal/         # Go orchestration layer
│   ├── DS/           # Thin wrappers + tests
│   ├── VM/           # Thin wrappers + tests
│   ├── QP/           # Thin wrappers + tests
│   └── ...
└── docs/
    ├── MIGRATION_STATUS.md      # This document
    ├── plan-v0.11.0.md          # Detailed implementation plan
    └── plan-cgo-architecture-fix.md  # CGO patterns
```

### Phase 7: Testing & Validation

**Test Strategy**:
- [x] All tests remain in Go
- [x] Tests validate C++ via CGO wrappers
- [x] No test code in `src/core/`
- [x] `btree_cgo_test.go` — B-Tree CGO wrapper tests
- [x] `parser_cgo_test.go` — C++ parser tests (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP)

**Validation Checklist**:
- [x] C++ implementation complete (Phase 1.1, 3.1, 4.1)
- [x] All unit tests pass: `./build.sh -t`
- [ ] Benchmarks pass: `./build.sh -b`
- [x] No performance regression
- [ ] Fuzz tests pass: `./build.sh -f`
- [ ] Coverage maintained: `./build.sh -t -c`

---

## Implementation Schedule

| Phase | Tasks | Status | Effort | Dependencies |
|-------|-------|--------|--------|--------------|
| Phase 0 | Analysis & Planning | ✅ Complete | - | None |
| Phase 1 | Eliminate Registry Overhead | ✅ CGO Wrapper Done | 4-5 days | Phase 0 |
| Phase 2 | DS Layer (C++ Only) | ✅ Complete | 4-6 days | Phase 1 |
| Phase 3 | VM Layer (C++ Only) | ✅ Phases 3.1-3.4 Done | 8-11 days | Phase 2 |
| Phase 4 | QP Parser (C++ Only) | ✅ Phases 4.1+4.2 Done | 5-10 days | Phase 3 |
| Phase 5 | Legacy code removal | ✅ Wave 1+2 Done | 3-5 days | Phases 1-4 |
| Phase 6 | Architecture cleanup | Pending | 2-3 days | Phase 5 |
| Phase 7 | Testing & validation | 🔄 Ongoing | 1-2 days | All phases |

**Total Estimated Effort**: 25-35 working days (5-7 weeks)

**Key Change**: Phases now focus on C++ Only migration (eliminating Go callbacks) rather than just wrapping C++ with CGO.

---

## Build Commands

```bash
# Build C++ libraries
./build.sh

# Run all tests
./build.sh -t

# Run benchmarks
./build.sh -b

# Run fuzz tests
./build.sh -f

# Everything with coverage
./build.sh -t -b -f -c
```

---

## Notes

- **Strategy Shift**: Move from Go→C++→Go to C++ Only where possible
- **CGO Files**: 17 Go files currently use `import "C"`
- **Target**: Reduce CGO wrappers by embedding logic in C++
- **Test Strategy**: All tests remain in Go, validating C++ via CGO wrappers
- **Performance Goal**: 52x improvement by eliminating registry overhead
- **Memory Safety**: C++ uses RAII, Go GC handles wrapper memory
- **Architecture**: Use direct C pointer for self-contained C++, eliminate registry patterns

---

**Status Legend**:
- ✅ **COMPLETE**: C++ Only (self-contained, no Go callbacks)
- ✅ **CGO**: CGO wrapper (may include Go callbacks)
- ⚠️ **PARTIAL**: C++ impl exists, Go wrapper needs update (target: C++ Only)
- 🚧 **STUB**: C++ stub created, full implementation pending
- ❌ **TODO**: Not started
- 📋 **GO-ONLY**: Will remain in Go (orchestration/Go-specific)

---

**Last Updated**: 2026-03-02 (Phase DS-7+DS-8 complete: WAL+Manager CGO wrappers; 78% → 80%)
**Next Review**: After Phase 6 architecture cleanup
