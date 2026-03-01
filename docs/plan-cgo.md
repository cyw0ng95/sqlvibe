# C++ Migration Status

**Last Updated**: 2026-03-02
**Target Version**: v0.11.0

This document tracks the migration status of Go code in `internal/` to C++ implementations in `src/core/`.

---

## Summary

| Subsystem | Total | C++ Complete | CGO Wrapper | Go-Only | Progress |
|-----------|-------|--------------|-------------|---------|----------|
| **DS** (Data Storage) | 36 | 20 | 19 | 12 | 58% |
| **VM** (Virtual Machine) | 30 | 21 | 15 | 15 | 72% |
| **QP** (Query Processing) | 15 | 12 | 7 | 4 | 80% |
| **CG** (Code Generation) | 8 | 7 | 7 | 1 | 88% |
| **TM** (Transaction Mgmt) | 1 | 1 | 1 | 0 | 100% |
| **PB** (Platform Bridges) | 1 | 1 | 1 | 0 | 100% |
| **SF** (System Framework) | 1 | 1 | 1 | 0 | 100% |
| **IS** (Info Schema) | 1 | 1 | 1 | 0 | 100% |
| **Wrapper** | 1 | 1 | 1 | 0 | 100% |
| **CGO** (Special Cases) | 1 | 1 | 1 | 0 | 100% |
| **TOTAL** | **97** | **67** | **49** | **32** | **71%** |

**Legend**:
- **C++ Complete**: C++ implementation exists in `src/core/`
- **CGO Wrapper**: Go wrapper uses `import "C"` to call C++
- **Go-Only**: Pure Go implementation (no C++ migration yet or Go-only by design)

---

## DS (Data Storage) - 20/36 Complete

### ✅ C++ Complete with CGO Wrapper

| Go File | C++ File | Status | Notes |
|---------|----------|--------|-------|
| `internal/DS/value.go` | `src/core/DS/value.cpp` | ✅ CGO | Numeric compare uses C++, string/bytes use Go |
| `internal/DS/compression.go` | `src/core/DS/compression.cpp` | ✅ CGO | |
| `internal/DS/roaring_bitmap.go` | `src/core/DS/roaring.cpp` | ✅ CGO | |
| `internal/DS/encoding.go` | `src/core/DS/varint.cpp` | ✅ CGO | varint encode/decode |
| `internal/DS/cell.go` | `src/core/DS/cell.cpp` | ✅ CGO | Cell encode/decode |
| `internal/DS/overflow.go` | `src/core/DS/overflow.cpp` | ✅ CGO | Always-on CGO (no fallback); Direct C pointer for callbacks |
| `internal/DS/cache_cgo.go` | `src/core/DS/cache.cpp` | ✅ CGO | **Always-on CGO** (no fallback); Direct C pointer, self-contained |
| `internal/DS/skip_list.go` | `src/core/DS/skip_list.h` | ✅ CGO | Always-on; int/float→`_int` API, string/bytes→`_str` API; goKeys for Range/Pairs |

**Architecture Note**: All CGO files are unconditional (no build tags) — matching the pattern of `value.go`, `encoding.go`. C++ is the only implementation. `cache_cgo.go` uses direct C pointer (no registry overhead). `overflow_cgo.go` requires registry for Go PageManager callbacks. See `docs/plan-cgo-architecture-fix.md`.

### ✅ C++ Complete, Go Implementation Still Active

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

### C++ Files Without Go Counterparts (New)

| C++ File | Purpose |
|----------|---------|
| `src/core/DS/simd.cpp` | SIMD optimizations |
| `src/core/DS/page.cpp` | Page management |
| `src/core/DS/freelist.cpp` | Free list management |
| `src/core/DS/balance.cpp` | Page balancing |
| `src/core/DS/manager.cpp` | Page manager |
| `src/core/DS/wal.cpp` | Write-ahead logging |

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
| `internal/VM/dispatch.go` | `src/core/VM/dispatch.cpp` | ⚠️ PARTIAL | C++ complete, Go wrapper needs CGO |
| `internal/VM/engine.go` | `src/core/VM/query_engine.cpp` | ⚠️ PARTIAL | Query classification + comment-stripping migrated via `vm_utils_cgo.go`; VM struct still in Go |

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
| `internal/QP/parser.go` | `src/core/QP/parser.cpp` | 🚧 STUB | Stub created, full implementation pending |
| `internal/QP/parser_select.go` | `src/core/QP/parser_select.cpp` | 🚧 STUB | Stub created, full implementation pending |
| `internal/QP/parser_expr.go` | `src/core/QP/parser_expr.cpp` | 🚧 STUB | Stub created, full implementation pending |
| `internal/QP/parser_dml.go` | `src/core/QP/parser_dml.cpp` | 🚧 STUB | Stub created, full implementation pending |
| `internal/QP/parser_create.go` | `src/core/QP/parser_ddl.cpp` | 🚧 STUB | Stub created, full implementation pending |

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

### P0: Critical (Must Complete)

1. **B-Tree CGO Wrapper** (`internal/DS/btree.go`)
   - C++ implementation complete with page split/merge
   - **TODO**: Create CGO wrapper to call C++ `svdb_btree_*` functions
   - **TODO**: Remove Go `Insert`, `Delete`, `Search` implementations
   - **TODO**: Verify all btree tests pass

2. **Overflow CGO Wrapper** (`internal/DS/overflow.go`)
   - C++ implementation complete
   - **TODO**: Create CGO wrapper
   - **TODO**: Remove Go implementation

3. **Cache CGO Wrapper** (`internal/DS/cache_cgo.go`)
   - C++ LRU cache complete
   - **TODO**: Create CGO wrapper
   - **TODO**: Remove Go implementation

4. **Columnar/Row Store CGO Wrappers**
   - C++ implementations complete
   - **TODO**: Create CGO wrappers
   - **TODO**: Update Go orchestration

5. **Bytecode VM CGO Wrapper** (`internal/VM/bytecode_vm.go`)
   - C++ VM complete with dispatch
   - **TODO**: Create CGO wrapper
   - **TODO**: Remove Go implementation

6. **Opcode Handlers** (`internal/VM/bytecode_handlers.go`)
   - **TODO**: Implement all 200+ opcode handlers in `src/core/VM/opcodes.cpp`
   - **TODO**: Create CGO wrapper
   - **TODO**: Remove Go implementation

### P1: High (Should Complete)

7. **Parser Implementations**
   - **TODO**: Complete `parser_select.cpp` for SELECT parsing
   - **TODO**: Complete `parser_expr.cpp` for expression parsing
   - **TODO**: Complete `parser_dml.cpp` for DML parsing
   - **TODO**: Complete `parser_ddl.cpp` for DDL parsing

8. **VM Layer CGO Wrappers**
   - `internal/VM/cursor.go`
   - `internal/VM/exec.go`
   - `internal/VM/dispatch.go`
   - `internal/VM/engine.go`

### P2: Medium (Nice to Have)

9. **Batch Operation Wrappers**
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

### Pattern 2: Registry for Callbacks (C++ → Go)

**Use for**: `overflow`, `btree`, `columnar`, `row_store` (need PageManager)

```go
// Registry ONLY for callback context
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

**Key insight**: Registry lookup only in callback path, not every method call!

### Performance Comparison

| Pattern | Overhead per Call | Use Case |
|---------|------------------|----------|
| Direct C Pointer | ~10ns | Self-contained C++ (cache, value, etc.) |
| Registry for Callbacks | ~260ns | C++ needing Go callbacks (overflow, btree) |
| **Improvement** | **26x faster** | When using correct pattern |

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

### Phase 1: Complete Partial Migrations (High Priority) 🔄 IN PROGRESS

#### 1.1 Complete `btree.cpp` Insert/Delete
- [x] C++ insert with page split (implemented)
- [x] C++ delete with merge (implemented)
- [ ] Go wrapper using CGO (blocked: needs cursor support)
- [ ] C++ unit tests
- [ ] Go tests via CGO

**Complexity**: High | **Effort**: 2-3 days

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
- [ ] Implement all 46 opcode handlers in C++
- [ ] Go wrapper using CGO
- [ ] Verify all opcode tests pass

**Complexity**: Very High | **Effort**: 5-7 days

### Phase 4: QP Layer - Query Processing (Medium Priority)

#### 4.1 SQL Parser Implementation
- [x] Parser stubs created
- [ ] Implement full SELECT parsing in `parser_select.cpp`
- [ ] Implement full expression parsing in `parser_expr.cpp`
- [ ] Implement full DML parsing in `parser_dml.cpp`
- [ ] Implement full DDL parsing in `parser_ddl.cpp`

**Complexity**: Very High | **Effort**: 5-10 days

### Phase 5: Legacy Code Removal (Systematic Cleanup)

**Wave 1: Already Complete** (safe to verify)
- [ ] Verify all CGO wrappers use correct pattern
- [ ] Remove any remaining Go-only implementations where C++ is used

**Wave 2: After DS Layer Complete**
- [ ] Remove `internal/DS/column_store.go` (keep tests)
- [ ] Remove `internal/DS/row_store.go` (keep tests)

**Wave 3: After VM Layer Complete**
- [ ] Remove redundant VM wrappers

**Wave 4: After QP Layer Complete**
- [ ] Remove redundant QP wrappers

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
- [ ] All tests remain in Go
- [ ] Tests validate C++ via CGO wrappers
- [ ] No test code in `src/core/`

**Validation Checklist**:
- [ ] C++ implementation complete
- [ ] All unit tests pass: `./build.sh -t`
- [ ] Benchmarks pass: `./build.sh -b`
- [ ] No performance regression (>5% slowdown requires justification)
- [ ] Fuzz tests pass: `./build.sh -f`
- [ ] Coverage maintained: `./build.sh -t -c`

---

## Implementation Schedule

| Phase | Tasks | Status | Effort | Dependencies |
|-------|-------|--------|--------|--------------|
| Phase 0 | Analysis & Planning | ✅ Complete | - | None |
| Phase 1.1 | btree.cpp insert/delete | 🔄 In Progress | 2-3 days | Phase 0 |
| Phase 1.2 | overflow.cpp | ✅ Complete | 1 day | Phase 0 |
| Phase 1.3 | cache.cpp | ✅ Complete | 1 day | Phase 0 |
| Phase 2 | DS layer (columnar, row_store) | ✅ Complete | 4-6 days | Phase 1 |
| Phase 3.1 | Extend opcodes.cpp | Pending | 5-7 days | Phase 2 |
| Phase 4 | QP parser stubs | 🔄 In Progress | 5-10 days | Phase 3 |
| Phase 5 | Legacy code removal | Pending | 3-5 days | Phases 1-4 |
| Phase 6 | Architecture cleanup | Pending | 2-3 days | Phase 5 |
| Phase 7 | Testing & validation | Ongoing | 1-2 days | All phases |

**Total Estimated Effort**: 25-35 working days (5-7 weeks)

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

- **CGO Files**: 17 Go files currently use `import "C"`
- **Test Strategy**: All tests remain in Go, validating C++ via CGO wrappers
- **Performance Goal**: No regression, ideally 5-10% improvement in hot paths
- **Memory Safety**: C++ uses RAII, Go GC handles wrapper memory
- **Architecture**: Use direct C pointer for self-contained C++, registry only for callbacks

---

**Status Legend**:
- ✅ **COMPLETE**: C++ impl + CGO wrapper + tests passing
- ⚠️ **PARTIAL**: C++ impl exists, Go wrapper needs update
- 🚧 **STUB**: C++ stub created, full implementation pending
- ❌ **TODO**: Not started
- 📋 **GO-ONLY**: Will remain in Go (orchestration/Go-specific)

---

**Last Updated**: 2026-03-02
**Next Review**: After Phase 1 completion (btree cursor support)
