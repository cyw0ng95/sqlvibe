# CGO Migration Plan: internal/ → src/core/

This document outlines the plan to migrate performance-critical code from Go (`internal/`) to C++ (`src/core/`) to maximize performance through native execution, SIMD optimizations, and reduced GC pressure.

**See also**: [`MIGRATION_STATUS.md`](MIGRATION_STATUS.md) for detailed per-file migration status.

---

## Current State (Updated: 2026-03-01)

```
src/core/
├── CG/       → compiler, expr_compiler, optimizer, plan_cache, register (5 modules)
├── cgo/      → hash_join (1 module)
├── DS/       → btree (partial), btree_cursor, cell, compression, roaring, simd, value, varint (8 modules)
├── IS/       → schema (1 module)
├── PB/       → vfs (1 module)
├── QP/       → tokenizer (1 module)
├── SF/       → opt (1 module)
├── TM/       → transaction (1 module)
├── VM/       → aggregate, aggregate_engine, compare, datetime, dispatch, expr_engine,
│               expr_eval, hash, registers, sort, string_funcs, string_pool,
│               type_conv, vm_dispatch (14 modules)
└── wrapper/  → invoke_chain_wrapper (1 module)
```

**Total:** 34 C++ modules compiled into `libsvdb`

### Migration Statistics

| Status | Count | Description |
|--------|-------|-------------|
| ✅ Complete | 36 | C++ implementation exists and is used by Go wrapper |
| 🟡 Partial | 1 | C++ exists but incomplete (`btree.cpp` - insert/delete are placeholders) |
| ❌ Pending | 51 | Need C++ implementation |
| 📋 Go-Only | 51 | Should remain in Go (orchestration, tests, Go-specific) |

**Total files analyzed**: 139

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Go Application Layer                    │
│  (internal/ - orchestration, tests, high-level logic)       │
├─────────────────────────────────────────────────────────────┤
│                      CGO Binding Layer                       │
│  (thin wrappers calling into C++ via cgo)                   │
├─────────────────────────────────────────────────────────────┤
│                    libsvdb (C++ Core)                        │
│  ┌─────┬─────┬─────┬─────┬─────────┐                        │
│  │ DS  │ VM  │ QP  │ CG  │ wrapper │                        │
│  └─────┴─────┴─────┴─────┴─────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Phase 1: DS (Data Storage) - High Priority

**Goal:** Migrate core storage primitives to C++

| Go File | C++ Target | Priority | Complexity | Notes |
|---------|-----------|----------|------------|-------|
| `page.go` | `page.cpp/h` | High | Medium | Page management, 4K pages |
| `encoding.go` | `encoding.cpp/h` | High | Low | Varint, zigzag, already partially in C++ |
| `freelist.go` | `freelist.cpp/h` | High | Medium | Free list management |
| `bloom_filter.go` | `bloom_filter.cpp/h` | Medium | Low | Already has C++ impl in roaring |
| `skip_list.go` | `skip_list.cpp/h` | Medium | Medium | Skip list data structure |
| `cache.go` | `cache.cpp/h` | Medium | Medium | LRU/ARC cache |
| `value.go` | `value.cpp/h` | High | High | SQL value handling, type system |
| `column_store.go` | `columnar.cpp/h` | Medium | High | Columnar storage engine |
| `row_store.go` | `row_store.cpp/h` | Medium | High | Row storage engine |
| `wal.go` | `wal.cpp/h` | High | High | Write-ahead logging |

**CGO wrappers to remove after migration:**
- `btree.go` → uses C++ btree (keep as thin wrapper or remove)
- `cell.go` → uses C++ cell (keep as thin wrapper or remove)
- `compression.go` → uses C++ compression (keep as thin wrapper or remove)
- `roaring_bitmap.go` → uses C++ roaring (keep as thin wrapper or remove)

---

## Phase 2: VM (Virtual Machine) - High Priority

**Goal:** Migrate bytecode VM execution engine to C++

| Go File | C++ Target | Priority | Complexity | Notes |
|---------|-----------|----------|------------|-------|
| `bytecode_vm.go` | `bytecode_vm.cpp/h` | High | High | Main VM loop |
| `bytecode_handlers.go` | `opcodes.cpp/h` | High | High | Opcode implementations |
| `exec.go` | `exec.cpp/h` | High | High | Query execution engine |
| `cursor.go` | `cursor.cpp/h` | High | Medium | Cursor management |
| `registers.go` | `registers.cpp/h` | Medium | Low | Register file |
| `program.go` | `program.cpp/h` | Medium | Medium | Bytecode program |
| `instruction.go` | `instruction.cpp/h` | Medium | Low | Instruction format |
| `string_pool.go` | `string_pool.cpp/h` | Low | Medium | String interning |
| `subquery_cache.go` | `subquery_cache.cpp/h` | Low | Medium | Subquery caching |
| `result_cache.go` | `result_cache.cpp/h` | Low | Medium | Result caching |

**CGO wrappers already calling C++ (to be removed):**
- `aggregate_funcs.go` → already calls C++
- `compare.go` → already calls C++
- `datetime.go` → already calls C++
- `hash.go` → already calls C++
- `sort.go` → already calls C++
- `string_funcs.go` → already calls C++
- `type_conv.go` → already calls C++

---

## Phase 3: QP (Query Processing) - Medium Priority

**Goal:** Migrate SQL parser and optimizer to C++

| Go File | C++ Target | Priority | Complexity | Notes |
|---------|-----------|----------|------------|-------|
| `parser.go` | `parser.cpp/h` | High | High | Main SQL parser |
| `parser_select.go` | `parser_select.cpp/h` | High | High | SELECT parsing |
| `parser_expr.go` | `parser_expr.cpp/h` | High | High | Expression parsing |
| `parser_dml.go` | `parser_dml.cpp/h` | Medium | Medium | INSERT/UPDATE/DELETE |
| `parser_create.go` | `parser_ddl.cpp/h` | Medium | Medium | CREATE statements |
| `binder.go` | `binder.cpp/h` | High | High | Name resolution |
| `analyzer.go` | `analyzer.cpp/h` | High | High | Semantic analysis |
| `optimizer.go` | `optimizer.cpp/h` | High | High | Query optimization |
| `dag.go` | `dag.cpp/h` | Medium | Medium | Query DAG representation |
| `normalize.go` | `normalize.cpp/h` | Medium | Medium | Query normalization |
| `topn.go` | `topn.cpp/h` | Low | Low | TOP-N optimization |

**Keep in Go:**
- `tokenizer.go` → pure Go tokenizer (FastTokenCount uses C++)
- `parser_test.go`, `*_test.go` → tests stay in Go

---

## Phase 4: CG (Code Generation) - Medium Priority

**Goal:** Complete bytecode compiler migration

| Go File | C++ Target | Priority | Complexity | Notes |
|---------|-----------|----------|------------|-------|
| `bytecode_compiler.go` | `bytecode_compiler.cpp/h` | High | High | Bytecode generation |
| `bytecode_expr.go` | `bytecode_expr.cpp/h` | High | High | Expression compilation |
| `direct_compiler.go` | `direct_compiler.cpp/h` | Medium | Medium | Direct compilation |
| `expr.go` | `expr.cpp/h` | Medium | Medium | Expression handling |
| `stmt_cache.go` | `stmt_cache.cpp/h` | Low | Low | Statement caching |

**Already in C++:**
- `compiler.cpp` ✓
- `expr_compiler.cpp` ✓
- `optimizer.cpp` ✓
- `plan_cache.cpp` ✓

---

## Phase 5: Clean Up - Low Priority

**Goal:** Remove redundant Go code, thin wrappers

| Action | Target | Notes |
|--------|--------|-------|
| Remove thin CGO wrappers | `internal/VM/*.go` | Keep only if Go-specific logic |
| Merge Go tests | `internal/*/*_test.go` | Move to `src/core/*/tests/` or keep |
| Remove duplicate implementations | Various | Ensure single source of truth |

---

## Migration Pattern

For each module:

1. **Create C++ implementation** in `src/core/<subsystem>/`
2. **Add to CMakeLists.txt** in `src/core/<subsystem>/CMakeLists.txt`
3. **Update main CMakeLists.txt** to include new sources in `libsvdb`
4. **Update Go CGO bindings** to call new C++ code
5. **Test** - ensure all tests pass
6. **Remove Go implementation** (or keep as thin wrapper if needed)
7. **Clean up** - remove old files

### Example: Migrating `value.go`

```cpp
// src/core/DS/value.cpp
#include "value.h"
#include <cstdint>
#include <string>

namespace svdb {

Value::Value() : type_(ValueType::Null), int_val_(0) {}

Value::Value(int64_t val) : type_(ValueType::Integer), int_val_(val) {}

Value::Value(const std::string& val) 
    : type_(ValueType::Text), text_val_(val) {}

ValueType Value::type() const { return type_; }

int64_t Value::as_int() const { return int_val_; }

std::string Value::as_text() const { return text_val_; }

} // namespace svdb
```

```go
// internal/DS/value.go - thin wrapper (or remove entirely)
package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "value.h"
*/
import "C"

// Value wraps C++ Value
type Value struct {
    ptr *C.svdb_Value
}
```

---

## Recommended Migration Order

| Order | Module | Rationale |
|-------|--------|-----------|
| 1 | `DS/value.cpp` | Foundation for all SQL value handling |
| 2 | `DS/page.cpp`, `DS/freelist.cpp` | Storage primitives |
| 3 | `VM/bytecode_vm.cpp`, `VM/opcodes.cpp` | Core VM execution |
| 4 | `VM/exec.cpp` | Query execution engine |
| 5 | `QP/parser.cpp` | SQL parsing |
| 6 | `QP/binder.cpp`, `QP/analyzer.cpp` | Semantic analysis |
| 7 | `CG/bytecode_compiler.cpp` | Code generation |

---

## Benefits

| Benefit | Description |
|---------|-------------|
| **Performance** | C++ execution is faster than Go for tight loops, better instruction-level optimization |
| **SIMD** | Better SIMD optimization in C++ with explicit intrinsics (AVX2, AVX-512) |
| **Memory** | Lower GC pressure, manual memory management for hot paths |
| **Unified** | Single codebase for performance-critical code, no Go/C++ duplication |
| **Maintainability** | Clear separation - C++ for performance-critical, Go for orchestration |

---

## Tracking Progress

### Completed (36 modules)

**Infrastructure:**
- [x] Move `internal/*/cgo/` → `src/core/`
- [x] Unified `libsvdb` library
- [x] Update all CGO paths
- [x] Update build system (CMakeLists.txt, build.sh)
- [x] All tests passing with CGO enabled

**DS (Data Storage) - 6 modules:**
- [x] `cell.cpp` — Cell encoding/decoding
- [x] `compression.cpp` — LZ4/ZSTD compression
- [x] `roaring.cpp` — Roaring bitmap (removed ~300 lines of pure Go)
- [x] `simd.cpp` — SIMD utilities
- [x] `value.cpp` — SQL value type with comparison
- [x] `varint.cpp` — Varint encoding/decoding

**VM (Virtual Machine) - 14 modules:**
- [x] `aggregate.cpp` / `aggregate_engine.cpp` — Aggregate functions
- [x] `compare.cpp` — Comparison functions
- [x] `datetime.cpp` — DateTime functions
- [x] `dispatch.cpp` / `vm_dispatch.cpp` — Bytecode dispatch
- [x] `expr_engine.cpp` / `expr_eval.cpp` — Expression evaluation
- [x] `hash.cpp` — xxHash functions
- [x] `hash_join.cpp` — Hash JOIN (in `cgo/`)
- [x] `registers.cpp` — Register allocator
- [x] `sort.cpp` — Sort utilities
- [x] `string_funcs.cpp` — String functions
- [x] `string_pool.cpp` — String interning pool
- [x] `type_conv.cpp` — Type conversion

**QP (Query Processing) - 1 module:**
- [x] `tokenizer.cpp` — Fast token counting

**CG (Code Generation) - 5 modules:**
- [x] `compiler.cpp` — Bytecode compiler
- [x] `expr_compiler.cpp` — Expression compiler
- [x] `optimizer.cpp` — Query optimizer
- [x] `plan_cache.cpp` — Plan caching
- [x] `register.cpp` — Register management

**TM (Transaction Management) - 1 module:**
- [x] `transaction.cpp` — Transaction handling

**PB (Platform Bridges) - 1 module:**
- [x] `vfs.cpp` — VFS implementation

**SF (System Framework) - 1 module:**
- [x] `opt.cpp` — CPU optimization detection

**IS (Information Schema) - 1 module:**
- [x] `schema.cpp` — Schema handling

**Wrapper - 1 module:**
- [x] `invoke_chain_wrapper.cpp` — Phase 4 invoke chain wrappers

### Partially Complete (1 module)

**DS:**
- [🟡] `btree.cpp` — Search implemented, **insert/delete are placeholders**

### Pending (51 modules)

**High Priority (DS Layer - storage foundation):**
- [ ] `btree.cpp` — Complete insert/delete with page split logic
- [ ] `btree_cursor.cpp` — Cursor traversal
- [ ] `page.cpp` — Page management
- [ ] `freelist.cpp` — Free list management
- [ ] `manager.cpp` — PageManager implementation
- [ ] `wal.cpp` — Write-ahead logging
- [ ] `balance.cpp` — B-Tree balancing
- [ ] `overflow.cpp` — Overflow page chains

**Medium Priority (VM Layer — execution engine):**
- [ ] `bytecode_vm.cpp` — Main VM loop
- [ ] `opcodes.cpp` — Bytecode opcode implementations
- [ ] `exec.cpp` — Query execution engine
- [ ] `cursor.cpp` — Cursor management
- [ ] `program.cpp` — Bytecode program
- [ ] `instruction.cpp` — Instruction format

**Medium Priority (QP Layer — parsing):**
- [ ] `parser.cpp` — Main SQL parser
- [ ] `parser_select.cpp` — SELECT parsing
- [ ] `parser_expr.cpp` — Expression parsing
- [ ] `parser_dml.cpp` — DML parsing
- [ ] `parser_ddl.cpp` — DDL parsing
- [ ] `binder.cpp` — Name resolution
- [ ] `analyzer.cpp` — Semantic analysis
- [ ] `dag.cpp` — Query DAG
- [ ] `normalize.cpp` — Query normalization
- [ ] `type_infer.cpp` — Type inference

**Low Priority (CG Layer — compilation):**
- [ ] `bytecode_compiler.cpp` — Bytecode generation
- [ ] `direct_compiler.cpp` — Direct compilation

**DS Layer (storage engines):**
- [ ] `columnar.cpp` — Columnar storage
- [ ] `row_store.cpp` — Row storage
- [ ] `cache.cpp` — LRU/ARC cache
- [ ] `skip_list.cpp` — Skip list

### Go-Only (51 modules - should NOT migrate)

These files implement Go-specific patterns or orchestration logic:

**Orchestration:**
- `hybrid_store.go`, `engine.go`, `compiler.go`, `transaction.go` (TM)

**Go Concurrency:**
- `worker_pool.go`, `parallel.go`, `prefetch.go`

**Memory Management (Go-optimized):**
- `arena.go`, `slab.go`, `cache.go` (DS)

**Virtual Tables:**
- `vtab.go`, `vtab_cursor.go`, `vtab_module.go`

**Information Schema (all):**
- `columns_view.go`, `constraints_view.go`, `information_schema.go`, etc.

**Error Handling & Logging:**
- `SF/errors/*.go`, `SF/log.go`

**Utilities:**
- `SF/util/assert.go`, `SF/util/pool.go`

---

## Notes

- **Tests remain in Go** - Keep Go test files for validation
- **Incremental migration** - Migrate one module at a time, verify tests pass
- **Backward compatibility** - Maintain Go API during migration
- **Performance benchmarks** - Measure before/after for each migration
