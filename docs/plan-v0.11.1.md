# Phase 6: Complete C++ Migration Plan (v0.11.1)

**Last Updated**: 2026-03-02
**Target Version**: v0.11.1
**Status**: 🚧 In Progress — Phase 6 CG_OP_* reconciliation complete, VM fallback cleanup done
**Goal**: Migrate all `internal/` and `pkg/sqlvibe/` to C++ with thin CGO wrapper

---

## Executive Summary

This document outlines the complete migration plan to transform **pkg/** and **internal/** into a fully C++ implementation with minimal Go wrapper layer.

### Vision

```
┌─────────────────────────────────────────────────────────────────┐
│                    Go Application Layer                          │
│  tests/ (Go test suite - REMAINS)                               │
│  driver/ (database/sql driver - REMAINS GO)                     │
│  pkg/sqlvibe/ (Public API - thin CGO wrapper ~500 LOC)          │
└─────────────────────────────────────────────────────────────────┘
                              │ CGO boundary (single layer)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              C++ libsvdb.so (Complete Engine ~15,000 LOC)        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Query Engine (VM orchestration)                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Storage Engine (DS - B-Tree, Columnar, Row, WAL)       │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Query Processor (QP/CG - Parser, Compiler, Optimizer)  │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Transaction Manager (TM - ACID, Locking, MVCC)         │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Current State Analysis

### File Distribution

| Layer | Directory | Go Files | LOC | C++ Status | Priority |
|-------|-----------|----------|-----|------------|----------|
| **Public API** | `pkg/sqlvibe/` | 90 | ~8,000 | ❌ 0% | P2 |
| **Driver** | `driver/` | ~7 | ~500 | ❌ 0% | **Remains Go** |
| **Query Execution** | `internal/VM/` | ~30 | ~15,000 | ✅ 80% | P0 |
| **Code Gen** | `internal/CG/` | ~8 | ~2,000 | ✅ 88% | P1 |
| **Query Processing** | `internal/QP/` | ~15 | ~4,000 | ✅ 100% | ✅ Complete |
| **Data Storage** | `internal/DS/` | ~36 | ~10,000 | ✅ 94% | P1 |
| **Transaction** | `internal/TM/` | ~1 | ~400 | ✅ 100% | ✅ Complete |
| **Platform** | `internal/PB/` | ~1 | ~200 | ✅ 100% | ✅ Complete |
| **Info Schema** | `internal/IS/` | ~1 | ~300 | ✅ 100% | ✅ Complete |
| **System** | `internal/SF/` | ~1 | ~100 | ✅ 100% | ✅ Complete |
| **TOTAL** | | **~324** | **~40,500** | **~95%** | |

### Completed C++ Migrations (v0.11.x)

✅ **Phase 1**: Query Engine Module (FilterRows, Join, Aggregate, Sort)
✅ **Phase 2**: Storage Layer (HybridStore, IndexEngine)
✅ **Phase 3**: VM Bytecode Handlers (execOpcode helper)
✅ **Test Migration**: `internal/TS/` → `tests/`
✅ **Phase 5**: VM Orchestration — all engine functions in engine.cpp + CGO wrappers in engine_cgo.go
✅ **Build Fix**: engine_api.h include path (`../SF/types.h` → `../../SF/types.h`) + C.svdb_value_t type correction

---

## Migration Phases

### ~~Phase 4: Complete DS Layer (1-2 weeks)~~ ✅ COMPLETE

#### ~~4.1: B-Tree Complete C++ Wrapper~~ ✅ COMPLETE
**Status**: C++ B-Tree implementation complete (`src/core/DS/btree.cpp`). Go wrapper uses callback pattern for PageManager integration.

#### ~~4.2: HybridStore Full C++ Migration~~ ✅ COMPLETE
**Status**: C++ HybridStore API and implementation complete with Scan, ScanWithFilter, ScanProjected.

---

### ~~Phase 5: Complete VM Orchestration~~ ✅ COMPLETE

**Status**: All engine functions implemented in `src/core/VM/engine/engine.cpp` and wrapped in `internal/VM/engine/engine_cgo.go`.

#### ~~5.1: SELECT Query Engine~~ ✅ COMPLETE
#### ~~5.2: JOIN Query Engine~~ ✅ COMPLETE
#### ~~5.3: Aggregate Engine~~ ✅ COMPLETE
#### ~~5.4: Sort Engine~~ ✅ COMPLETE
#### ~~5.5: Window Function Engine~~ ✅ COMPLETE
#### ~~5.6: Subquery Engine~~ ✅ COMPLETE

---

### Phase 6: Complete CG Layer (In Progress)

**Goal**: Finish code generation Go wrapper and reconcile opcode constants

#### 6.1: Bytecode Compiler Wrapper (Partial ✅)
**Files**: `internal/CG/cg_cgo.go`, `internal/CG/compiler.go`, `pkg/sqlvibe/vm_exec.go`, `src/core/CG/optimizer.cpp`, `src/core/CG/compiler.cpp`

**Completed (2026-03-02)**:
- Fixed `BC_RESULT_ROW = 31 → 30` in `optimizer.cpp` (was off-by-one) ✅
- Fixed `eliminateBcDeadCode` in `optimizer.cpp` — conservative default ✅
- Fixed `eliminateDeadCode` in `optimizer.cpp` — conservative default ✅
- Fixed `cgEliminateDeadCode` in `compiler.cpp` — conservative default + `p4_regs` marking ✅
- Fixed `programToJSON` in `cg_cgo.go` — handle `map[string]int` P4 ✅
- **Wired `OptimizeBytecodeInstrs` into `execBytecode`** (BytecodeVM path) ✅
- **Reconciled `CG_OP_*` constants** in `compiler.cpp` with correct `VM.OpCode` values ✅
- **Wired `CGOptimizeProgram` into `compiler.finalize()`** (legacy VM.Program path) ✅

**TODO**:
- [ ] Migrate statement cache (`stmt_cache.go`) to C++
- [ ] Migrate plan cache (`plan_cache.go`) to C++

**Expected Impact**: -400 Go LOC, +600 C++ LOC

---

### Phase 7: DS Layer Cleanup (New — 1 week)

**Goal**: Remove Go fallback code, consolidate DS C++ wrappers

#### 7.1: B-Tree Callback Removal
**Files**: `internal/DS/btree_cgo.go`, `internal/DS/btree.go`

**TODO**:
- [ ] Evaluate removing Go callback pattern — embed C++ PageManager directly
- [ ] If feasible, remove `goPageRead/Write/Allocate/Free` callbacks
- [ ] Simplify `btree_cgo.go` to direct C++ calls

**Expected Impact**: -200 Go LOC

---

#### 7.2: DS Utility Migration
**Files**: `internal/DS/cache_cgo.go`, `internal/DS/freelist_cgo.go`, `internal/DS/overflow_cgo.go`

**TODO**:
- [ ] Migrate cache operations to C++ (`src/core/DS/cache.cpp`)
- [ ] Migrate freelist operations to C++ (`src/core/DS/freelist.cpp`) ✅ exists
- [ ] Migrate overflow operations to C++ (`src/core/DS/overflow.cpp`) ✅ exists
- [ ] Consolidate CGO wrappers

**Expected Impact**: -300 Go LOC

---

#### 7.3: WAL and Persistence
**Files**: `internal/DS/wal_cgo.go`, `internal/DS/wal.go`, `internal/DS/persistence.go`

**TODO**:
- [ ] Complete WAL C++ implementation (`src/core/DS/wal.cpp`) ✅ exists
- [ ] Migrate persistence layer to C++
- [ ] Handle checkpoint operations in C++

**Expected Impact**: -400 Go LOC, +500 C++ LOC

---

### Phase 8: VM Layer Cleanup (New — 1 week)

**Goal**: Remove Go fallback code from VM layer

#### 8.1: Remove Go Engine Fallbacks
**Files**: `internal/VM/engine/*.go` (aggregate.go, join.go, select.go, sort.go, subquery.go, window.go)

**Completed (2026-03-02)**: ✅
- Removed all unused `go*` prefixed fallback functions ✅
- Simplified `GroupRows` to pure Go (removed wasted `CGroupRows` call) ✅
- Inlined `goDenseRanks` into `DenseRanks` ✅
- All tests pass ✅

**Expected Impact**: -800 Go LOC

---

#### 8.2: Bytecode VM Consolidation
**Files**: `internal/VM/bytecode_vm.go`, `internal/VM/dispatch_cgo.go`

**TODO**:
- [ ] Verify bytecode dispatch uses C++ handlers
- [ ] Remove Go fallback dispatch table
- [ ] Consolidate CGO dispatch wrapper

**Expected Impact**: -200 Go LOC

---

### Phase 9: pkg/sqlvibe Thin Wrapper (2-3 weeks)

**Goal**: Reduce public API to ~500 LOC CGO wrapper

#### 9.1: Database Operations
**Files**: `pkg/sqlvibe/database.go`, `pkg/sqlvibe/exec_state.go`

**Functions to Migrate**:
- `Open()` - database opening
- `Close()` - database closing
- `Exec()` - statement execution
- `Query()` - query execution
- `Prepare()` - statement preparation

**C++ Target**: `src/core/sqlvibe/database.cpp` (new)

**TODO**:
- [ ] Create C++ Database class with embedded PageManager
- [ ] Migrate Exec/Query to C++ execution
- [ ] Handle result materialization in C++
- [ ] Create `src/core/sqlvibe/` directory structure

**Expected Impact**: -2,000 Go LOC, +1,500 C++ LOC

---

#### 9.2: Transaction Operations
**Files**: `pkg/sqlvibe/savepoint.go`, `pkg/sqlvibe/lock_opt.go`

**Functions to Migrate**:
- `Begin()` / `Commit()` / `Rollback()` - transaction lifecycle
- `Savepoint()` / `Release()` / `RollbackTo()` - savepoint operations
- Lock optimization hints

**C++ Target**: `src/core/sqlvibe/transaction.cpp` (new)

**TODO**:
- [ ] Integrate with existing TM layer
- [ ] Create C++ transaction wrapper
- [ ] Handle isolation levels in C++

**Expected Impact**: -500 Go LOC, +400 C++ LOC

---

#### 9.3: Schema Operations
**Files**: `pkg/sqlvibe/info.go`, `pkg/sqlvibe/index_test.go`

**Functions to Migrate**:
- `Tables()` - list tables
- `Columns()` - list columns
- `Indexes()` - list indexes
- `CreateIndex()` / `DropIndex()` - index management

**C++ Target**: `src/core/sqlvibe/schema.cpp` (new)

**TODO**:
- [ ] Create C++ schema introspection
- [ ] Integrate with IS layer
- [ ] Handle concurrent schema changes

**Expected Impact**: -400 Go LOC, +300 C++ LOC

---

#### 9.4: Extension Operations
**Files**: `pkg/sqlvibe/ext_json.go`, `pkg/sqlvibe/ext_math.go`, `pkg/sqlvibe/sqlvibe_extensions.go`

**Functions to Migrate**:
- JSON function registration
- Math function registration
- Extension querying

**C++ Target**: `src/core/sqlvibe/extensions.cpp` (new)

**TODO**:
- [ ] Create C++ extension registry
- [ ] Migrate function dispatch to C++
- [ ] Handle build tags in C++

**Expected Impact**: -300 Go LOC, +200 C++ LOC

---

#### 9.5: Utility Operations
**Files**: `pkg/sqlvibe/backup.go`, `pkg/sqlvibe/vacuum.go`, `pkg/sqlvibe/dump.go`, `pkg/sqlvibe/export.go`, `pkg/sqlvibe/import.go`

**Functions to Migrate**:
- `BackupTo()` - database backup
- `Vacuum()` - database compaction
- `Dump()` - SQL dump
- `Export()` / `Import()` - data migration

**C++ Target**: `src/core/sqlvibe/utilities.cpp` (new)

**TODO**:
- [ ] Create C++ backup with streaming
- [ ] Implement vacuum in C++
- [ ] Handle dump/import in C++

**Expected Impact**: -800 Go LOC, +600 C++ LOC

---

#### 9.6: PRAGMA Operations
**Files**: `pkg/sqlvibe/pragma.go`, `pkg/sqlvibe/pragma_ctx.go`, `pkg/sqlvibe/pragma_test.go`

**Functions to Migrate**:
- All PRAGMA get/set operations
- PRAGMA context handling

**C++ Target**: `src/core/sqlvibe/pragma.cpp` (new)

**TODO**:
- [ ] Create C++ PRAGMA registry
- [ ] Migrate all PRAGMA handlers
- [ ] Handle PRAGMA persistence

**Expected Impact**: -600 Go LOC, +500 C++ LOC

---

#### 9.7: Advanced SQL Features
**Files**: `pkg/sqlvibe/setops.go`, `pkg/sqlvibe/hash_join.go`, `pkg/sqlvibe/fk_trigger.go`, `pkg/sqlvibe/window.go`, `pkg/sqlvibe/vm_exec.go`, `pkg/sqlvibe/vm_context.go`, `pkg/sqlvibe/vtab_exec.go`, `pkg/sqlvibe/vtab_series.go`

**Functions to Migrate**:
- SET operations (UNION, INTERSECT, EXCEPT)
- Hash join execution
- Foreign key triggers
- Window function execution
- Virtual table execution

**C++ Target**: `src/core/sqlvibe/advanced_sql.cpp` (new)

**TODO**:
- [ ] Create C++ SET operation executor
- [ ] Migrate hash join to C++
- [ ] Handle FK triggers in C++
- [ ] Complete window function execution

**Expected Impact**: -1,500 Go LOC, +1,200 C++ LOC

---

#### 9.8: Pool and Cache Operations
**Files**: `pkg/sqlvibe/pools.go`, `pkg/sqlvibe/statement_pool.go`, `pkg/sqlvibe/row_pool.go`

**Functions to Migrate**:
- Statement pooling
- Row pooling
- Result caching

**C++ Target**: `src/core/sqlvibe/pools.cpp` (new)

**TODO**:
- [ ] Create C++ pool allocators
- [ ] Implement statement cache in C++
- [ ] Handle concurrent access

**Expected Impact**: -300 Go LOC, +400 C++ LOC

---

#### 9.9: Integrity and Info Operations
**Files**: `pkg/sqlvibe/integrity.go`, `pkg/sqlvibe/info_test.go`, `pkg/sqlvibe/index_usage_test.go`

**Functions to Migrate**:
- `IntegrityCheck()` - database integrity
- Schema info queries
- Index usage tracking

**C++ Target**: `src/core/sqlvibe/integrity.cpp` (new)

**TODO**:
- [ ] Create C++ integrity checker
- [ ] Migrate info queries to C++
- [ ] Handle index usage tracking

**Expected Impact**: -200 Go LOC, +300 C++ LOC

---

### Phase 10: Final Integration (1 week)

**Goal**: Complete integration and cleanup

#### 10.1: CGO Wrapper Consolidation
**Files**: All `*_cgo.go` files

**TODO**:
- [ ] Consolidate CGO wrappers into unified `internal/cgo/` package
- [ ] Remove redundant type conversions
- [ ] Optimize memory ownership patterns
- [ ] Create unified CGO header file

**Expected Impact**: -400 Go LOC

---

#### 10.2: Build System Updates
**Files**: `CMakeLists.txt`, `build.sh`, `src/CMakeLists.txt`

**TODO**:
- [ ] Add all new C++ files to CMakeLists.txt
- [ ] Create `src/core/sqlvibe/CMakeLists.txt`
- [ ] Update build tags if needed
- [ ] Optimize compile flags for C++
- [ ] Add SIMD optimizations for new C++ code

---

#### 10.3: Test Validation
**Files**: `tests/`

**TODO**:
- [ ] Run full test suite with C++ only paths
- [ ] Remove Go-only test helpers
- [ ] Add C++ unit tests for new modules
- [ ] Benchmark comparison with Go fallback removed

---

## Files That REMAIN in Go

| File/Directory | LOC | Reason |
|----------------|-----|--------|
| `driver/` | ~500 | Implements Go's `database/sql/driver` interface - must be Go |
| `tests/` | ~10,000 | Test suite - Go testing framework |
| `pkg/sqlvibe/` (wrapper) | ~500 | Thin CGO wrapper for public API |
| `internal/DS/arena.go` | ~100 | Go arena allocator (Go-specific) |
| `internal/DS/worker_pool.go` | ~100 | Go concurrency primitives |
| `internal/DS/parallel.go` | ~50 | Go parallel utilities |
| `internal/DS/prefetch.go` | ~100 | Go prefetch utilities |
| `internal/DS/vtab*.go` | ~200 | Virtual table interface (Go interface pattern) |
| **TOTAL REMAINING** | **~11,750** | |

---

## Timeline Summary

| Phase | Duration | Go LOC Reduced | C++ LOC Added | Risk |
|-------|----------|----------------|---------------|------|
| **Phase 6**: CG Complete | 1 week | -400 | +600 | Low |
| **Phase 7**: DS Cleanup | 1 week | -900 | +500 | Low |
| **Phase 8**: VM Cleanup | 1 week | -1,000 | - | Low |
| **Phase 9**: pkg/sqlvibe | 2-3 weeks | -6,600 | +5,000 | Medium |
| **Phase 10**: Integration | 1 week | -400 | - | Low |
| **TOTAL** | **6-8 weeks** | **-9,300 Go LOC** | **+6,100 C++ LOC** | |

---

## Final State (v0.11.1)

### Code Distribution

| Component | Language | LOC | Percentage |
|-----------|----------|-----|------------|
| **libsvdb.so (C++ core)** | C++ | ~15,000 | 55% |
| **pkg/sqlvibe (wrapper)** | Go | ~500 | 2% |
| **driver/** | Go | ~500 | 2% |
| **tests/** | Go | ~10,000 | 37% |
| **internal/ (remaining)** | Go | ~1,000 | 4% |
| **TOTAL** | | **~27,000** | **100%** |

### Performance Targets

| Workload | Current (v0.11.0) | Target (v0.11.1) | Improvement |
|----------|-------------------|-------------------|-------------|
| SELECT all | 3.0× faster | 5.0× faster | +67% |
| WHERE filter | 2.9× slower | 1.0× (equal) | +190% |
| GROUP BY | 5.2× faster | 8.0× faster | +54% |
| JOIN | 1.8× slower | 1.0× (equal) | +80% |
| ORDER BY | 1.5× slower | 1.0× (equal) | +50% |
| INSERT | 2.2× faster | 3.0× faster | +36% |
| **AVG Speedup** | **3.0×** | **5.0×** | **+67%** |

---

## Risk Mitigation

### Risk 1: C++ Memory Management Complexity
**Mitigation**: Use RAII, smart pointers, arena allocators. Follow existing patterns in DS layer.

### Risk 2: Go Concurrency Features
**Mitigation**: Retain Go for high-level concurrency (worker pools, prefetch). Migrate only hot paths.

### Risk 3: Breaking Changes
**Mitigation**: Maintain backwards-compatible Go API during migration. Deprecate gradually.

### Risk 4: Performance Regression
**Mitigation**: Benchmark after each phase. Keep Go fallback until C++ is validated.

### Risk 5: CGO Overhead
**Mitigation**: Batch operations, minimize boundary crossings, use direct C pointers.

---

## Success Criteria

- [x] **All VM orchestration** (Phase 5) migrated to C++ — all 14 engine functions in engine.cpp ✅
- [x] **Build error fixed** — engine_api.h include path corrected, C.svdb_value_t type fixed ✅
- [x] **HybridStore C++ scan** complete — Scan, ScanWithFilter, ScanProjected in hybrid_store.cpp ✅
- [x] **Phase 6 bytecode optimizer bugs fixed** — `BC_RESULT_ROW` off-by-one fixed, conservative default for unknown opcodes, `p4_regs` marking for OpInsert ✅
- [x] **Phase 6 bytecode optimizer wired** — `OptimizeBytecodeInstrs` in `execBytecode` (BytecodeVM path); all SQL1999 tests pass ✅
- [x] **Phase 6 legacy optimizer** — `CG_OP_*` constants reconciled with `VM.OpCode` values; `CGOptimizeProgram` wired into `compiler.finalize()` ✅
- [x] **README performance updated** — fresh v0.11.1 benchmarks (AMD EPYC 7763) ✅
- [x] **All 89+ SQL:1999 tests** passing ✅
- [x] **Phase 8.1 VM Fallback cleanup** — all `go*` fallback functions removed from `internal/VM/engine/` ✅
- [ ] **DS Layer Cleanup** (Phase 7) — WAL, cache, freelist, overflow C++ wrappers consolidated
- [x] **VM Layer Cleanup** (Phase 8.1) — Go engine fallbacks removed ✅
- [ ] **pkg/sqlvibe/** reduced to <500 LOC wrapper (Phase 9)
- [ ] **5× average speedup** over SQLite (Phase 9/10)
- [ ] **CG statement/plan cache** migrated to C++ (Phase 6/9)
- [ ] **Documentation** updated (ARCHITECTURE.md)

---

## Related Documents

- `docs/plan-cgo.md` - CGO architecture and progress tracking
- `docs/plan-v0.11.1.md` - Phase 5 libsvdb consolidation (superseded by this plan)
- `docs/ARCHITECTURE.md` - System architecture overview
- `docs/HISTORY.md` - Release history (update with v0.11.1 changes)

---

## Appendix: C++ Module Status

| Module | Directory | Status | Files |
|--------|-----------|--------|-------|
| **Core VM** | `src/core/VM/` | ✅ Complete | engine.cpp, aggregate_engine.cpp, bytecode_vm.cpp, etc. |
| **Core DS** | `src/core/DS/` | ✅ Complete | btree.cpp, hybrid_store.cpp, wal.cpp, etc. |
| **Core CG** | `src/core/CG/` | 🚧 In Progress | optimizer.cpp, compiler.cpp, bytecode_compiler.cpp |
| **Core QP** | `src/core/QP/` | ✅ Complete | Parser, tokenizer in C++ |
| **Core TM** | `src/core/TM/` | ✅ Complete | Transaction manager |
| **Core IS** | `src/core/IS/` | ✅ Complete | Information schema |
| **Core SF** | `src/core/SF/` | ✅ Complete | System foundation |
| **Core PB** | `src/core/PB/` | ✅ Complete | Platform abstraction |
| **sqlvibe API** | `src/core/sqlvibe/` | ❌ Not Started | (new directory to be created) |
| **Extensions** | `src/ext/` | ✅ Complete | JSON, Math, FTS5 |
