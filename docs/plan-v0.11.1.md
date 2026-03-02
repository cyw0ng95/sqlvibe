# Phase 6: Complete C++ Migration Plan (v0.11.1)

**Last Updated**: 2026-03-02
**Target Version**: v0.11.1
**Status**: 🚧 In Progress — Phase 5 VM orchestration complete, build error fixed
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
| **Public API** | `pkg/sqlvibe/` | ~40 | ~8,000 | ❌ 0% | P2 |
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

### Phase 4: Complete DS Layer (1-2 weeks)

**Goal**: Finish remaining DS orchestration code

#### 4.1: B-Tree Complete C++ Wrapper
**Files**: `internal/DS/btree.go`, `internal/DS/btree_cgo.go`

**Current State**:
- C++ B-Tree implementation exists (`src/core/DS/btree.cpp`)
- Go wrapper still uses callback pattern
- Search, Insert, Delete implemented in C++

**TODO**:
- [ ] Remove Go callback registry from btree_cgo.go
- [ ] Use embedded C++ PageManager pattern
- [ ] Delete `internal/DS/btree.go` (keep only thin wrapper)

**Expected Impact**: -400 Go LOC

---

#### 4.2: HybridStore Full C++ Migration ✅
**Files**: `internal/DS/hybrid_store.go`, `internal/DS/hybrid_store_cgo.go`

**Current State**: ✅ Complete
- C++ HybridStore API created (`src/core/DS/hybrid_store_api.h`) ✅
- C++ implementation complete with Scan, ScanWithFilter, ScanProjected (`src/core/DS/hybrid_store.cpp`) ✅
- Go wrapper uses C++ indexes ✅

---

### Phase 5: Complete VM Orchestration ✅

**Goal**: Migrate all query execution orchestration to C++ — **COMPLETE**

All engine functions implemented in `src/core/VM/engine/engine.cpp` and wrapped in `internal/VM/engine/engine_cgo.go`. Build issue with `engine_api.h` relative include path fixed.

#### 5.1: SELECT Query Engine ✅
- [x] `CFilterRows()` → `svdb_engine_filter_rows()` (Go callback predicate)
- [x] `CApplyDistinct()` → `svdb_engine_apply_distinct()` (Go callback key function)
- [x] `CApplyLimitOffset()` → `svdb_engine_apply_limit_offset()`
- [x] Go wrapper delegates to C++ by default

#### 5.2: JOIN Query Engine ✅
- [x] `CMergeRows()` → `svdb_engine_merge_rows()`
- [x] `CMergeRowsWithAlias()` → `svdb_engine_merge_rows_alias()`
- [x] `CCrossJoin()` → `svdb_engine_cross_join()`
- [x] `CInnerJoin()` → `svdb_engine_inner_join()` (Go callback predicate)
- [x] `CLeftOuterJoin()` → `svdb_engine_left_outer_join()` (Go callback predicate)

#### 5.3: Aggregate Engine ✅
- [x] `CGroupRows()` → `svdb_engine_group_rows()` (Go callback key function)
- [x] `CCountRows()` → `svdb_engine_count_rows()`
- [x] `CSumRows()` → `svdb_engine_sum_rows()`
- [x] `CAvgRows()` → `svdb_engine_avg_rows()`
- [x] `CMinRows()/CMaxRows()` → C++ min/max

#### 5.4: Sort Engine ✅
- [x] `CSortRows()` → `svdb_engine_sort_rows()` (multi-key with NULL ordering)
- [x] `CReverseRows()` → `svdb_engine_reverse_rows()`

#### 5.5: Window Function Engine ✅
- [x] `CRowNumbers()` → `svdb_engine_row_numbers()`
- [x] `CRanks()` → `svdb_engine_ranks()`
- [x] `CDenseRanks()` → `svdb_engine_dense_ranks()`

#### 5.6: Subquery Engine ✅
- [x] `CExistsRows()` → `svdb_engine_exists_rows()`
- [x] `CInRows()` → `svdb_engine_in_rows()`
- [x] `CNotInRows()` → `svdb_engine_not_in_rows()`

---

### Phase 6: Complete CG Layer ✅ (Partial — bytecode optimizer wired)

**Goal**: Finish code generation Go wrapper

#### 6.1: Bytecode Compiler Wrapper ✅ (Partial)
**Files**: `internal/CG/cg_cgo.go`, `internal/CG/compiler.go`, `pkg/sqlvibe/vm_exec.go`, `src/core/CG/optimizer.cpp`, `src/core/CG/compiler.cpp`

**Completed (2026-03-02)**:
- Fixed `BC_RESULT_ROW = 31 → 30` in `optimizer.cpp` (was off-by-one, causing all constant loads to be incorrectly eliminated) ✅
- Fixed `eliminateBcDeadCode` in `optimizer.cpp` — conservative default: mark unknown-opcode operands as "read" ✅
- Fixed `eliminateDeadCode` in `optimizer.cpp` — same conservative fix for legacy CG path ✅
- Fixed `cgEliminateDeadCode` in `compiler.cpp` — conservative default + mark `p4_regs` registers as "read" for OpInsert ✅
- Fixed `programToJSON` in `cg_cgo.go` — handle `map[string]int` P4 (named-column INSERT) as register list ✅
- **Wired `OptimizeBytecodeInstrs` into `execBytecode`** (BytecodeVM path) ✅ — all SQL1999 tests pass

**Blocked**:
- `CGOptimizeProgram` wiring into `compiler.finalize()` is blocked because `CG_OP_*` constants in `compiler.cpp` don't match Go's `VM.OpCode` values (different numbering). Attempts cause F201/CastInSubquery and other failures. Requires reconciling the two opcode numbering systems before `CGOptimizeProgram` can be safely applied to the legacy VM.Program path.

**TODO**:
- [ ] Reconcile `CG_OP_*` constants in `compiler.cpp` with Go `VM.OpCode` values
- [ ] After reconciliation, wire `CGOptimizeProgram` into `compiler.finalize()`

---

### Phase 7: pkg/sqlvibe Thin Wrapper (2-3 weeks)

**Goal**: Reduce public API to ~500 LOC CGO wrapper

#### 7.1: Database Operations
**Files**: `pkg/sqlvibe/database.go`, `pkg/sqlvibe/exec_state.go`

**Functions to Migrate**:
- `Open()` - database opening
- `Close()` - database closing
- `Exec()` - statement execution
- `Query()` - query execution
- `Prepare()` - statement preparation

**C++ Target**: `src/core/sqlvibe/database.cpp`

**TODO**:
- [ ] Create C++ Database class with embedded PageManager
- [ ] Migrate Exec/Query to C++ execution
- [ ] Handle result materialization in C++

**Expected Impact**: -2,000 Go LOC

---

#### 7.2: Transaction Operations
**Files**: `pkg/sqlvibe/savepoint.go`, `pkg/sqlvibe/lock_opt.go`

**Functions to Migrate**:
- `Begin()` / `Commit()` / `Rollback()` - transaction lifecycle
- `Savepoint()` / `Release()` / `RollbackTo()` - savepoint operations
- Lock optimization hints

**C++ Target**: `src/core/sqlvibe/transaction.cpp`

**TODO**:
- [ ] Integrate with existing TM layer
- [ ] Create C++ transaction wrapper
- [ ] Handle isolation levels in C++

**Expected Impact**: -500 Go LOC

---

#### 7.3: Schema Operations
**Files**: `pkg/sqlvibe/info.go`, `pkg/sqlvibe/index_test.go`

**Functions to Migrate**:
- `Tables()` - list tables
- `Columns()` - list columns
- `Indexes()` - list indexes
- `CreateIndex()` / `DropIndex()` - index management

**C++ Target**: `src/core/sqlvibe/schema.cpp`

**TODO**:
- [ ] Create C++ schema introspection
- [ ] Integrate with IS layer
- [ ] Handle concurrent schema changes

**Expected Impact**: -400 Go LOC

---

#### 7.4: Extension Operations
**Files**: `pkg/sqlvibe/ext_json.go`, `pkg/sqlvibe/ext_math.go`, `pkg/sqlvibe/sqlvibe_extensions.go`

**Functions to Migrate**:
- JSON function registration
- Math function registration
- Extension querying

**C++ Target**: `src/core/sqlvibe/extensions.cpp`

**TODO**:
- [ ] Create C++ extension registry
- [ ] Migrate function dispatch to C++
- [ ] Handle build tags in C++

**Expected Impact**: -300 Go LOC

---

#### 7.5: Utility Operations
**Files**: `pkg/sqlvibe/backup.go`, `pkg/sqlvibe/vacuum.go`, `pkg/sqlvibe/dump.go`, `pkg/sqlvibe/export.go`, `pkg/sqlvibe/import.go`

**Functions to Migrate**:
- `BackupTo()` - database backup
- `Vacuum()` - database compaction
- `Dump()` - SQL dump
- `Export()` / `Import()` - data migration

**C++ Target**: `src/core/sqlvibe/utilities.cpp`

**TODO**:
- [ ] Create C++ backup with streaming
- [ ] Implement vacuum in C++
- [ ] Handle dump/import in C++

**Expected Impact**: -800 Go LOC

---

#### 7.6: PRAGMA Operations
**Files**: `pkg/sqlvibe/pragma.go`, `pkg/sqlvibe/pragma_ctx.go`, `pkg/sqlvibe/pragma_test.go`

**Functions to Migrate**:
- All PRAGMA get/set operations
- PRAGMA context handling

**C++ Target**: `src/core/sqlvibe/pragma.cpp`

**TODO**:
- [ ] Create C++ PRAGMA registry
- [ ] Migrate all PRAGMA handlers
- [ ] Handle PRAGMA persistence

**Expected Impact**: -600 Go LOC

---

#### 7.7: Advanced SQL Features
**Files**: `pkg/sqlvibe/setops.go`, `pkg/sqlvibe/hash_join.go`, `pkg/sqlvibe/fk_trigger.go`, `pkg/sqlvibe/window.go`, `pkg/sqlvibe/vm_exec.go`, `pkg/sqlvibe/vm_context.go`, `pkg/sqlvibe/vtab_exec.go`, `pkg/sqlvibe/vtab_series.go`

**Functions to Migrate**:
- SET operations (UNION, INTERSECT, EXCEPT)
- Hash join execution
- Foreign key triggers
- Window function execution
- Virtual table execution

**C++ Target**: `src/core/sqlvibe/advanced_sql.cpp`

**TODO**:
- [ ] Create C++ SET operation executor
- [ ] Migrate hash join to C++
- [ ] Handle FK triggers in C++
- [ ] Complete window function execution

**Expected Impact**: -1,500 Go LOC

---

#### 7.8: Pool and Cache Operations
**Files**: `pkg/sqlvibe/pools.go`, `pkg/sqlvibe/statement_pool.go`, `pkg/sqlvibe/row_pool.go`

**Functions to Migrate**:
- Statement pooling
- Row pooling
- Result caching

**C++ Target**: `src/core/sqlvibe/pools.cpp`

**TODO**:
- [ ] Create C++ pool allocators
- [ ] Implement statement cache in C++
- [ ] Handle concurrent access

**Expected Impact**: -300 Go LOC

---

#### 7.9: Integrity and Info Operations
**Files**: `pkg/sqlvibe/integrity.go`, `pkg/sqlvibe/info_test.go`, `pkg/sqlvibe/index_usage_test.go`

**Functions to Migrate**:
- `IntegrityCheck()` - database integrity
- Schema info queries
- Index usage tracking

**C++ Target**: `src/core/sqlvibe/integrity.cpp`

**TODO**:
- [ ] Create C++ integrity checker
- [ ] Migrate info queries to C++
- [ ] Handle index usage tracking

**Expected Impact**: -200 Go LOC

---

### Phase 8: Final Integration (1 week)

**Goal**: Complete integration and cleanup

#### 8.1: Remove Go Fallback Functions
**Files**: All `internal/VM/engine/*.go`, `internal/DS/*.go`

**TODO**:
- [ ] Remove `goFilterRows()`, `goInnerJoin()`, etc. fallback functions
- [ ] Remove Go-only implementations after validation
- [ ] Update tests to use C++ paths only

**Expected Impact**: -500 Go LOC

---

#### 8.2: CGO Wrapper Cleanup
**Files**: All `*_cgo.go` files

**TODO**:
- [ ] Consolidate CGO wrappers
- [ ] Remove redundant type conversions
- [ ] Optimize memory ownership patterns

**Expected Impact**: -200 Go LOC

---

#### 8.3: Build System Updates
**Files**: `CMakeLists.txt`, `build.sh`

**TODO**:
- [ ] Add all new C++ files to CMakeLists.txt
- [ ] Update build tags if needed
- [ ] Optimize compile flags for C++

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
| **Phase 4**: DS Complete | 1-2 weeks | -700 | +500 | Low |
| **Phase 5**: VM Complete | 3-4 weeks | -1,050 | +2,000 | Medium |
| **Phase 6**: CG Complete | 1 week | -200 | +300 | Low |
| **Phase 7**: pkg/sqlvibe | 2-3 weeks | -6,600 | +4,000 | Medium |
| **Phase 8**: Integration | 1 week | -700 | - | Low |
| **TOTAL** | **8-11 weeks** | **-9,250 Go LOC** | **+6,800 C++ LOC** | |

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
- [ ] **Phase 6 legacy optimizer** — `CGOptimizeProgram` in `finalize()` blocked: `CG_OP_*` constants in compiler.cpp don't match Go `VM.OpCode` values
- [x] **README performance updated** — fresh v0.11.1 benchmarks (AMD EPYC 7763) ✅
- [x] **All 89+ SQL:1999 tests** passing ✅
- [ ] **B-Tree Phase 4.1** — Remove Go callbacks, use embedded C++ PageManager
- [ ] **pkg/sqlvibe/** reduced to <500 LOC wrapper (Phase 7)
- [ ] **5× average speedup** over SQLite (Phase 7/8)
- [ ] **CG statement cache** migrated to C++ (Phase 6, optional)
- [ ] **Documentation** updated (ARCHITECTURE.md)

---

## Related Documents

- `docs/plan-cgo.md` - CGO architecture and progress tracking
- `docs/plan-v0.11.1.md` - Phase 5 libsvdb consolidation (superseded by this plan)
- `docs/ARCHITECTURE.md` - System architecture overview
- `docs/HISTORY.md` - Release history (update with v0.11.1 changes)
