# Phase 6: Complete C++ Migration Plan (v0.11.1)

**Last Updated**: 2026-03-02
**Target Version**: v0.11.1
**Status**: 📋 Planning
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

#### 4.2: HybridStore Full C++ Migration
**Files**: `internal/DS/hybrid_store.go`, `internal/DS/hybrid_store_cgo.go`

**Current State**:
- C++ HybridStore API created (`src/core/DS/hybrid_store_api.h`)
- C++ implementation started (`src/core/DS/hybrid_store.cpp`)
- Go wrapper uses C++ indexes

**TODO**:
- [ ] Complete C++ Scan(), ScanWithFilter(), ScanProjected() implementations
- [ ] Migrate row materialization to C++
- [ ] Remove Go HybridStore orchestration

**Expected Impact**: -300 Go LOC

---

### Phase 5: Complete VM Orchestration (3-4 weeks)

**Goal**: Migrate all query execution orchestration to C++

#### 5.1: SELECT Query Engine
**Files**: `internal/VM/engine/select.go`, `internal/VM/engine/engine_cgo.go`

**Functions to Migrate**:
- `FilterRows()` - predicate filtering
- `ProjectRows()` - column projection
- `ApplyDistinct()` - DISTINCT elimination
- `ApplyLimitOffset()` - LIMIT/OFFSET

**C++ Target**: `src/core/VM/engine/select.cpp`

**TODO**:
- [ ] Create C++ FilterRows with std::function predicate
- [ ] Create C++ ProjectRows with projection map
- [ ] Create C++ ApplyDistinct with key function
- [ ] Update Go wrapper to call C++ by default

**Expected Impact**: -150 Go LOC

---

#### 5.2: JOIN Query Engine
**Files**: `internal/VM/engine/join.go`

**Functions to Migrate**:
- `MergeRows()` - row merging
- `MergeRowsWithAlias()` - qualified column merging
- `CrossJoin()` - Cartesian product
- `InnerJoin()` - predicate join
- `LeftOuterJoin()` - outer join with NULL padding

**C++ Target**: `src/core/VM/engine/join.cpp`

**TODO**:
- [ ] Create C++ MergeRows with column deduplication
- [ ] Create C++ CrossJoin with pre-sized output
- [ ] Create C++ InnerJoin with predicate callback
- [ ] Create C++ LeftOuterJoin with NULL handling

**Expected Impact**: -200 Go LOC

---

#### 5.3: Aggregate Engine
**Files**: `internal/VM/engine/aggregate.go`

**Functions to Migrate**:
- `GroupRows()` - GROUP BY partitioning
- `CountRows()` - COUNT aggregate
- `SumRows()` - SUM aggregate
- `AvgRows()` - AVG aggregate
- `MinRows()` / `MaxRows()` - MIN/MAX aggregates
- `GroupByAndAggregate()` - combined GROUP BY + aggregate

**C++ Target**: `src/core/VM/engine/aggregate.cpp`

**TODO**:
- [ ] Create C++ GroupRows with unordered_map grouping
- [ ] Create C++ aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] Handle NULL propagation in C++
- [ ] Support streaming aggregates

**Expected Impact**: -250 Go LOC

---

#### 5.4: Sort Engine
**Files**: `internal/VM/engine/sort.go`

**Functions to Migrate**:
- `SortRowsByKeys()` - multi-key sorting
- `TopKRows()` - LIMIT with ORDER BY optimization
- `ReverseRows()` - row reversal

**C++ Target**: `src/core/VM/engine/sort.cpp`

**TODO**:
- [ ] Create C++ SortRows with std::sort and custom comparator
- [ ] Implement TopK optimization (partial sort)
- [ ] Handle NULL ordering (NULLS FIRST/LAST)

**Expected Impact**: -100 Go LOC

---

#### 5.5: Window Function Engine
**Files**: `internal/VM/engine/window.go`

**Functions to Migrate**:
- `PartitionRows()` - PARTITION BY
- `RowNumbers()` - ROW_NUMBER()
- `Ranks()` - RANK()
- `DenseRanks()` - DENSE_RANK()
- `LagValues()` / `LeadValues()` - LAG/LEAD
- `NthValues()` - NTH_VALUE

**C++ Target**: `src/core/VM/engine/window.cpp`

**TODO**:
- [ ] Create C++ PartitionRows with unordered_map
- [ ] Implement window functions with frame handling
- [ ] Support ROWS/RANGE frame specifications

**Expected Impact**: -200 Go LOC

---

#### 5.6: Subquery Engine
**Files**: `internal/VM/engine/subquery.go`

**Functions to Migrate**:
- `ExistsRows()` - EXISTS subquery
- `ScalarRow()` - scalar subquery
- `InRows()` / `NotInRows()` - IN/NOT IN subquery
- `AllRows()` / `AnyRows()` - ALL/ANY subquery

**C++ Target**: `src/core/VM/engine/subquery.cpp`

**TODO**:
- [ ] Create C++ subquery execution with result caching
- [ ] Implement correlated vs non-correlated subquery detection
- [ ] Handle three-valued logic (NULL handling)

**Expected Impact**: -150 Go LOC

---

### Phase 6: Complete CG Layer (1 week)

**Goal**: Finish code generation Go wrapper

#### 6.1: Bytecode Compiler Wrapper
**Files**: `internal/CG/cg_cgo.go`, `internal/CG/compiler.go`

**Current State**:
- C++ compiler exists (`src/core/CG/compiler.cpp`)
- Go wrapper handles statement caching

**TODO**:
- [ ] Create C++ statement cache
- [ ] Migrate bytecode optimization to C++
- [ ] Remove Go compiler orchestration

**Expected Impact**: -200 Go LOC

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

- [ ] **All internal/ orchestration** migrated to C++
- [ ] **pkg/sqlvibe/** reduced to <500 LOC wrapper
- [ ] **All 89+ SQL:1999 tests** passing
- [ ] **5× average speedup** over SQLite
- [ ] **No Go callbacks** in C++ inner loops
- [ ] **Clean architecture**: C++ core, Go thin wrappers
- [ ] **Documentation** updated (ARCHITECTURE.md, HISTORY.md)

---

## Related Documents

- `docs/plan-cgo.md` - CGO architecture and progress tracking
- `docs/plan-v0.11.1.md` - Phase 5 libsvdb consolidation (superseded by this plan)
- `docs/ARCHITECTURE.md` - System architecture overview
- `docs/HISTORY.md` - Release history (update with v0.11.1 changes)
