# sqlvibe v0.11.4 — Final Go Elimination Plan

**Date**: 2026-03-04
**Target Version**: v0.11.4
**Goal**: Eliminate remaining Go business logic, migrate to pure C++ with thin Go wrappers

---

## Current Architecture Analysis (v0.11.3)

### Remaining Go Code in `pkg/sqlvibe/` (~1,000 LOC)

```
pkg/sqlvibe/
├── database.go (1054 LOC)      ← API layer, type conversion, virtual table handling
├── vtab_handler.go (663 LOC)   ← Go business logic: virtual table registry & dispatch
├── vtab_series.go (114 LOC)    ← Go business logic: series virtual table implementation
└── [test files]
```

### The Problem: C++→CGo→C++ Anti-Pattern

```
┌─────────────────────────────────────────────────────────┐
│              Go Application (pkg/sqlvibe)               │
│  - Virtual table registry (Go map)                      │
│  - Virtual table dispatch (Go interface calls)          │
│  - Type conversion (Go ↔ C++)                           │
└─────────────────────────────────────────────────────────┘
                          ↓ CGO call
┌─────────────────────────────────────────────────────────┐
│          Go CGO Wrapper (internal/cgo) ~300 LOC         │
│  - Pure C API wrapper (svdb.h)                          │
│  - NO business logic (good!)                            │
└─────────────────────────────────────────────────────────┘
                          ↓ C API call
┌─────────────────────────────────────────────────────────┐
│           C++ Core Engine (src/core/) ~20,000 LOC       │
│  - All SQL execution in C++                             │
│  - BUT: Virtual tables handled in Go ← PROBLEM          │
│  - C++ → Go callback for vtab → C++ ← ANTI-PATTERN      │
└─────────────────────────────────────────────────────────┘
```

### Anti-Pattern Flow (Virtual Table Example)

1. **SQL EXEC in C++**: `SELECT * FROM series(1, 10)`
2. **C++ detects vtab**: Needs to call Go for `series` module
3. **CGO callback**: C++ → Go `vtab_handler.go`
4. **Go dispatch**: Look up in Go map, call Go `seriesModule.Create()`
5. **Go creates vtab**: Returns Go `seriesVTab` struct
6. **CGO back to C++**: Pass vtab handle back to C++
7. **C++ iteration**: C++ calls Go `cursor.Next()`, `cursor.Column()` via CGO

**Problem**: Bidirectional CGO calls (C++→Go→C++) for every vtab operation!

---

## Solution: Pure C++ Virtual Tables

### New Architecture (v0.11.4)

```
┌─────────────────────────────────────────────────────────┐
│              Go Application (pkg/sqlvibe)               │
│  - Pure API layer (Open, Query, Exec, etc.)            │
│  - NO business logic                                    │
│  - NO virtual table registry (moved to C++)            │
│  (~200 LOC, down from ~1,800 LOC)                       │
└─────────────────────────────────────────────────────────┘
                          ↓ One-way CGO (~5ns)
┌─────────────────────────────────────────────────────────┐
│          Go Type Wrappers (internal/cgo) ~300 LOC       │
│  - Pure C API wrapper (svdb.h)                          │
│  - NO callbacks, NO business logic                      │
└─────────────────────────────────────────────────────────┘
                          ↓ One-way CGO (~5ns)
┌─────────────────────────────────────────────────────────┐
│           C++ Core Engine (src/core/) ~22,000 LOC       │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ Virtual Table Registry                     │   │
│  │  - Module registry (C++ map)                    │   │
│  │  - Module dispatch (C++ vtable)                 │   │
│  │  - NO Go callbacks                              │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ Series VTab (pure C++)                     │   │
│  │  - series_module (C++ class)                    │   │
│  │  - series_vtab (C++ class)                      │   │
│  │  - series_cursor (C++ class)                    │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ FTS5 VTab (pure C++)                       │   │
│  │  - fts5_module (C++ class)                      │   │
│  │  - fts5_vtab (C++ class)                        │   │
│  │  - fts5_cursor (C++ class)                      │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  Total: ~22,000 LOC C++ (self-contained, NO callbacks) │
└─────────────────────────────────────────────────────────┘
```

---

## Migration Plan

### Phase 1: C++ Virtual Table Framework (Week 1)

**Goal**: Create C++ vtab registry and dispatch framework

**Deliverables**:
- `src/core/IS/vtab_registry.h/cpp` (400 LOC)
  - `VTabModule` abstract base class
  - `VTab` abstract base class
  - `VTabCursor` abstract base class
  - `VTabRegistry` singleton

- `src/core/IS/vtab_api.h` (100 LOC)
  - C API for vtab registration
  - `svdb_vtab_register_module()`
  - `svdb_vtab_create()`
  - `svdb_vtab_cursor_open()`

- `src/core/svdb/svdb.h` updates
  - Add vtab-related C APIs

**Go Wrapper**:
- `internal/cgo/vtab_cgo.go` (50 LOC)
  - Pure type conversion, no business logic

**Files to Remove**:
- `pkg/sqlvibe/vtab_handler.go` (663 LOC)
- `internal/DS/vtab_module_type.go` (stub, will be replaced)

### Phase 2: C++ Series VTab (Week 1-2)

**Goal**: Migrate series virtual table to pure C++

**Deliverables**:
- `src/core/IS/vtab_series.h/cpp` (300 LOC)
  - `SeriesModule : public VTabModule`
  - `SeriesVTab : public VTab`
  - `SeriesCursor : public VTabCursor`

- Auto-registration in C++ static init

**Go Wrapper**: None needed (pure C++)

**Files to Remove**:
- `pkg/sqlvibe/vtab_series.go` (114 LOC)

### Phase 3: C++ FTS5 VTab (Week 2-3)

**Goal**: Migrate FTS5 virtual table to pure C++

**Deliverables**:
- `src/core/ext/fts5/fts5_vtab.h/cpp` (800 LOC)
  - `FTS5Module : public VTabModule`
  - `FTS5VTab : public VTab`
  - `FTS5Cursor : public VTabCursor`

- Integration with existing FTS5 extension

**Files to Remove**:
- `ext/fts5/fts5.go` (migrate logic to C++)

### Phase 4: Go API Layer Simplification (Week 3)

**Goal**: Simplify `pkg/sqlvibe/database.go` to pure API layer

**Changes**:
- Remove `vtabs *vtabState` field
- Remove virtual table handling logic
- Keep only:
  - Type conversion (Go ↔ C++)
  - Error handling
  - Context support

**Result**:
- `database.go`: ~200 LOC (down from ~1,000 LOC)
- No business logic, pure API wrapper

### Phase 5: Cleanup & Optimization (Week 4)

**Goal**: Final cleanup, performance validation

**Tasks**:
- Remove all Go vtab-related code
- Optimize CGO boundary (batching, zero-copy)
- Performance benchmarks
- Documentation update

**Files to Remove**:
- `internal/DS/vtab_module_type.go`
- Any remaining vtab stubs

---

## Code Metrics Projection

### Before v0.11.4 (v0.11.3)

| Layer | Go LOC | C++ LOC | Total |
|-------|--------|---------|-------|
| API (pkg/sqlvibe) | ~1,800 | 0 | 1,800 |
| CGO Wrapper | ~300 | 0 | 300 |
| C++ Core | 0 | ~20,000 | 20,000 |
| **Total** | **~2,100** | **~20,000** | **~22,100** |

### After v0.11.4 (Target)

| Layer | Go LOC | C++ LOC | Total | Reduction |
|-------|--------|---------|-------|-----------|
| API (pkg/sqlvibe) | ~200 | 0 | 200 | 89% |
| CGO Wrapper | ~300 | 0 | 300 | 0% |
| C++ Core | 0 | ~22,000 | 22,000 | +10% |
| **Total** | **~500** | **~22,000** | **~22,500** | **76%** |

**Go Code Reduction**: 2,100 LOC → 500 LOC (**76% additional reduction**)

**Cumulative Reduction** (v0.10.x → v0.11.4): 21,900 LOC → 500 LOC (**98% reduction**)

---

## Benefits

### Performance
- **Eliminate bidirectional CGO**: No more C++→Go→C++ callbacks
- **Faster vtab operations**: Pure C++ dispatch (~10x faster)
- **Reduced GC pressure**: No Go allocations for vtab

### Code Quality
- **Single language**: All business logic in C++
- **Cleaner separation**: Go = API layer, C++ = engine
- **Easier maintenance**: No cross-language debugging

### Architecture
- **True C++ core**: Self-contained, no Go dependencies
- **Thin Go wrappers**: Pure type conversion, no logic
- **Future-proof**: Easy to add more languages (Python, Rust, etc.)

---

## Risk Analysis

### Critical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| C++ vtab framework bugs | Medium | High | Extensive unit tests, fuzzing |
| Series/FTS5 migration breaks compatibility | Medium | High | Keep Go impl as fallback for 2 weeks |
| CGO API changes require Go updates | Low | Medium | Careful API design, versioning |

### Mitigation Strategy

1. **Parallel Implementation**: Keep Go vtab code as fallback during migration
2. **Gradual Rollout**: Migrate series first, then FTS5
3. **Extensive Testing**: C++ unit tests + Go integration tests
4. **Performance Monitoring**: Benchmark after each phase

---

## Success Criteria

### Functional

- [ ] All v0.11.3 tests passing
- [ ] All SQL:1999 test suites passing
- [ ] Zero callback-based code
- [ ] Go API layer <500 LOC total

### Performance

- [ ] No benchmark regression >5%
- [ ] 50% improvement on vtab-heavy queries
- [ ] Zero-GC for simple queries
- [ ] CGO overhead <5ns per call

### Code Quality

- [ ] >80% test coverage for new C++ code
- [ ] All C++ code follows style guide
- [ ] Go wrappers <500 LOC total
- [ ] Documentation updated

---

## Timeline Summary

| Phase | Component | Start | End | Duration |
|-------|-----------|-------|-----|----------|
| **Phase 1** | C++ VTab Framework | 2026-03-05 | 2026-03-11 | 1 week |
| **Phase 2** | C++ Series VTab | 2026-03-12 | 2026-03-18 | 1 week |
| **Phase 3** | C++ FTS5 VTab | 2026-03-19 | 2026-03-25 | 1 week |
| **Phase 4** | Go API Simplification | 2026-03-26 | 2026-04-01 | 1 week |
| **Phase 5** | Cleanup & Optimization | 2026-04-02 | 2026-04-08 | 1 week |

**Total Duration**: 5 weeks
**Completion Target**: 2026-04-08

---

## Next Steps

### Immediate (Week of 2026-03-05)

1. [ ] Create `src/core/IS/vtab_registry.h/cpp`
2. [ ] Create `src/core/IS/vtab_api.h`
3. [ ] Update `src/core/svdb/svdb.h` with vtab APIs
4. [ ] Write C++ unit tests for vtab registry
5. [ ] Create `internal/cgo/vtab_cgo.go` wrapper

### Week of 2026-03-12

1. [ ] Create `src/core/IS/vtab_series.h/cpp`
2. [ ] Remove `pkg/sqlvibe/vtab_series.go`
3. [ ] Migrate series tests to C++
4. [ ] Keep Go series as fallback

---

**Document Version**: 1.0
**Created**: 2026-03-04
**Maintainer**: sqlvibe team
**Next Review**: 2026-03-05
