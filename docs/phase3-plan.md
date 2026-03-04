# Phase 3: QP/CG/TM Migration Plan

**Date**: 2026-03-04
**Target Version**: v0.11.2
**Status**: Planning Complete

---

## Overview

Phase 3 migrates the Query Processing (QP), Code Generation (CG), and Transaction Management (TM) layers from Go to C++. This completes the core database engine migration.

### Goals

1. **QP Layer**: Move tokenizer, parser, optimizer to C++ (4000 LOC → 200 LOC)
2. **CG Layer**: Move compiler, optimizer, plan cache to C++ (2500 LOC → 200 LOC)
3. **TM Layer**: Move MVCC, lock manager to C++ (1500 LOC → 200 LOC)
4. **Maintain API**: Keep existing Go interfaces for backward compatibility
5. **Performance**: Zero regression on query compilation and transaction throughput

---

## Phase 3.1: Query Processing (QP) Migration

**Duration**: 2026-03-11 to 2026-03-18 (1 week)
**Priority**: High

### Current State

| Component | C++ LOC | Go LOC | Status |
|-----------|---------|--------|--------|
| Tokenizer | 400 | 795 | ✅ C++ complete |
| Parser | 800 | 584 | ✅ C++ complete |
| Analyzer | 300 | 300 | ✅ C++ complete |
| Binder | 200 | 271 | ✅ C++ complete |
| Normalizer | 150 | 300 | ✅ C++ complete |
| Optimizer | 150 | 412 | ⚠️ Partial |
| Type Inference | 100 | 200 | ⚠️ Partial |
| DAG/Plan | 200 | 150 | ✅ C++ complete |
| **Total** | **2300** | **3012** | **75% complete** |

### Migration Tasks

1. **[x] C++ Tokenizer** - `src/core/QP/tokenizer.cpp/h`
   - SQL keyword recognition
   - String/number parsing
   - Operator tokenization

2. **[x] C++ Parser** - `src/core/QP/parser*.cpp/h`
   - SELECT/INSERT/UPDATE/DELETE parsing
   - DDL/DML parsing
   - Expression parsing

3. **[x] C++ Analyzer** - `src/core/QP/analyzer.cpp/h`
   - Semantic analysis
   - Type checking
   - Scope resolution

4. **[ ] C++ Optimizer** - `src/core/QP/optimizer.cpp/h` (NEW)
   - Query rewrite rules
   - Predicate pushdown
   - Join reordering (basic)

5. **[ ] Go Wrapper** - `internal/QP/qp_cgo.go` (NEW)
   - Tokenize() wrapper
   - Parse() wrapper
   - Optimize() wrapper

### Go Files to Reduce

| File | Current | Target | Notes |
|------|---------|--------|-------|
| `tokenizer.go` | 795 LOC | 50 LOC | Keep token types, wrap C++ |
| `parser.go` | 584 LOC | 100 LOC | Keep AST types, wrap C++ |
| `analyzer.go` | 300 LOC | 50 LOC | Wrap C++ |
| `optimizer.go` | 412 LOC | 50 LOC | Move logic to C++ |
| `binder.go` | 271 LOC | 50 LOC | Wrap C++ |
| `normalize.go` | 300 LOC | 50 LOC | Wrap C++ |
| `type_infer.go` | 200 LOC | 50 LOC | Wrap C++ |

**Reduction**: 3012 LOC → 400 LOC (87% reduction)

### Architecture

```
Go SQL Query
    ↓
Go Tokenizer Wrapper (50 LOC)
    ↓ CGO
C++ Tokenizer (400 LOC)
    ↓
C++ Parser (800 LOC)
    ↓
C++ Analyzer (300 LOC)
    ↓
C++ Optimizer (150 LOC)
    ↓ CGO
Go AST Types (pure data, no logic)
    ↓
C++ Code Generator
```

---

## Phase 3.2: Code Generation (CG) Migration

**Duration**: 2026-03-18 to 2026-03-25 (1 week)
**Priority**: High

### Current State

| Component | C++ LOC | Go LOC | Status |
|-----------|---------|--------|--------|
| Compiler | 600 | 1315 | ✅ C++ complete |
| Expr Compiler | 300 | 300 | ✅ C++ complete |
| Bytecode Compiler | 200 | 340 | ✅ C++ complete |
| Direct Compiler | 150 | 200 | ✅ C++ complete |
| Optimizer | 200 | 719 | ⚠️ Partial |
| Plan Cache | 100 | 200 | ⚠️ Partial |
| Register API | 50 | 100 | ✅ C++ complete |
| **Total** | **1600** | **3174** | **50% complete** |

### Migration Tasks

1. **[x] C++ Compiler** - `src/core/CG/compiler.cpp/h`
   - Statement compilation
   - Register allocation
   - Bytecode emission

2. **[x] C++ Expr Compiler** - `src/core/CG/expr_compiler.cpp/h`
   - Expression tree compilation
   - Function call compilation

3. **[x] C++ Bytecode Compiler** - `src/core/CG/bytecode_compiler.cpp/h`
   - VM instruction emission
   - Opcode selection

4. **[ ] C++ Optimizer** - `src/core/CG/optimizer.cpp/h` (enhance existing)
   - Bytecode optimization
   - Dead code elimination
   - Register coalescing

5. **[ ] C++ Plan Cache** - `src/core/CG/plan_cache.cpp/h` (enhance existing)
   - Prepared statement caching
   - Plan invalidation

6. **[ ] Go Wrapper** - `internal/CG/cg_cgo.go` (NEW)
   - Compile() wrapper
   - Optimize() wrapper
   - Cache wrapper

### Go Files to Reduce

| File | Current | Target | Notes |
|------|---------|--------|-------|
| `compiler.go` | 1315 LOC | 100 LOC | Move logic to C++ |
| `bytecode_compiler.go` | 340 LOC | 50 LOC | Wrap C++ |
| `expr_compiler.go` | 300 LOC | 50 LOC | Wrap C++ |
| `optimizer.go` | 719 LOC | 50 LOC | Move logic to C++ |
| `plan_cache.go` | 200 LOC | 50 LOC | Wrap C++ |
| `direct_compiler.go` | 200 LOC | 50 LOC | Wrap C++ |

**Reduction**: 3174 LOC → 400 LOC (87% reduction)

---

## Phase 3.3: Transaction Management (TM) Migration

**Duration**: 2026-03-25 to 2026-04-01 (1 week)
**Priority**: High

### Current State

| Component | C++ LOC | Go LOC | Status |
|-----------|---------|--------|--------|
| Transaction | 300 | 404 | ⚠️ Partial |
| MVCC | 0 | 400 | ❌ Go only |
| Lock Manager | 0 | 350 | ❌ Go only |
| Isolation | 0 | 200 | ❌ Go only |
| WAL Coordination | 200 | 371 | ⚠️ Partial |
| **Total** | **500** | **1725** | **25% complete** |

### Migration Tasks

1. **[ ] C++ MVCC Engine** - `src/core/TM/mvcc.cpp/h` (NEW)
   - Versioned value storage
   - Snapshot isolation
   - Garbage collection

2. **[ ] C++ Lock Manager** - `src/core/TM/lock_manager.cpp/h` (NEW)
   - Read/write locks
   - Lock escalation
   - Deadlock detection

3. **[ ] C++ Transaction** - `src/core/TM/transaction.cpp/h` (enhance existing)
   - Transaction lifecycle
   - Commit/rollback
   - Savepoints

4. **[ ] C++ Isolation** - `src/core/TM/isolation.cpp/h` (NEW)
   - Isolation level enforcement
   - Serializable conflict detection

5. **[ ] Go Wrapper** - `internal/TM/tm_cgo.go` (NEW)
   - Transaction wrapper
   - MVCC wrapper
   - Lock wrapper

### Go Files to Reduce

| File | Current | Target | Notes |
|------|---------|--------|-------|
| `transaction.go` | 404 LOC | 50 LOC | Wrap C++ |
| `mvcc.go` | 400 LOC | 50 LOC | Move logic to C++ |
| `lock.go` | 350 LOC | 50 LOC | Move logic to C++ |
| `isolation.go` | 200 LOC | 50 LOC | Move logic to C++ |
| `wal.go` | 371 LOC | 50 LOC | Wrap C++ coordination |

**Reduction**: 1725 LOC → 250 LOC (85% reduction)

---

## Integration Strategy

### Week 1 (2026-03-11): QP Tokenizer/Parser

1. Create `internal/QP/qp_cgo.go` wrapper
2. Migrate `Tokenize()` to C++ call
3. Migrate `Parse()` to C++ call
4. Update all QP tests
5. Benchmark tokenizer performance

### Week 2 (2026-03-18): QP Optimizer + CG Compiler

1. Create C++ optimizer (`src/core/QP/optimizer.cpp`)
2. Migrate `Optimize()` to C++ call
3. Create `internal/CG/cg_cgo.go` wrapper
4. Migrate `Compile()` to C++ call
5. Update all CG tests

### Week 3 (2026-03-25): TM MVCC/Locks

1. Create C++ MVCC engine (`src/core/TM/mvcc.cpp`)
2. Create C++ lock manager (`src/core/TM/lock_manager.cpp`)
3. Create `internal/TM/tm_cgo.go` wrapper
4. Migrate transaction operations to C++
5. Update all TM tests

### Week 4 (2026-04-01): Integration + Testing

1. Full integration testing
2. Performance benchmarking
3. Fix any regressions
4. Documentation update
5. Code review

---

## Performance Targets

### QP Benchmarks

| Operation | Current (Go) | Target (C++) | Improvement |
|-----------|--------------|--------------|-------------|
| Tokenize 1KB SQL | 50 µs | <20 µs | 60% faster |
| Parse SELECT | 200 µs | <100 µs | 50% faster |
| Optimize query | 500 µs | <300 µs | 40% faster |

### CG Benchmarks

| Operation | Current (Go) | Target (C++) | Improvement |
|-----------|--------------|--------------|-------------|
| Compile SELECT | 1 ms | <500 µs | 50% faster |
| Compile aggregate | 2 ms | <1 ms | 50% faster |
| Plan cache hit | 10 µs | <5 µs | 50% faster |

### TM Benchmarks

| Operation | Current (Go) | Target (C++) | Improvement |
|-----------|--------------|--------------|-------------|
| Begin transaction | 5 µs | <2 µs | 60% faster |
| Commit transaction | 50 µs | <30 µs | 40% faster |
| MVCC read | 100 ns | <50 ns | 50% faster |
| Lock acquisition | 200 ns | <100 ns | 50% faster |

---

## Risks & Mitigations

### High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| MVCC correctness | High | Extensive testing, parallel run with Go |
| Deadlock detection | High | Port Go algorithm exactly, add tests |
| Query optimizer bugs | High | Keep Go optimizer as fallback initially |

### Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| AST type mismatches | Medium | Keep Go AST types, C++ produces Go-compatible |
| CGO overhead in hot path | Medium | Batch operations, minimize crossings |
| Plan cache invalidation | Medium | Careful testing of DDL invalidation |

### Low Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Tokenizer edge cases | Low | Port all Go tests to C++ |
| Parser error messages | Low | Match Go error format exactly |

---

## Success Criteria

### Functional

- [ ] All SQL:1999 test suites passing
- [ ] All optimizer tests passing
- [ ] All compiler tests passing
- [ ] All transaction tests passing
- [ ] MVCC isolation levels working correctly

### Performance

- [ ] No benchmark regression >5%
- [ ] At least 30% improvement on 5+ benchmarks
- [ ] Query compilation <1ms for simple queries
- [ ] Transaction overhead <100µs

### Code Quality

- [ ] >80% test coverage for new C++ code
- [ ] All Go wrappers <1000 LOC total
- [ ] Documentation updated
- [ ] Migration guide written

---

## Timeline Summary

| Phase | Component | Start | End | Duration |
|-------|-----------|-------|-----|----------|
| 3.1 | QP Migration | 2026-03-11 | 2026-03-18 | 1 week |
| 3.2 | CG Migration | 2026-03-18 | 2026-03-25 | 1 week |
| 3.3 | TM Migration | 2026-03-25 | 2026-04-01 | 1 week |
| 3.4 | Integration | 2026-04-01 | 2026-04-08 | 1 week |

**Total Phase 3 Duration**: 4 weeks
**Phase 3 Completion**: 2026-04-08

---

## Next Steps

### Immediate (Week of 2026-03-11)

1. [ ] Create `internal/QP/qp_cgo.go` wrapper
2. [ ] Migrate `Tokenize()` to C++
3. [ ] Migrate `Parse()` to C++
4. [ ] Write QP CGO tests
5. [ ] Benchmark QP operations

### Week of 2026-03-18

1. [ ] Create C++ optimizer
2. [ ] Migrate `Compile()` to C++
3. [ ] Create `internal/CG/cg_cgo.go` wrapper
4. [ ] Write CG CGO tests
5. [ ] Benchmark CG operations

### Week of 2026-03-25

1. [ ] Create C++ MVCC engine
2. [ ] Create C++ lock manager
3. [ ] Create `internal/TM/tm_cgo.go` wrapper
4. [ ] Write TM CGO tests
5. [ ] Benchmark TM operations

---

**Document Version**: 1.0
**Created**: 2026-03-04
**Maintainer**: sqlvibe team
**Next Review**: 2026-03-11
