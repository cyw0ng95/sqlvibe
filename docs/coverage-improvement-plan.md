# Coverage Improvement Plan v0.10.1

## Current Coverage Status

| Package | Coverage | Status |
|---------|----------|--------|
| internal/TM | 85.8% | Excellent |
| internal/PB | 82.4% | Excellent |
| internal/IS | 77.0% | Good |
| internal/DS | 53.1% | Moderate |
| ext | 48.4% | Moderate |
| pkg/sqlvibe | 23.0% | Low |
| internal/QP | 11.2% | Very Low |
| internal/CG | 6.8% | Very Low |
| internal/VM | 5.1% | Very Low |
| ext/json | 0.0% | None |

**Overall: 63.8%**

---

## Priority 1: Critical (Very Low Coverage)

### 1. internal/VM (5.1%)

**Files needing tests:**
- `internal/VM/engine.go` - Core database engine
- `internal/VM/exec.go` - Execution engine
- `internal/VM/compiler.go` - SQL compiler
- `internal/VM/query_engine.go` - Query execution
- `internal/VM/expr_eval.go` - Expression evaluation
- `internal/VM/row_eval.go` - Row evaluation
- `internal/VM/cursor.go` - Cursor management
- `internal/VM/registers.go` - Register allocation
- `internal/VM/program.go` - Program management

**Target: 40%**

---

### 2. internal/CG (6.8%)

**Files needing tests:**
- `internal/CG/compiler.go` - Main compiler
- `internal/CG/expr.go` - Expression compilation
- `internal/CG/direct_compiler.go` - Direct compilation
- `internal/CG/optimizer.go` - Query optimizer
- `internal/CG/plan_cache.go` - Plan caching
- `internal/CG/bytecode_compiler.go` - Bytecode compiler

**Target: 35%**

---

### 3. internal/QP (11.2%)

**Files needing tests:**
- `internal/QP/analyzer.go` - Query analyzer
- `internal/QP/optimizer.go` - Query optimizer
- `internal/QP/binder.go` - Binding
- `internal/QP/normalize.go` - Normalization
- `internal/QP/dag.go` - DAG operations
- `internal/QP/tokenizer.go` - SQL tokenizer

**Target: 35%**

---

## Priority 2: Important (Low Coverage)

### 4. pkg/sqlvibe (23.0%)

**Files needing tests:**
- `pkg/sqlvibe/database.go` - Main database API
- `pkg/sqlvibe/query.go` - Query execution
- `pkg/sqlvibe/statement.go` - Statement handling

**Target: 45%**

---

### 5. ext/json (0.0%)

**Files needing tests:**
- `ext/json/json.go` - JSON extension functions

**Priority: Lower (JSON extension is optional)**

---

## Priority 3: Maintain (Good Coverage)

### 6. internal/DS (53.1%)

**Files needing more tests:**
- `internal/DS/btree.go` - B-Tree operations
- `internal/DS/balance.go` - Tree balancing
- `internal/DS/compact.go` - Compaction
- `internal/DS/compression.go` - Compression

**Target: 65%**

---

### 7. ext (48.4%)

**Target: 60%**

---

## Implementation Strategy

### Phase 1: VM Coverage (Week 1-2)
1. Add `internal/VM/engine_test.go` - Database lifecycle tests
2. Add `internal/VM/exec_test.go` - Execution tests
3. Add `internal/VM/compiler_test.go` - Compiler tests
4. Add `internal/VM/cursor_test.go` - Cursor tests
5. Add `internal/VM/registers_test.go` - Register tests

### Phase 2: CG + QP Coverage (Week 3-4)
1. Add `internal/CG/compiler_test.go` - Compiler tests
2. Add `internal/CG/expr_test.go` - Expression tests
3. Add `internal/QP/analyzer_test.go` - Analyzer tests
4. Add `internal/QP/tokenizer_test.go` - Tokenizer tests

### Phase 3: API + Extensions (Week 5-6)
1. Add `pkg/sqlvibe/database_test.go` - Database API tests
2. Add `ext/json/json_test.go` - JSON tests (reuse existing)

---

## Success Criteria

- [ ] internal/VM: 5.1% -> 40%
- [ ] internal/CG: 6.8% -> 35%
- [ ] internal/QP: 11.2% -> 35%
- [ ] pkg/sqlvibe: 23.0% -> 45%
- [ ] Overall: 63.8% -> 75%
