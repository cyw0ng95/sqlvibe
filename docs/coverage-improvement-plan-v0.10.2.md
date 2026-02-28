# Coverage Improvement Plan v0.10.2

## Current Coverage Status

| Package | Coverage | Status | Priority |
|---------|----------|--------|----------|
| internal/SF/util | 100.0% | Perfect | - |
| internal/SF | 88.2% | Excellent | - |
| internal/TM | 86.0% | Excellent | - |
| internal/IS | 77.0% | Good | - |
| internal/PB | 82.4% | Excellent | - |
| ext/math | 76.4% | Good | - |
| internal/DS | 53.1% | Medium | Medium |
| ext | 48.4% | Medium | Medium |
| ext/json | 35.9% | Medium | Medium |
| pkg/sqlvibe | 23.1% | Low | **High** |
| internal/CG | 15.5% | Low | **High** |
| internal/QP | 14.7% | Low | **High** |
| internal/VM | 10.6% | Low | **High** |
| driver | 2.1% | Very Low | Medium |
| cmd/sv-check | 0.0% | None | Low |
| cmd/sv-cli | 0.0% | None | Low |
| internal/SF/vfs | 0.0% | None | Low |

**Overall: 64.2%**

---

## Priority 1: High Impact (Critical)

### 1. internal/VM (10.6%)

**Target: 40%**

**Key files needing tests:**
- `exec.go` - VM execution engine (200+ opcodes)
- `engine.go` - Database engine
- `query_engine.go` - Query execution
- `expr_eval.go` - Expression evaluation
- `row_eval.go` - Row evaluation
- `cursor.go` - Cursor management

**Test strategy:**
- Test each opcode handler (OpLoadConst, OpAdd, OpEq, OpLt, etc.)
- Test cursor operations with different data types
- Test expression evaluation with various expression types

**Estimated improvement: +25%**

---

### 2. internal/CG (15.5%)

**Target: 35%**

**Key files needing tests:**
- `compiler.go` - Main compiler (SELECT, INSERT, UPDATE, DELETE)
- `expr.go` - Expression compilation
- `direct_compiler.go` - Fast path compilation
- `optimizer.go` - Query optimization

**Test strategy:**
- Test compilation of various SQL statements
- Test expression compilation (binary, unary, functions)
- Test optimizer passes (constant folding, dead code elimination)

**Estimated improvement: +20%**

---

### 3. internal/QP (14.7%)

**Target: 35%**

**Key files needing tests:**
- `parser.go` - SQL parser
- `tokenizer.go` - SQL tokenizer
- `analyzer.go` - Query analysis
- `optimizer.go` - Query optimization
- `binder.go` - Expression binding
- `normalize.go` - Query normalization

**Test strategy:**
- Test parser for all SQL statement types
- Test tokenizer for all SQL tokens
- Test analysis functions (RequiredColumns, etc.)
- Test predicate pushdown optimization

**Estimated improvement: +20%**

---

### 4. pkg/sqlvibe (23.1%)

**Target: 45%**

**Key files needing tests:**
- `database.go` - Main database API
- `query.go` - Query execution
- `statement.go` - Statement handling
- `exec_columnar.go` - Columnar execution
- `vm_exec.go` - VM-based execution

**Test strategy:**
- Test database lifecycle (open, close, pragma)
- Test various query types (SELECT, INSERT, UPDATE, DELETE)
- Test transaction handling
- Test result set handling

**Estimated improvement: +22%**

---

## Priority 2: Medium Impact

### 5. internal/DS (53.1%)

**Target: 65%**

**Key files needing tests:**
- `btree.go` - B-Tree operations
- `balance.go` - Tree balancing
- `compact.go` - Compaction
- `compression.go` - Compression

**Estimated improvement: +12%**

---

### 6. ext/json (35.9%)

**Target: 60%**

**Test JSON functions:**
- json_each, json_tree
- json_extract, json_type
- json_valid, json_quote

**Estimated improvement: +24%**

---

### 7. ext (48.4%)

**Target: 60%**

**Test extension framework:**
- Extension registration
- Function dispatch
- Type coercion

**Estimated improvement: +12%**

---

## Priority 3: Low Priority

### 8. driver (2.1%)

**Target: 30%**

**Test database/sql driver:**
- Connection pooling
- Statement preparation
- Transaction handling

---

### 9. cmd/sv-cli, cmd/sv-check (0%)

**Target: 20%**

**Test CLI tools:**
- Command line parsing
- Output formatting

---

### 10. internal/SF/vfs (0%)

**Target: 30%**

**Test VFS interface:**
- File operations
- Path handling

---

## Implementation Phases

### Phase 1: VM + CG + QP (Weeks 1-3)

1. **internal/VM tests**
   - Add `exec_test.go` with opcode tests
   - Add `cursor_ops_test.go` for cursor operations
   - Add `expr_eval_test.go` for expression evaluation

2. **internal/CG tests**
   - Add `compiler_select_test.go` for SELECT compilation
   - Add `compiler_dml_test.go` for INSERT/UPDATE/DELETE
   - Add `expr_compile_test.go` for expression compilation

3. **internal/QP tests**
   - Add `parser_test.go` for SQL parsing
   - Add `tokenizer_test.go` for tokenization
   - Add `analyzer_test.go` for query analysis

### Phase 2: API + Extensions (Weeks 4-5)

4. **pkg/sqlvibe tests**
   - Add `database_test.go` for DB lifecycle
   - Add `query_test.go` for query execution
   - Add `transaction_test.go` for transactions

5. **ext/json tests**
   - Add `json_func_test.go` for JSON functions

### Phase 3: Remaining Packages (Week 6)

6. **internal/DS tests**
   - Add `btree_ops_test.go` for B-Tree operations

7. **driver tests**
   - Add `driver_conn_test.go` for connection handling

---

## Success Criteria

| Package | Current | Target | Priority |
|---------|---------|--------|----------|
| internal/VM | 10.6% | 40% | Critical |
| internal/CG | 15.5% | 35% | Critical |
| internal/QP | 14.7% | 35% | Critical |
| pkg/sqlvibe | 23.1% | 45% | High |
| internal/DS | 53.1% | 65% | Medium |
| ext/json | 35.9% | 60% | Medium |
| ext | 48.4% | 60% | Medium |
| Overall | 64.2% | 75% | Goal |

---

## Notes

- Main focus on core engine packages (VM, CG, QP)
- These packages contain the critical path for query execution
- High coverage in these packages will significantly improve overall reliability
- Tests should focus on edge cases and error conditions
