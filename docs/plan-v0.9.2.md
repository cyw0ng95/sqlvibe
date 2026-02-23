# Plan v0.9.2 - Remaining Optimizations & Stabilization

## Summary

This plan captures remaining tasks from v0.9.1 that were not completed, plus additional stabilization work.

## Remaining Tasks from v0.9.1

### Phase 1: Covering Index (Incomplete)

- [ ] Modify `OpOpenRead` to detect covering index opportunity
- [ ] Implement `executeCoveringScan` in VM
- [ ] Add `idx_covering` virtual column to track index entries

### Phase 2: Column Projection (Incomplete)

- [ ] Modify `OpColumn` to use projected column data
- [ ] Update query execution to pass required columns to storage

### Phase 3: Index Skip Scan (Incomplete)

- [ ] Add `CompositeIndex` struct to `internal/DS/index_engine.go`
- [ ] Modify index selection to consider skip scan
- [ ] Track column cardinality in `ANALYZE`

### Phase 4: Slab Allocator (Incomplete)

- [ ] Integrate slab allocator into VM
- [ ] Replace row buffer allocations with slab allocations
- [ ] Add allocator stats to `PRAGMA storage_info`

### Phase 5: Prepared Statement Pool (Incomplete)

- [ ] Create `pkg/sqlvibe/statement.go` with `PreparedStatement` struct
- [ ] Add parameter binding support to VM
- [ ] Add `?` placeholder parsing in tokenizer
- [ ] Update Database API documentation

### Phase 6: Direct Threaded VM (Incomplete)

- [ ] Extract all opcode handlers to individual functions
- [ ] Refactor `Exec()` to use dispatch table

### Phase 7: Query Compilation Pipeline (Incomplete)

- [ ] Implement single-pass SELECT compilation
- [ ] Implement single-pass INSERT/UPDATE/DELETE
- [ ] Integrate with existing Database API

### Phase 8: Expression Bytecode (Incomplete)

- [ ] Create `internal/VM/expr_vectorized.go` with vectorized evaluation
- [ ] Add `OpExprEval` opcode to VM
- [ ] Modify CG to use expression bytecode for complex expressions

---

## New Features for v0.9.2

### Bug Fixes

- [ ] Fix JULIANDAY function returning nil for certain queries
- [ ] Fix ROUND function not working with Julianday results

### Performance Improvements

- [ ] Optimize string comparison operations
- [ ] Add more opcodes to dispatch table

### Testing

- [ ] Add regression tests for date/time functions
- [ ] Add integration tests for prepared statements

---

## Timeline Estimate

| Category | Tasks | Hours |
|----------|-------|-------|
| Covering Index (remaining) | 3 | 4h |
| Column Projection (remaining) | 2 | 3h |
| Index Skip Scan (remaining) | 3 | 5h |
| Slab Allocator (remaining) | 3 | 4h |
| Prepared Statement (remaining) | 4 | 6h |
| Direct Threaded VM (remaining) | 2 | 4h |
| Query Pipeline (remaining) | 3 | 6h |
| Expression Bytecode (remaining) | 3 | 5h |
| Bug Fixes | 2 | 2h |
| Testing | 2 | 3h |

**Total:** ~42 hours

---

## Success Criteria

- [ ] All covering index tasks completed
- [ ] All column projection tasks completed
- [ ] All skip scan tasks completed
- [ ] All slab allocator integration tasks completed
- [ ] Prepared statements fully functional
- [ ] Direct threaded VM fully operational
- [ ] Direct compiler integrated
- [ ] Expression bytecode vectorized evaluation working
- [ ] JULIANDAY and ROUND bugs fixed
- [ ] All tests passing
