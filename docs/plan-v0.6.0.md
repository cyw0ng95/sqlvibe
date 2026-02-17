# Plan v0.6.0 - Complete BTree Implementation

## Goal
Complete the SQLite-compatible BTree implementation started in v0.5.0, focusing on the remaining critical components: overflow pages, page balancing, and freelist management.

---

## Current State (v0.5.0 Completion)

### ✅ Completed in v0.5.0
- Wave 6: CG (Code Generator) subsystem
- Wave 7: VFS (Virtual File System) architecture
- Wave 8 (Partial): BTree encoding infrastructure
  - Varint encoding/decoding (397 lines)
  - Record format encoding/decoding
  - Cell format for all 4 page types (311 lines)
  - Comprehensive test suites (all passing)

### 📊 BTree Progress
- **Completed**: ~980 lines (33-49% of target)
- **Remaining**: ~1200-1900 lines (51-67%)

---

## Wave 9: Complete BTree Implementation

### Task 9.1: Overflow Page Management
**Priority**: MEDIUM
**Effort**: ~300-400 lines

**Description**: Implement overflow page reading and writing for large payloads

**Files**:
- `internal/DS/overflow.go`
- `internal/DS/overflow_test.go`

**Details**:
- Overflow page structure (next page pointer + data)
- Split payload across multiple overflow pages
- Reassemble payload from overflow chain
- Integrate with cell encoding
- Handle overflow page allocation/deallocation

### Task 9.2: Page Balancing Algorithms
**Priority**: **CRITICAL**
**Effort**: ~500-800 lines

**Description**: Implement page split/merge/redistribute for tree maintenance

**Files**:
- `internal/DS/balance.go`
- `internal/DS/balance_test.go`

**Details**:
- **Split**: Divide overfull page into two pages
- **Merge**: Combine underfull sibling pages
- **Redistribute**: Move cells between siblings to balance load
- Update parent divider keys
- Handle root page promotion/demotion
- Maintain minimum fill factor
- Test with various insertion/deletion patterns

**Algorithms**:
```
Split(page):
  1. Allocate new page
  2. Distribute cells evenly
  3. Update parent with new divider key
  4. If parent full, recursively split parent

Merge(page, sibling):
  1. Copy all cells from sibling to page
  2. Update parent (remove divider, pointer)
  3. Deallocate sibling page
  4. If parent underfull, recursively balance parent

Redistribute(page, sibling):
  1. Calculate optimal distribution
  2. Move cells between pages
  3. Update parent divider key
```

### Task 9.3: Freelist Management
**Priority**: LOW (can defer further if needed)
**Effort**: ~300-400 lines

**Description**: Implement free page management

**Files**:
- `internal/DS/freelist.go`
- `internal/DS/freelist_test.go`

**Details**:
- Freelist trunk page structure
- Freelist leaf page structure
- Allocate page from freelist
- Return page to freelist
- Freelist compaction
- Database file size management

### Task 9.4: BTree Integration
**Priority**: **CRITICAL**
**Effort**: ~200-300 lines

**Description**: Integrate new encoding with existing BTree operations

**Files**:
- `internal/DS/btree.go` (updates)
- `internal/DS/btree_test.go` (updates)

**Details**:
- Update Insert() to use new cell encoding
- Update Delete() to use new cell encoding
- Update Search() to use new cell decoding
- Add overflow support to all operations
- Integrate page balancing on Insert/Delete
- Update cursor operations
- Maintain backwards compatibility where possible

### Task 9.5: Comprehensive Testing
**Priority**: HIGH
**Effort**: ~100-200 lines

**Description**: End-to-end testing and SQLite compatibility validation

**Files**:
- `internal/DS/integration_test.go` (new)
- `test/btree_compat_test.go` (new)

**Details**:
- Large dataset tests (10k+ rows)
- Random insert/delete patterns
- Verify tree structure integrity
- Compare with SQLite behavior
- Performance benchmarks
- Memory usage profiling

---

## Wave 10: WHERE Clause Operators

### Task 10.1: Implement Remaining WHERE Operators
**Priority**: HIGH
**Effort**: ~200-300 lines

**Description**: Complete WHERE clause operators with stub implementations

**Files**:
- `internal/VM/compiler.go` (updates)
- `internal/VM/exec.go` (updates)

**Operators to Implement**:
- `OR`: Logical OR (currently returns 1 row instead of 2)
- `IN`: Value in list (currently returns 0 rows instead of 3)
- `BETWEEN`: Range check (currently returns 5 rows instead of 2)
- `LIKE`: Pattern matching (currently returns 0 rows instead of 1)
- `IS NULL`: NULL check (currently returns 4 rows instead of 1)

**Details**:
- Implement proper bytecode generation
- Add jump targets for short-circuit evaluation
- Test with SQLite comparison tests
- Ensure all 13/13 WHERE operators pass

---

## Success Criteria

- [ ] All BTree operations use new encoding (varint, record, cell)
- [ ] Overflow pages working for large payloads (>page size)
- [ ] Page balancing maintains tree integrity
- [ ] Freelist reduces database file growth
- [ ] All WHERE operators passing (13/13)
- [ ] SQLite compatibility tests passing
- [ ] Performance comparable to v0.4.x baseline
- [ ] No regressions in existing tests

---

## Dependencies

**Wave 9 → Wave 10**: Sequential
- BTree completion enables better testing
- WHERE operators can proceed in parallel after Task 9.4

**Risk**: Page balancing (Task 9.2) is complex
- **Mitigation**: Start with simple split algorithm
- Defer complex rebalancing to v0.7.0 if needed

---

## Timeline Estimate

- **Task 9.1** (Overflow): 4-6 hours
- **Task 9.2** (Balancing): 8-12 hours ⚠️ Critical path
- **Task 9.3** (Freelist): 4-6 hours
- **Task 9.4** (Integration): 3-5 hours
- **Task 9.5** (Testing): 2-4 hours
- **Task 10.1** (WHERE): 3-5 hours

**Total**: 24-38 hours (3-5 days of focused work)

---

## Notes

- v0.5.0 delivered solid foundation (~980 lines of encoding infrastructure)
- v0.6.0 focuses on completing BTree to production quality
- Page balancing is the most complex component - allocate adequate time
- Consider incremental releases: v0.6.0 (balancing), v0.6.1 (overflow), v0.6.2 (freelist)
