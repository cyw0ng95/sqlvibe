# SQL1999 Test Failure Fix Plan

**Date**: 2026-02-18
**Version**: v0.5.0 Plan
**Goal**: Fix critical SQL1999 test failures to achieve 75%+ pass rate

## Current Status

- **Total Test Suites**: 16
- **Total Subtests**: 91
- **Passed**: 45 (49.5%)
- **Failed**: 38 (41.8%)
- **Skipped**: 8 (8.8%)
- **Target**: 75%+ pass rate

## Fix Priority Matrix

### P0 - Critical Blockers (Immediate Fix Required)

| ID | Issue | Suite | Impact | Effort | Priority |
|----|-------|-------|--------|--------|----------|
| P0-1 | E101 syntax error | E101 | Blocks entire suite | 5 min | CRITICAL |
| P0-2 | LEFT OUTER JOIN | E091, E041 | Blocks JOIN tests | 2-3 days | CRITICAL |
| P0-3 | JOIN column reading | E041 | All JOIN queries fail | 1-2 days | CRITICAL |
| P0-4 | information_schema | E031 | All metadata queries fail | 2-3 days | CRITICAL |

### P1 - High Priority (Short Term)

| ID | Issue | Suite | Impact | Effort | Priority |
|----|-------|-------|--------|--------|----------|
| P1-1 | Subquery compilation | E071 | 0% pass rate | 2-3 days | HIGH |
| P1-2 | NULL handling | E131 | 0% pass rate | 2-3 days | HIGH |
| P1-3 | E051 query issues | E051 | 50% pass rate | 1 day | HIGH |

### P2 - Medium Priority (Medium Term)

| ID | Issue | Suite | Impact | Effort | Priority |
|----|-------|-------|--------|--------|----------|
| P2-1 | View support | E171 | 0% pass rate | 3-5 days | MEDIUM |
| P2-2 | UPDATE/DELETE | E153 | 0% pass rate | 1-2 days | MEDIUM |
| P2-3 | Predicate handling | E061 | 25% pass rate | 1-2 days | MEDIUM |

### P3 - Low Priority (Long Term)

| ID | Issue | Suite | Impact | Effort | Priority |
|----|-------|-------|--------|--------|----------|
| P3-1 | Privilege system | E081 | 0% pass rate | 1-2 weeks | LOW |

## Detailed Fix Plans

### P0-1: Fix E101 Syntax Error

**File**: `internal/TS/SQL1999/E101/07_test.go:6:45`
**Error**: `string literal not terminated`

**Steps**:
1. Read `internal/TS/SQL1999/E101/07_test.go`
2. Locate line 6, column 45
3. Fix unterminated string literal
4. Verify: `go test ./internal/TS/SQL1999/E101/... -v`

**Expected Impact**:
- Unblocks entire E101 suite
- +X tests (to be determined after fix)
- Time: 5 minutes

---

### P0-2: Implement LEFT/RIGHT/FULL OUTER JOIN

**File**: `internal/VM/compiler.go` (compileJoin function)
**Current Status**: Panics with "JOIN type 'LEFT' is not yet implemented"

**Steps**:

#### Phase 1: LEFT OUTER JOIN
1. Modify `compileJoin` to handle `LEFT` join type
2. Implement LEFT JOIN logic in VM:
   - For each left row, find matching right rows
   - If no match, emit row with NULLs for right table columns
   - Use OpNull instruction for NULL values
3. Update VM instructions if needed:
   - Add OpNull or use existing NULL handling
   - Ensure proper cursor management

#### Phase 2: RIGHT OUTER JOIN
1. Implement RIGHT JOIN as reversed LEFT JOIN
2. Or implement separately with similar logic
3. Test right table row emission when no left match

#### Phase 3: FULL OUTER JOIN
1. Combine LEFT and RIGHT JOIN logic
2. Emit all rows from both tables
3. Use NULLs where no match exists

**Testing**:
1. `go test ./internal/TS/SQL1999/E091/... -v -run TestSQL1999_F301_E09103_L1`
2. Verify LEFT JOIN with no matches returns NULLs
3. Verify RIGHT JOIN behavior
4. Verify FULL JOIN behavior

**Expected Impact**:
- Fixes E091/03 (LEFT OUTER JOIN)
- Enables many E041 JOIN tests
- +5-10 tests
- Time: 2-3 days

---

### P0-3: Fix JOIN Column Reading

**File**: `internal/VM/compiler.go` (compileColumnRef, compileJoin)
**Root Cause**: `TableColIndices` only contains schema for first table

**Steps**:

#### Phase 1: Multi-Table Schema Tracking
1. Modify compiler to track schema for all tables in FROM clause
2. Create `map[string][]Schema` mapping table names to column schemas
3. Pass this map to `compileColumnRef`

#### Phase 2: Cursor-to-Table Mapping
1. Maintain mapping: `cursorID -> tableName -> columnSchema`
2. When compiling `table.column`, lookup correct cursor ID
3. Get column index from correct table's schema

#### Phase 3: Dynamic Column Lookup
1. Modify `compileColumnRef` to:
   - Parse qualified column name (table.column or just column)
   - Find which cursor owns the column
   - Get correct column index for that cursor
2. Update `OpColumn` to use cursor-specific column indices

**Alternative Approach**:
1. Flatten schema at compile time
2. Use global column indices across all cursors
3. Track which cursor each column belongs to

**Testing**:
1. `go test ./internal/TS/SQL1999/E041/... -v`
2. Verify INNER JOIN returns correct data
3. Verify CROSS JOIN returns correct data
4. Verify qualified column names work

**Expected Impact**:
- Fixes most E041 tests (0/12 passing → 8-10/12)
- Fixes JOIN queries across all suites
- +15-20 tests
- Time: 1-2 days

---

### P0-4: Implement information_schema Tables

**Files**: `internal/DS/schema.go`, `internal/VM/virtual_tables.go` (new)
**Current Status**: `information_schema.tables` queries fail

**Steps**:

#### Phase 1: information_schema Virtual Tables
1. Create virtual table interface
2. Implement `information_schema.tables`:
   - table_name
   - table_schema
   - table_type
   - is_view
3. Query from catalog metadata

#### Phase 2: Column Information
1. Implement `information_schema.columns`:
   - table_name
   - column_name
   - ordinal_position
   - data_type
   - is_nullable
   - column_default
2. Read from table schemas

#### Phase 3: Constraint Information
1. Implement `information_schema.key_column_usage`:
   - table_name
   - column_name
   - constraint_name
   - referenced_table_name
   - referenced_column_name
2. Read from foreign key constraints

#### Phase 4: Other Metadata Tables
1. `information_schema.referential_constraints`
2. `information_schema.table_constraints`
3. Any other required tables

**Implementation Details**:
1. Create virtual table reader that scans metadata
2. Return rows matching query conditions
3. Handle WHERE clause filtering on metadata

**Testing**:
1. `go test ./internal/TS/SQL1999/E031/... -v`
2. Verify all E031 tests pass
3. Verify metadata queries work

**Expected Impact**:
- Fixes all E031 tests (0/6 → 6/6)
- +6 tests
- Time: 2-3 days

---

### P1-1: Fix Subquery Compilation

**File**: `internal/CG/compiler.go`, `internal/VM/compiler.go`
**Current Status**: E071 has 0% pass rate

**Steps**:

#### Phase 1: Subquery Parsing
1. Ensure parser handles subqueries correctly
2. Test with simple EXISTS, IN, scalar subqueries

#### Phase 2: Subquery Compilation
1. Compile subquery as separate program
2. Pass subquery result to outer query
3. Handle correlated subqueries (reference outer query columns)

#### Phase 3: Subquery Execution
1. Execute subquery first or during iteration
2. Handle subquery result caching
3. Implement correlated subquery evaluation

**Testing**:
1. `go test ./internal/TS/SQL1999/E071/... -v`
2. Test EXISTS subqueries
3. Test IN subqueries
4. Test scalar subqueries

**Expected Impact**:
- Fixes most E071 tests (0/6 → 4-6/6)
- +4-6 tests
- Time: 2-3 days

---

### P1-2: Improve NULL Handling

**Files**: `internal/VM/executor.go`, `internal/VM/ops.go`
**Current Status**: E131 has 0% pass rate

**Steps**:

#### Phase 1: NULL in Expressions
1. Ensure NULL propagates through arithmetic
2. NULL + value = NULL
3. NULL * value = NULL

#### Phase 2: NULL in Comparisons
1. NULL = value → NULL (not true or false)
2. NULL != value → NULL
3. Implement three-valued logic (TRUE, FALSE, UNKNOWN)

#### Phase 3: NULL in Aggregates
1. COUNT(*) counts all rows including NULLs
2. COUNT(column) counts non-NULL values
3. SUM/AVG ignore NULL values

#### Phase 4: NULL in Functions
1. COALESCE(NULL, value) → value
2. Other NULL handling functions

**Testing**:
1. `go test ./internal/TS/SQL1999/E131/... -v`
2. Test NULL in arithmetic
3. Test NULL in comparisons
4. Test NULL in aggregates

**Expected Impact**:
- Fixes most E131 tests (0/7 → 5-7/7)
- +5-7 tests
- Time: 2-3 days

---

### P1-3: Fix E051 Query Issues

**Files**: `internal/CG/compiler.go`, `internal/VM/compiler.go`
**Current Status**: E051 has 50% pass rate

**Steps**:

1. Investigate failing E051 tests
2. Fix type coercion issues
3. Fix SELECT clause edge cases
4. Improve query planning

**Testing**:
1. `go test ./internal/TS/SQL1999/E051/... -v`
2. Verify all E051 tests pass

**Expected Impact**:
- Fixes E051 tests (3/6 → 6/6)
- +3 tests
- Time: 1 day

---

## Implementation Order

### Week 1: Critical Blockers (P0)
- Day 1: P0-1 (E101 syntax error)
- Day 2-4: P0-3 (JOIN column reading)
- Day 5-7: P0-2 (OUTER JOIN)

### Week 2: High Priority (P1) + P0-4
- Day 1-3: P0-4 (information_schema)
- Day 4-6: P1-1 (Subqueries)
- Day 7: P1-3 (E051)

### Week 3: Medium Priority (P2) + P1-2
- Day 1-3: P1-2 (NULL handling)
- Day 4-6: P2-3 (Predicates)
- Day 7: P2-2 (UPDATE/DELETE)

### Week 4+: Low Priority (P3)
- P2-1: Views (if time permits)
- P3-1: Privilege system (future work)

## Expected Progress

| Version | Date | Pass Rate | Tests Added |
|---------|------|-----------|-------------|
| v0.5.0 | Week 2 end | 75% | +25-30 |
| v0.5.1 | Week 3 end | 85% | +10-15 |
| v0.6.0 | Week 4 end | 90% | +5-10 |
| v0.7.0+ | Future | 95%+ | +5 |

## Testing Strategy

### Before Each Fix
1. Run full test suite to establish baseline
2. Record failures
3. Identify root cause

### During Implementation
1. Write unit tests for new code
2. Test against SQLite for correctness
3. Verify no regressions

### After Each Fix
1. Run affected test suite
2. Run full test suite for regressions
3. Update documentation
4. Commit with descriptive message

### Commit Format

```bash
# Bug fixes
git commit -m "#bugfix: Fix LEFT OUTER JOIN implementation

- Implemented LEFT JOIN logic in VM compiler
- Added NULL emission for non-matching right rows
- Fixed E091/03 test"

# Features
git commit -m "feat: Implement information_schema virtual tables

- Added information_schema.tables table
- Added information_schema.columns table
- Added information_schema.key_column_usage table
- Fixed all E031 tests"
```

## Verification

### Weekly Test Report
```bash
# Run full suite and generate report
go test ./internal/TS/SQL1999/... -v > test_results.txt
grep -E "^(PASS|FAIL)" test_results.txt | wc -l
```

### Regression Testing
```bash
# After each fix, ensure previous fixes still work
go test ./internal/TS/SQL1999/... -v
```

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking changes | High | Run full test suite after each change |
| Performance regression | Medium | Benchmark before/after |
| Incomplete implementation | Medium | Follow AGENTS.md commit policy |
| Unexpected bugs | Low | Incremental testing |

## Success Criteria

- [x] P0-1: E101 syntax error fixed
- [ ] P0-2: OUTER JOIN implemented
- [ ] P0-3: JOIN column reading fixed
- [ ] P0-4: information_schema implemented
- [ ] P1-1: Subqueries working
- [ ] P1-2: NULL handling improved
- [ ] P1-3: E051 issues fixed
- [ ] Overall pass rate >= 75%

## Next Actions

1. **Start with P0-1**: Fix E101 syntax error (5 minutes)
2. **Proceed to P0-3**: Fix JOIN column reading (highest ROI)
3. **Implement P0-2**: OUTER JOIN (enables many tests)
4. **Implement P0-4**: information_schema (quick wins)
5. **Continue with P1 items**: Subqueries, NULL, E051
6. **Track progress**: Update this document after each fix
