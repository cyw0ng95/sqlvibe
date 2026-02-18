# SQL1999 Testsuite Failure Analysis

**Date**: 2026-02-18  
**Total Tests**: 1034  
**Passed**: 829 (80.2%)  
**Failed**: 203 (19.6%)  
**Hung**: 1 test (E091/03)

## Summary by Test Suite

| Suite | Feature | Passed | Failed | Total | Pass Rate |
|-------|---------|--------|--------|-------|-----------|
| E011  | Numeric Data Types | 261 | 1 | 264 | 98.9% |
| E021  | Character String Types | 181 | 0 | 187 | 96.8% |
| E031  | Identifiers | 25 | 38 | 63 | 39.7% |
| E041  | Basic Query Spec | 155 | 65 | 220 | 70.5% |
| E051  | Basic Query Spec | 105 | 14 | 119 | 88.2% |
| E061  | Basic Predicates | 56 | 26 | 82 | 68.3% |
| E071  | Basic Query Expressions | 17 | 36 | 53 | 32.1% |
| E081  | Privileges | 0 | 8 | 8 | 0% |
| E091  | Set Functions | 1 | 1 | 3 | 33.3% |
| E101  | Basic Data Manipulation | - | - | - | Setup Failed |
| E111  | Single Row SELECT | 6 | 0 | 6 | 100% |
| E121  | Basic Cursor Support | 6 | 0 | 6 | 100% |
| E131  | Null Value Support | 0 | 7 | 7 | 0% |
| E141  | Basic Integrity Constraints | 8 | 0 | 8 | 100% |
| E151  | Transaction Support | 8 | 0 | 8 | 100% |

## Critical Issues

### 1. **HUNG TEST: E091/03 - LEFT OUTER JOIN** (CRITICAL)
- **Status**: HANGS (timeout after 57.4s)
- **Test**: `TestSQL1999_F301_E09103_L1`
- **Cause**: LEFT OUTER JOIN not implemented in VM compiler
- **SQL**: `SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.a = t2.x`
- **Impact**: Blocks E091 suite execution
- **Fix Required**: Implement LEFT JOIN logic (emit rows with NULLs for non-matching right table)

### 2. **JOIN Column Reading Issues** (CRITICAL)
- **Status**: FAILED
- **Affected**: E041/04 and other JOIN tests
- **Symptoms**: 
  - INNER JOIN returns 0 rows
  - CROSS JOIN returns rows but with NULL values for right table columns
- **Root Cause**: Compiler doesn't have per-table schema information for JOINs
- **Details**:
  - `TableColIndices` only contains schema for first table in FROM clause
  - When compiling `b.y`, uses wrong column index for cursor 1
  - `OpColumn` reads from `cursor.Columns[colIdx]` but colIdx is from wrong table
- **Fix Required**: Pass multi-table schema to compiler or implement dynamic column lookup

### 3. **Rows.Next() Iterator Bug** (FIXED)
- **Status**: FIXED in commit a68a29c
- **Issue**: First row was skipped when using iterator API
- **Cause**: `pos` started at 0, first `Next()` incremented to 1
- **Solution**: Added `started` flag to track first call

## Detailed Failure Analysis

### E031: Identifiers (38 failures, 39.7% pass rate)
- **Primary Issue**: information_schema table queries
- Most failures are queries against `information_schema.key_column_usage` and `information_schema.referential_constraints`
- These are metadata tables not yet implemented

### E041: Basic Query Spec (65 failures, 70.5% pass rate)
- **JOIN Issues**: All JOIN-related tests fail due to column reading bug
  - E04104: Foreign Key + JOIN tests
  - Symptoms: INNER JOIN returns 0 rows, column count mismatches
- **BLOB Handling**: Some BLOB column tests fail
- **Column Aliasing**: Issues with qualified column names (`table.column`)

### E051: Basic Query Spec (14 failures, 88.2% pass rate)
- Generally good pass rate
- Some edge cases with type coercion
- Minor SELECT clause issues

### E061: Basic Predicates (26 failures, 68.3% pass rate)
- MATCH operator not fully working
- Some complex predicate combinations fail
- WHERE clause edge cases

### E071: Basic Query Expressions (36 failures, 32.1% pass rate)
- **Subquery Issues**: Nested subqueries failing
- **Set Operations**: Some UNION/INTERSECT/EXCEPT cases fail
- Complex query expressions

### E081: Privileges (0% pass rate)
- **Not Implemented**: Privilege system not implemented
- All 8 tests fail
- GRANT/REVOKE statements not supported

### E091: Set Functions (1 failure + 1 hung)
- E09101: PASS - Basic set functions work
- E09102: FAIL - Advanced set function cases
- E09103: HUNG - LEFT OUTER JOIN test

### E131: Null Value Support (0% pass rate)
- All 7 tests fail
- NULL handling in various contexts needs improvement
- NULL in expressions, comparisons, aggregates

## Bugs Fixed

### 1. JOIN Infinite Loop (commit 96affab)
- **Issue**: JOIN queries hung indefinitely
- **Cause**: Incorrect nested loop control flow in `compileJoin`
- **Fix**: 
  - Properly set up jump targets for OpRewind, OpNext
  - Fixed rightDonePos calculation
  - Added proper leftDone handling
- **Result**: E041/04 test no longer times out (0.007s vs timeout)

### 2. Rows.Next() Skipping First Row (commit a68a29c)
- **Issue**: Iterator API only returned last row
- **Cause**: `pos` incremented before check on first call
- **Fix**: Added `started` flag to handle first call specially
- **Result**: All rows now correctly returned via iterator

## Recommendations

### High Priority (P0)
1. **Fix JOIN column reading** - Implement multi-table schema handling in compiler
2. **Implement LEFT/RIGHT OUTER JOIN** - Required for E091/03 and other tests
3. **Fix E131 NULL handling** - Critical for SQL compliance

### Medium Priority (P1)
4. **Implement information_schema tables** - Required for E031 metadata queries
5. **Fix subquery compilation** - Needed for E071 complex expressions
6. **Improve BLOB handling** - Some E041 tests failing

### Low Priority (P2)
7. **Implement privilege system (E081)** - GRANT/REVOKE support
8. **Edge case fixes** - Various small issues across suites

## Test Execution Notes

- Total test suites: 15
- Suites with 100% pass: 4 (E111, E121, E141, E151)
- Suites with >90% pass: 2 (E011, E051)
- Suites with <50% pass: 4 (E031, E071, E081, E131)
- Hung tests: 1 (E091/03)

## Files Modified

1. `internal/VM/compiler.go`:
   - Fixed JOIN loop control flow
   - Added table-to-cursor mapping
   - Updated compileColumnRef for multi-cursor support

2. `pkg/sqlvibe/database.go`:
   - Fixed Rows.Next() iterator bug

## Next Steps

1. Implement multi-table schema support in compiler for JOIN fixes
2. Implement LEFT/RIGHT OUTER JOIN logic
3. Add information_schema tables
4. Fix NULL handling issues in E131
5. Re-run full testsuite to verify fixes
