# SQL1999 Fundamental Architecture Gaps

## Executive Summary

Analysis of the 1,331 SQL1999 test cases reveals **4 fundamental architectural gaps** in the compiler that cause 311 tests (23%) to fail with 0% pass rate in their respective suites.

**Status**: Gap 1 (UNION with *) has been fixed. 8 tests now passing.

## Critical Gaps

### Gap 1: UNION with SELECT Star (`SELECT *`) - ✅ FIXED

**Affected Tests**: E081 (8 tests) - **100% pass rate** (was 0%)

**Root Cause**: `pkg/sqlvibe/database.go:1879-1886` - The `execSelectStmt` function extracted column names from `stmt.Columns` without expanding `*` to actual column names.

**Impact**: When executing `SELECT * FROM t1 UNION SELECT * FROM t1`:
- `stmt.Columns` contains `[*QP.ColumnRef{Name: "*"}]`
- Column names were set to `["*"]` instead of `["a", "b"]`
- Results had correct data but wrong column names

**Fix Applied** (Commit 5e3abda):
Modified `pkg/sqlvibe/database.go` in `execSelectStmt` function to expand star columns:
```go
if colRef, ok := col.(*QP.ColumnRef); ok {
    // Handle SELECT * - expand to table columns
    if colRef.Name == "*" {
        cols = append(cols, tableCols...)
    } else {
        cols = append(cols, colRef.Name)
    }
}
```

**Verification**: All 8 E081 tests now passing ✅

---

### Gap 2: GROUP BY Not Implemented

**Affected Tests**: E131 (7 tests) - 0% pass rate

**Root Cause**: `internal/VM/compiler.go:1073-1080`
```go
if stmt.GroupBy != nil {
    groupRegs := make([]int, 0)
    for _, gb := range stmt.GroupBy {
        reg := c.compileExpr(gb)
        groupRegs = append(groupRegs, reg)
    }
    _ = groupRegs  // COMPILED BUT NEVER USED!
}
```

**Impact**: 
- GROUP BY expressions compile to registers but no grouping occurs
- Aggregates like `COUNT(*)` return NULL instead of grouped counts
- Results show `sqlvibe=<nil>, sqlite=1`

**Fix Required**: Implement GROUP BY operator in VM:
1. Add GROUP BY opcode to VM
2. Emit grouping logic after scanning rows
3. Accumulate aggregate values per group

---

### Gap 3: HAVING Clause Not Compiled

**Affected Tests**: E131 (7 tests) - 0% pass rate

**Root Cause**: `internal/VM/compiler.go` - No HAVING compilation path exists

**Impact**: HAVING clause is ignored entirely

**Fix Required**: Add HAVING compilation after GROUP BY

---

### Gap 4: UPDATE with Subqueries Not Working

**Affected Tests**: E153 (1 test) - 0% pass rate

**Root Cause**: UPDATE with subquery in SET clause - `UPDATE t1 SET val = (SELECT MAX(val) FROM t1) WHERE id = 1`

**Impact**: 
- Simple UPDATE works correctly (verified with manual test)
- UPDATE with subquery in SET clause returns nil instead of subquery result
- Subquery evaluation in UPDATE context not implemented

**Fix Required**: 
1. Detect subquery expressions in UPDATE SET clause
2. Compile and execute subquery to get value
3. Use subquery result in UPDATE operation

**Note**: This is actually about subquery evaluation in UPDATE context, not basic UPDATE execution.

---

## Test Failure Summary

| Suite | Tests | Pass Rate | Root Gap | Status |
|-------|-------|-----------|-----------|--------|
| E081 | 8 | 100% ✅ | UNION with * | FIXED |
| E131 | 7 | 0% | GROUP BY not implemented | TODO |
| E153 | 1 | 0% | UPDATE with subqueries | TODO |
| E171 | 1 | 0% | SQLSTATE not implemented | TODO |

---

## Priority Fix Order

1. **P0 - UNION with *** - ✅ DONE (Commit 5e3abda)
2. **P0 - GROUP BY** - Complex, requires VM overhaul
3. **P1 - HAVING** - Depends on GROUP BY
4. **P2 - UPDATE with subqueries** - Requires subquery evaluation support

---

## Impact Analysis

| Fix | Tests Fixed | Pass Rate Impact | Status |
|-----|-------------|-----------------|--------|
| UNION * | +8 | +0.6% | ✅ DONE |
| GROUP BY | +7 | +0.5% | TODO |
| UPDATE subquery | +1 | +0.1% | TODO |
| **Total Possible** | **+16** | **+1.2%** | **50% done** |

---

## Files to Modify

1. `internal/VM/compiler.go` - Main fixes
2. `internal/VM/opcodes.go` - Add GROUP BY opcode
3. `internal/VM/exec.go` - Implement GROUP BY execution

---

## Verification

```bash
# Run affected tests
go test ./internal/TS/SQL1999/E081/... -v
go test ./internal/TS/SQL1999/E131/... -v
go test ./internal/TS/SQL1999/E153/... -v
```
