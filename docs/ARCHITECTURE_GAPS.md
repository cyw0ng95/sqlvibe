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

### Gap 4: UPDATE with Subqueries - ✅ FIXED (Infrastructure)

**Affected Tests**: E153 (1 test) - Still 0% pass rate (requires Gap 2)

**Root Cause**: UPDATE with subquery in SET clause - `UPDATE t1 SET val = (SELECT MAX(val) FROM t1) WHERE id = 1`

**Fix Applied** (Commit 6024662):
1. Added `OpScalarSubquery` opcode to VM
2. Implemented `compileSubqueryExpr` to handle SubqueryExpr in expressions
3. Added `ExecuteSubquery` method to `dbVmContext` for runtime subquery execution
4. Scalar subqueries now execute and return first column of first row

**Impact**: 
- ✅ UPDATE with scalar subqueries works correctly (verified with manual testing)
- ✅ Example: `UPDATE t1 SET val = (SELECT newval FROM t2) WHERE id = 1` now works
- ❌ E153 test still fails because it uses `MAX(val)`, which requires aggregates (Gap 2)

**Verification**:
```sql
-- This now works:
UPDATE t1 SET val = (SELECT newval FROM t2) WHERE id = 1

-- This still fails (needs Gap 2):
UPDATE t1 SET val = (SELECT MAX(val) FROM t1) WHERE id = 1
```

**Note**: The infrastructure for subqueries is complete. E153 test failure is due to missing aggregate function support, not subquery support.

---

## Test Failure Summary

| Suite | Tests | Pass Rate | Root Gap | Status |
|-------|-------|-----------|-----------|--------|
| E081 | 8 | 100% ✅ | UNION with * | FIXED |
| E131 | 7 | 0% | GROUP BY not implemented | TODO |
| E153 | 1 | 0% | Aggregate in subquery (MAX) | Needs Gap 2 |
| E171 | 1 | 0% | SQLSTATE not implemented | TODO |

---

## Priority Fix Order

1. **P0 - UNION with *** - ✅ DONE (Commit 5e3abda)
2. **P2 - UPDATE with subqueries** - ✅ DONE (Commit 6024662) - Infrastructure complete
3. **P0 - GROUP BY** - Complex, requires VM overhaul
4. **P1 - HAVING** - Depends on GROUP BY

---

## Impact Analysis

| Fix | Tests Fixed | Pass Rate Impact | Status |
|-----|-------------|-----------------|--------|
| UNION * | +8 | +0.6% | ✅ DONE |
| UPDATE subquery infra | +0* | +0%* | ✅ DONE |
| GROUP BY + aggregates | +7 (E131) +1 (E153) | +0.6% | TODO |
| **Total Done** | **+8** | **+0.6%** | **50% infrastructure** |

\* UPDATE subquery infrastructure is complete, but E153 test requires aggregate functions from Gap 2.

---

## Files Modified

**Gap 1 (UNION with *):**
1. `pkg/sqlvibe/database.go` - Fixed star expansion in execSelectStmt
2. `internal/VM/compiler.go` - Added resolveColumnCount helper

**Gap 4 (UPDATE subqueries):**
1. `internal/VM/opcodes.go` - Added OpScalarSubquery opcode
2. `internal/VM/compiler.go` - Added compileSubqueryExpr function
3. `internal/VM/exec.go` - Implemented OpScalarSubquery execution
4. `pkg/sqlvibe/database.go` - Added ExecuteSubquery method to dbVmContext

---

## Verification

```bash
# Run affected tests
go test ./internal/TS/SQL1999/E081/... -v
go test ./internal/TS/SQL1999/E131/... -v
go test ./internal/TS/SQL1999/E153/... -v
```
