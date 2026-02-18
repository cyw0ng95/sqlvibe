# SQL1999 Fundamental Architecture Gaps

## Executive Summary

Analysis of the 1,331 SQL1999 test cases reveals **4 fundamental architectural gaps** in the compiler that cause 311 tests (23%) to fail with 0% pass rate in their respective suites.

## Critical Gaps

### Gap 1: UNION with SELECT Star (`SELECT *`)

**Affected Tests**: E081 (8 tests) - 0% pass rate

**Root Cause**: `internal/VM/compiler.go:1163`
```go
numCols := len(stmt.Columns)  // Returns 1 for "*", not actual column count
```

**Impact**: When executing `SELECT * FROM t1 UNION SELECT * FROM t2`:
- `stmt.Columns` contains `[*QP.ColumnRef{Name: "*"}]`
- `len(stmt.Columns)` returns 1
- Query returns wrong number of columns

**Fix Required**: Resolve column count from table schema when Columns contains star:
```go
numCols := len(stmt.Columns)
if numCols == 1 {
    if star, ok := stmt.Columns[0].(*QP.ColumnRef); ok && star.Name == "*" {
        // Get actual column count from table schema
        numCols = c.getColumnCountFromTable(stmt.From)
    }
}
```

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

### Gap 4: UPDATE Execution Not Working

**Affected Tests**: E153 (1 test) - 0% pass rate

**Root Cause**: `internal/VM/compiler.go` - UPDATE compiles but doesn't execute

**Impact**: UPDATE returns nil instead of updated values

**Fix Required**: Debug UPDATE bytecode execution

---

## Test Failure Summary

| Suite | Tests | Pass Rate | Root Gap |
|-------|-------|-----------|-----------|
| E081 | 8 | 0% | UNION with * |
| E131 | 7 | 0% | GROUP BY not implemented |
| E153 | 1 | 0% | UPDATE not working |
| E171 | 1 | 0% | SQLSTATE not implemented |

---

## Priority Fix Order

1. **P0 - UNION with *** - Simple fix, unblocks E081
2. **P0 - GROUP BY** - Medium complexity, unblocks E131
3. **P1 - HAVING** - Depends on GROUP BY
4. **P2 - UPDATE** - Debug execution

---

## Impact Analysis

| Fix | Tests Fixed | Pass Rate Impact |
|-----|-------------|-----------------|
| UNION * | +8 | +0.6% |
| GROUP BY | +7 | +0.5% |
| UPDATE | +1 | +0.1% |
| **Total** | **+16** | **+1.2%** |

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
