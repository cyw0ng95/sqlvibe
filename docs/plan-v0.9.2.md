# Plan v0.9.2 - Stabilization & Bug Fixes

## Summary

v0.9.2 focuses on correctness improvements: proper error reporting for
undefined functions, JULIANDAY/ROUND bug fixes, and performance refinements.

## Completed Tasks

### Phase 1: Unknown Function Error Reporting ✅

- [x] `evaluateFuncCallOnRow` (VM/exec.go): set `vm.err` when function not found
- [x] `OpCallScalar` handler: return `vm.err` to propagate the error
- [x] `evalFuncCall` (VM/query_engine.go): add `evalErr` field to QueryEngine,
      set it when extension dispatch fails
- [x] `evalConstantExpression` (pkg/sqlvibe/database.go): check `LastError()`
      after `EvalExpr` and propagate

### Phase 2: JULIANDAY NULL Bug ✅

- [x] Fix JULIANDAY(NULL) to return NULL (not current Julian day)
- [x] Fix in both exec.go (VM path) and query_engine.go (QE path)
- [x] `parseDateTimeValue` now handles float64 (Julian day number) input
- [x] `parseQEDateTime` now handles float64 input consistently

### Phase 3: ROUND Float64 Bug ✅

- [x] `getRound` returns `float64` for 0-decimal case (matches SQLite)
- [x] Previously returned `int64`, causing type mismatch

### Phase 4: Math Functions in QE path ✅

- [x] Add ROUND, ABS, CEIL, CEILING, FLOOR, SQRT, POWER, POW, EXP,
      LOG, LN, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2 to `evalFuncCall`
- [x] Add `toFloat64QE` helper
- [x] Add ROW() row constructor support in QE path
- [x] Math functions now work in constant SELECT (no FROM) context

### Phase 5: Performance - Dispatch Table Expansion ✅

- [x] Add OpUpper, OpLower, OpLength, OpConcat to dispatch table
- [x] Optimize `compareVals` to use `bytes.Compare` for `[]byte`

### Phase 6: Testing ✅

- [x] Regression test suite (`internal/TS/Regression/regression_test.go`)
  - TestRegression_UnknownFunction_L1
  - TestRegression_UnknownFunction_Constant_L1
  - TestRegression_JulianDayNULL_L1
  - TestRegression_RoundFloat_L1
  - TestRegression_RoundJulianDay_L1
- [x] F874 test suite for v0.9.2 features
  - TestSQL1999_F874_DateTimeFunctions_L1 (9 cases)
  - TestSQL1999_F874_UnknownFunctionError_L1
  - TestSQL1999_F874_MathFunctions_L1 (5 cases)

## Deferred Tasks (from original incomplete v0.9.1 list)

The following items were listed as "incomplete" in the original plan but are
already implemented in v0.9.1. No further work is required.

- Covering Index: IndexMeta/CoversColumns, FindCoveringIndex/SelectBestIndex/CanSkipScan
- Column Projection: RequiredColumns in QP/analyzer.go
- Slab Allocator: DS/slab.go
- Statement Pool: pkg/sqlvibe/statement_pool.go
- Direct Threaded VM: VM/dispatch.go (foundation)
- DirectCompiler: CG/direct_compiler.go
- Expression Bytecode: VM/expr_bytecode.go, VM/expr_eval.go

## Success Criteria

- [x] All covering index tasks completed (deferred - already in v0.9.1)
- [x] JULIANDAY and ROUND bugs fixed
- [x] Unknown function error surfaced to user
- [x] All tests passing (57 test packages)
- [x] README performance section updated
