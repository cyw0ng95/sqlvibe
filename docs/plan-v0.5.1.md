# Plan v0.5.1 - Bug Fixes Release

## Summary
Focus on fixing critical test failures identified in v0.5.0 before proceeding to new features.

## Priority: HIGH
All items must be completed before any new features.

---

## Known Issues from v0.5.0

### Critical: DS Encoding Tests
- **TestSerialTypes/int32**: GetSerialType() returns 3, wants 4; SerialTypeLen() returns 3, wants 4
- **TestSerialTypes/int64**: GetSerialType() returns 5, wants 6; SerialTypeLen() returns 6, wants 8
- **Location**: `internal/DS/encoding.go`, `encoding_test.go:80,85`
- **Root cause**: Serial type code mapping for int32/int64 is incorrect

### Critical: SQL1999 Integration Tests
- **Where_NOT_AND**: row count mismatch (sqlvibe=6, sqlite=5)
- **OrderBy_Expression**: Multiple row value mismatches
- **OrderBy_ABS**: Multiple row value mismatches
- **IN_List / NOT_IN_List**: Row value mismatches
- **BETWEEN_True / NOT_BETWEEN_True**: Row value mismatches
- **VarcharTrim**: Returns empty string instead of "1234567890"
- **Substr_From3_Len2**: Substring operation failures

### Medium: WHERE Operators
- **LIKE operator**: 1 edge case failing (case sensitivity nuance)

---

## Success Criteria

### Must Fix (Release Blockers)
- [ ] Fix DS encoding serial type mapping (int32 → 4, int64 → 6)
- [ ] Fix ORDER BY expression handling
- [ ] Fix ORDER BY ABS function
- [ ] Fix IN/NOT IN operator bugs
- [ ] Fix BETWEEN/NOT BETWEEN operator bugs
- [ ] Fix TRIM string function (returns empty)
- [ ] Fix SUBSTR string function
- [ ] All DS encoding tests pass
- [ ] All SQL1999 tests pass (or documented deviations)

### Nice to Have
- [ ] Fix LIKE operator edge case

---

## Tasks

### Task 1: Fix DS Encoding Serial Type Mapping
- **Files**: `internal/DS/encoding.go`
- **Description**: Fix incorrect serial type codes
- **Details**:
  - int32 serial type should be 4 (currently returns 3)
  - int64 serial type should be 6 (currently returns 5)
  - Verify against SQLite specification: https://www.sqlite.org/fileformat2.html#record_format

### Task 2: Fix ORDER BY Expression Handling
- **Files**: `internal/VM/exec.go`, `internal/CG/compiler.go`
- **Description**: Fix ORDER BY with expressions
- **Details**:
  - ORDER BY with expressions returning wrong results
  - NULL handling in ORDER BY incorrect
  - Expression evaluation in ORDER BY context broken

### Task 3: Fix ORDER BY ABS Function
- **Files**: `internal/VM/exec.go`, `internal/CG/compiler.go`
- **Description**: Fix ABS() in ORDER BY clause
- **Details**:
  - ABS() function not working in ORDER BY context
  - Multiple column ABS evaluation issues

### Task 4: Fix IN/NOT IN Operators
- **Files**: `internal/VM/exec.go`
- **Description**: Fix IN and NOT IN operator behavior
- **Details**:
  - IN operator returns wrong values
  - NOT IN operator returns wrong values
  - NULL handling in IN/NOT IN incorrect

### Task 5: Fix BETWEEN Operators
- **Files**: `internal/VM/exec.go`
- **Description**: Fix BETWEEN and NOT BETWEEN operators
- **Details**:
  - BETWEEN returns wrong values
  - NOT BETWEEN returns wrong values
  - NULL handling incorrect

### Task 6: Fix String Functions (TRIM, SUBSTR)
- **Files**: `internal/VM/exec.go`
- **Description**: Fix TRIM and SUBSTR string operations
- **Details**:
  - TRIM returns empty string instead of trimmed result
  - SUBSTR not handling positions correctly

### Task 7: Run Full Test Suite
- **Command**: `go test ./...`
- **Verify**: All tests pass

---

## Verification

```bash
# Run all tests
go test ./...

# Run DS encoding tests specifically
go test ./internal/DS/... -run TestSerialTypes

# Run SQL1999 tests
go test ./internal/TS/SQL1999/...

# Run WHERE operator tests
go test ./... -run "TestWhere"
```

---

## Timeline Estimate

- Task 1: Fix DS encoding - 1 hour
- Task 2: Fix ORDER BY expression - 2 hours
- Task 3: Fix ORDER BY ABS - 1 hour
- Task 4: Fix IN/NOT IN - 2 hours
- Task 5: Fix BETWEEN - 1 hour
- Task 6: Fix String functions - 2 hours
- Task 7: Verify all tests - 1 hour

**Total**: ~10 hours

---

## Dependencies

All tasks are independent and can be done in parallel once the issues are identified.

---

## Notes

- This is a bug fix release - no new features
- Must achieve >95% test pass rate before v0.5.1 release
- Document any intentional deviations from SQLite behavior
