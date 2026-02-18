# Plan v0.5.2 - Complete Bug Fixes Release

## Summary
Fix all remaining test failures to achieve near 100% compatibility with SQLite.

## Context
- **Previous**: v0.5.1 released with major bug fixes
- **Remaining**: ~10 test failures across LIKE, GLOB, DECIMAL, SUBSTR edge cases
- **Goal**: Fix all remaining issues for near-complete SQLite compatibility

## Priority: HIGH
All items should be completed before any new features.

---

## Remaining Test Failures

### 1. LIKE/GLOB Pattern Matching (E02112)
- **LikePattern**: Pattern matching returns wrong result
- **NotLike**: NULL handling issue
- **LikeEscape**: Escape character handling
- **LikeNumbers**: Number pattern matching
- **LikeSpecial**: Special character patterns
- **GLOB_Pattern**: GLOB pattern matching
- **GLOB_Upper**: Case sensitivity in GLOB
- **Location**: `internal/VM/exec.go` - getLike function
- **Root cause**: Pattern matching logic incorrect

### 2. DECIMAL/NUMERIC Type Handling (E01106)
- **VerifyDecimalTable**: Row values mismatched
- **Location**: Type affinity/casting issues
- **Root cause**: DECIMAL/NUMERIC not handled correctly

### 3. SUBSTR Edge Case (E02106)
- **Substr_Zero**: SUBSTR(str, 0, n) returns full string instead of first n-1 chars
- **SQLite behavior**: SUBSTR('hello', 0, 5) = 'hell' (excludes position 0)

---

## Success Criteria

### Must Fix (Release Blockers)
- [ ] Fix LIKE pattern matching (all 5 failing cases)
- [ ] Fix GLOB pattern matching (all 2 failing cases)
- [ ] Fix DECIMAL/NUMERIC type handling
- [ ] Fix SUBSTR(str, 0, n) edge case
- [ ] All SQL1999 tests pass

---

## Tasks

### Task 1: Fix LIKE Pattern Matching
- **Files**: `internal/VM/exec.go`
- **Function**: `getLike`
- **Description**: Fix pattern matching with wildcards %, _
- **Details**:
  - Handle escape characters properly
  - Fix _ (single character) matching
  - Fix % (zero or more characters) matching
  - Fix case sensitivity

### Task 2: Fix GLOB Pattern Matching
- **Files**: `internal/VM/exec.go`
- **Function**: `getGLOB`
- **Description**: Fix GLOB (case-sensitive LIKE)
- **Details**:
  - GLOB uses [] for character classes
  - Case-sensitive matching
  - Backslash escape

### Task 3: Fix DECIMAL/NUMERIC Types
- **Files**: `internal/QP/parser.go`, `internal/VM/exec.go`
- **Description**: Handle DECIMAL/NUMERIC column types
- **Details**:
  - DECIMAL(10,2) should store as float64
  - Proper type affinity
  - Comparison operators for decimal

### Task 4: Fix SUBSTR Edge Case
- **Files**: `internal/VM/exec.go`
- **Function**: `stringSubstr`
- **Description**: Fix SUBSTR when start=0
- **Details**:
  - SQLite: SUBSTR(str, 0, n) excludes position 0
  - Should return first n-1 characters

### Task 5: Run Full Test Suite
- **Command**: `go test ./...`
- **Verify**: All tests pass

---

## Verification

```bash
# Run all tests
go test ./...

# Run specific failing tests
go test ./internal/TS/SQL1999/... -run "TestSQL1999_F301_E02112"
go test ./internal/TS/SQL1999/... -run "TestSQL1999_F301_E01106"
go test ./internal/TS/SQL1999/... -run "Substr_Zero"
```

---

## Timeline Estimate

- Task 1: Fix LIKE - 2 hours
- Task 2: Fix GLOB - 1 hour
- Task 3: Fix DECIMAL - 2 hours
- Task 4: Fix SUBSTR - 30 min
- Task 5: Verify - 30 min

**Total**: ~6 hours

---

## Notes

- This is final bug fix release before new features
- Target: >99% test pass rate
- Document any intentional deviations from SQLite
