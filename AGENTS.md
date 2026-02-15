# OpenCode Agents Guidance

## Project: sqlvibe - SQLite-Compatible Database Engine in Go

This document provides guidance for OpenCode agents working on the sqlvibe project.

---

## 1. Project Overview

**Goal**: Implement a SQLite-compatible database engine in Go that achieves blackbox-level correctness compared to real SQLite.

**Language**: Go (Golang)  
**Architecture**: Modular subsystem design (PB, DS, QP, QE, TM)

---

## 2. Important Restrictions

### 2.1 Git Operations

- **NEVER use force push** (`git push --force`, `git push -f`)
- Always create meaningful commit messages
- Commit frequently to maintain clear history

### 2.2 Bugfix Commits

When fixing bugs, the commit message MUST include:
- `#bugfix` tag at the beginning
- Clear description of what was fixed

**Format**:
```
#bugfix: Description of the fix

- Root cause explanation
- What was changed
- How this fixes the issue
```

**Example**:
```
#bugfix: Fix page boundary check in B-Tree split

- The split function was not correctly handling the case where
  the left page would be exactly at capacity
- Changed split logic to ensure both pages have minimum fill
- Added test case for boundary condition
```

### 2.3 Code Quality

- **NEVER** use type suppression (`as any`, `@ts-ignore`, `@ts-expect-error`)
- **NEVER** leave empty catch blocks
- **NEVER** delete failing tests to "pass"
- Always fix root causes, not symptoms
- **NEVER** add external packages to the codebase. Only use Go standard library and the existing codebase.

---

## 3. Development Workflow

### 3.1 Before Starting Work

1. Read the relevant documentation in `docs/`
2. Understand the current phase and task priorities
3. Check existing tests to understand expected behavior

### 3.2 Implementation Rules

1. **Each Iteration Must Deliver Valid Results**
   - Never implement partial features that cannot be tested
   - Every change should result in a working system
   - If blocked, ask for clarification

2. **Test-Driven Development**
   - Write tests before or alongside implementation
   - Run SQLite comparison tests frequently
   - Verify against real SQLite outputs

3. **SQLite Compatibility**
   - Run blackbox tests against real SQLite
   - Document any intentional deviations
   - Aim for 100% compatibility on covered features

### 3.3 Verification Requirements

Before marking a task complete, verify:

- [ ] Unit tests pass
- [ ] Integration tests pass  
- [ ] SQLite comparison tests pass
- [ ] No regressions in previous features
- [ ] Code follows project conventions

### 3.4 Commit When Task is Complete

**When a task is complete, make and push commits in time:**

1. After completing any task or subtask, commit immediately
2. Run tests first to ensure everything works
3. Create a meaningful commit message describing what was done
4. Push to remote so progress is tracked

```
# Example workflow after completing a task
go test ./...
git add -A
git commit -m "feat: Implement B-Tree search operation

- Added search method to B-Tree structure
- Handles both leaf and interior nodes
- Added unit tests"
git push
```

**Commit timing rules:**
- Commit within the same session when task completes
- Never leave completed work uncommitted
- If working across multiple sessions, commit before stopping

### 3.5 Track Progress Against Plan

During development, always reference the current plan:

- When working on any task, always update the plan in docs/ accordingly (mark progress, add/revise task, document completion, etc.) before committing and pushing.

1. **Read the plan** - Start each session by reading `docs/plan-VERSION.md`
2. **Find next task** - Look for incomplete items in Success Criteria
3. **Update status** - Mark tasks as completed in the plan after finishing:
   ```markdown
   ## Success Criteria
   - [x] Feature A implemented and tested  ← completed
   - [ ] Feature B implemented and tested  ← pending
   ```
4. **Commit and push EVERY time** - After marking tasks complete OR any plan change:
   ```
   git add -A
   git commit -m "docs: Update plan-v0.3.x progress"
   git push
   ```
5. **When all tasks complete** - It's time to release:
   - Run full test suite
   - Create tag and push
   - Update HISTORY.md
   - Merge to main

### 3.6 Handling New Tasks

When user adds new tasks during development:

1. **Refine the DAG first** - Update the Implementation DAG to show dependencies
2. **Analyze dependencies** - Determine what can run in parallel, what depends on what
3. **Update Success Criteria** - Add new tasks with `[ ]` status
4. **Commit and push immediately** - Never proceed without committing plan changes:
   ```
   git add -A
   git commit -m "docs: Add new task to plan-v0.3.x"
   git push
   ```
5. **Implement according to DAG** - Never start implementation without following the DAG order

```
Example: User adds "Feature X" mid-iteration

1. Update DAG (mermaid format):
   ```mermaid
   graph LR
       A[Feature A] --> B[Feature B]
       B --> X[Feature X]
       B --> C[Feature C]
   ```

2. Add to Success Criteria:
   - [x] Feature A implemented
   - [ ] Feature B implemented
   - [ ] Feature X implemented  ← new
   - [ ] Feature C implemented

3. Commit and push plan changes immediately

4. Implement following DAG order
```

#### 8.4.7 Regression Testsuite

The **Regression** testsuite is for capturing specific SQL patterns that have been identified as error-prone during development. When a bug is discovered or a specific SQL case causes issues, it should be added here to prevent future regressions.

**Naming Format**:
```
TestRegression_<Description>_<Level>
```

**Example**:
```go
// TestRegression_CoalesceNULL_L1 tests COALESCE with NULL values
// Regression case for COALESCE function handling
// Level: 1 (Fundamental - uses :memory: backend)
func TestRegression_CoalesceNULL_L1(t *testing.T) {
    sqlvibePath := ":memory:"
    sqlitePath := ":memory:"
    // ... test code
}

// TestRegression_InsertMultiRow_L1 tests multi-row INSERT
// Regression case for batch insert edge cases
func TestRegression_InsertMultiRow_L1(t *testing.T) {
    // ... test code
}
```

**When to Add Regression Tests**:
- A specific SQL query is found to produce different results than SQLite
- A particular edge case causes crashes or panics
- A SQL pattern was previously buggy and has been fixed
- Any specific scenario that needs to be guarded against future regressions

**Regression Test Guidelines**:
1. **Descriptive Name**: Use clear description of the specific case being tested
2. **Minimal Reproduction**: Create the smallest possible test case that reproduces the issue
3. **Document the Bug**: Add a comment explaining what bug this test prevents
4. **Use L1 when possible**: Most regression tests can use `:memory:` backend unless they specifically test file/persistence behavior
5. **Group by Feature**: If multiple regression tests exist for the same feature, group them together

**Example - Adding a New Regression Test**:

When you discover a bug like "COALESCE with NULL returns wrong result":

```go
// TestRegression_CoalesceNULL_L1 regression test for COALESCE function
// Bug: COALESCE(NULL, 'default') was returning NULL instead of 'default'
// Fixed in commit X - this test prevents future regression
func TestRegression_CoalesceNULL_L1(t *testing.T) {
    sqlvibePath := ":memory:"
    sqlitePath := ":memory:"
    // ... test code
}
```

---

## 9. Documentation

### 9.1 Code Documentation

- Document public interfaces
- Explain complex algorithms in comments
- Use meaningful variable/function names

### 9.2 Project Documentation

- Update `docs/` when making architectural changes
- Keep PHASES.md current with progress
- Document any design decisions

---

## 10. File Structure

```
sqlvibe/
├── cmd/              # CLI application
├── pkg/              # Public API
├── internal/         # Internal packages
│   ├── pb/          # Platform Bridges
│   ├── ds/          # Data Storage
│   ├── qp/          # Query Processing
│   ├── qe/          # Query Execution
│   └── tm/          # Transaction Monitor
├── test/            # Test files
│   └── sqllogictest/
├── docs/            # Documentation
├── go.mod
├── go.sum
└── Makefile
```

---

## 11. Build and Test Commands

```bash
# Build the project
go build ./...

# Run all tests
go test ./...

# Run SQLite comparison tests
go test ./test/sqllogictest/...

# Run specific test
go test -run TestName ./...

# Generate parser
make parser

# Format code
go fmt ./...

# Run linter
go vet ./...
```

---

## 12. Getting Help

1. Read `docs/ARCHITECTURE.md` for system design
2. Read `docs/PHASES.md` for implementation plan
3. Check existing tests for expected behavior
4. If stuck, consult with Oracle agent

---

## Summary

- **No force push ever**
- Use `#bugfix` prefix for bug fixes
- Each iteration must deliver valid results
- Test against SQLite frequently
- Commit frequently with meaningful messages
- Follow code quality standards
