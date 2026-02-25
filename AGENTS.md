# OpenCode Agents Guidance

## Project: sqlvibe - SQLite-Compatible Database Engine in Go

This document provides guidance for OpenCode agents working on the sqlvibe project.

---

## 1. Project Overview

**Goal**: Implement a high-performance in-memory database engine in Go with SQL compatibility.

**Language**: Go (Golang)  
**Architecture**: Hybrid row/columnar storage with adaptive execution

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

### 2.4 Defensive Programming with Assertions

The codebase uses aggressive defensive programming through assertion statements to catch bugs early. All assertions are located in `internal/SF/util/assert.go`.

**Available Assertion Functions**:
- `util.Assert(condition, format, args...)` - Panics if condition is false
- `util.AssertNotNil(value, name)` - Panics if value is nil (including typed nils)
- `util.AssertTrue(condition, message)` - Panics if condition is false
- `util.AssertFalse(condition, message)` - Panics if condition is true

**When to Add Assertions**:
1. **Preconditions**: Validate function inputs at entry points
   - Non-nil pointers/interfaces
   - Valid ranges (indices, sizes, bounds)
   - Non-empty strings/arrays where required
   
2. **Invariants**: Validate internal state assumptions
   - Array/slice bounds before indexing
   - Valid enum/constant values
   - Cursor/register ID bounds
   - Page types and sizes
   
3. **Data Structure Integrity**: Validate structural constraints
   - B-Tree page types (0x0d, 0x02, 0x05, 0x0a)
   - Page size bounds [512, 65536] and power-of-2
   - Cell offsets within page bounds
   - Varint buffer sizes

**Examples**:
```go
// Good: Validate inputs
func (bt *BTree) Search(key []byte) ([]byte, error) {
    util.Assert(len(key) > 0, "search key cannot be empty")
    util.AssertNotNil(page, "page")
    util.Assert(len(page.Data) >= 12, "page data too small: %d bytes", len(page.Data))
    // ... implementation
}

// Good: Validate bounds
func (vm *VM) OpOpenRead(cursorID int) {
    util.Assert(cursorID >= 0 && cursorID < MaxCursors, 
        "cursor ID %d out of bounds [0, %d)", cursorID, MaxCursors)
    // ... implementation
}

// Bad: Don't duplicate error handling
func FreePage(pageNum uint32) error {
    // Don't do this - assertion duplicates the error check
    util.Assert(pageNum > 0, "cannot free page 0")
    if pageNum == 0 {
        return fmt.Errorf("cannot free page 0")
    }
}
```

**Assertion vs Error Handling**:
- **Use assertions** for programming errors (bugs, violated invariants)
- **Use error returns** for runtime errors (file I/O, user input, resource exhaustion)
- Assertions should never fire in correct code with valid inputs

**Key Constants**:
- `MaxCursors = 256` (VM subsystem)
- `MinPageSize = 512`, `MaxPageSize = 65536` (DS subsystem)
- Page types: 0x0d (table leaf), 0x02 (index leaf), 0x05 (table interior), 0x0a (index interior)

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
   git commit -m "docs: Update plan-v0.8.0 progress"
   git push
   ```
5. **When all tasks complete** - It's time to release:
   - Run full test suite
   - Create tag and push
   - Update HISTORY.md
   - Merge to main

### 3.6 Version Release Process

When wrapping up a version/iteration (e.g., completing v0.4.1), follow these steps:

#### Step 1: Verify All Tests Pass
```bash
go test ./...
```

#### Step 2: Update HISTORY.md
Add a new section at the top of `docs/HISTORY.md`:
```markdown
## **v0.X.Y** (YYYY-MM-DD)

### Bug Fixes
- List all bug fixes

### Features
- List all new features (if any)

### Breaking Changes
- List any breaking changes (if any)
```

#### Step 3: Create and Push Tag
```bash
git tag -a v0.X.Y -m "Release v0.X.Y: Description"
git push origin v0.X.Y
```

#### Step 4: Push the HISTORY Commit
```bash
git add -A
git commit -m "docs: Add v0.X.Y release notes to HISTORY.md"
git push
```

**Important:**
- Always update HISTORY.md BEFORE creating the tag (or in the same commit)
- The tag marks the exact release point
- Push the tag separately: `git push origin v0.X.Y`

### 3.7 Handling New Tasks

When user adds new tasks during development:

1. **Refine the DAG first** - Update the Implementation DAG to show dependencies
2. **Analyze dependencies** - Determine what can run in parallel, what depends on what
3. **Update Success Criteria** - Add new tasks with `[ ]` status
4. **Commit and push immediately** - Never proceed without committing plan changes:
   ```
   git add -A
   git commit -m "docs: Add new task to plan-v0.8.0"
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

#### 8.4.8 Fuzzer Bug Tracking (PlainFuzzer)

When PlainFuzzer confirms a bug, record it in `internal/TS/PlainFuzzer/HUNTINGS.md` instead of HISTORY.md to avoid redundancy.

**HUNTINGS.md Format**:
```markdown
### Bug Title

| Attribute | Value |
|-----------|-------|
| **Severity** | High/Medium/Low |
| **Type** | Bug category |
| **File** | affected file path |
| **Function** | function name |
| **Trigger SQL** | SQL that triggers the bug |
| **Impact** | What happens (panic, hang, wrong result) |
| **Root Cause** | Explanation of the bug |
| **Fix** | How it was fixed |
| **Found By** | PlainFuzzer |
| **Date** | YYYY-MM-DD |
```

**Workflow**:
1. Add bug entry to `HUNTINGS.md` with full details
2. Add regression test in `internal/TS/Regression/`
3. Commit and push
4. Do NOT add to HISTORY.md - HUNTINGS.md is the source of truth for fuzzer bugs

---

#### 8.4.9 SQLValidator Bug Tracking

When SQLValidator finds a correctness mismatch between sqlvibe and SQLite, record it in
`internal/TS/SQLValidator/HUNTINGS.md` instead of HISTORY.md.

**HUNTINGS.md Format**:
```markdown
### Bug Title

| Attribute | Value |
|-----------|-------|
| **Severity** | High/Medium/Low |
| **Type** | ResultMismatch / ErrorMismatch / NullHandling / TypeConversion |
| **Table(s)** | TPC-C table(s) involved |
| **Trigger SQL** | exact SQL that triggers the mismatch |
| **SQLite Result** | rows / error returned by SQLite |
| **SQLVibe Result** | rows / error returned by SQLVibe |
| **Root Cause** | explanation of the bug |
| **Fix** | how it was fixed |
| **Seed** | LCG seed that reproduces the mismatch |
| **Found By** | SQLValidator |
| **Date** | YYYY-MM-DD |
```

**Workflow**:
1. Add bug entry to `internal/TS/SQLValidator/HUNTINGS.md` with full details
2. Add regression test in `internal/TS/Regression/` or as a `TestSQLValidator_Regression` subcase
3. Fix the root cause in the engine
4. Commit and push
5. Do NOT add to HISTORY.md - HUNTINGS.md is the source of truth for SQLValidator bugs

**SQLValidator** uses an LCG random generator with the TPC-C schema as its starter
schema. It runs generated SQL against both SQLite and sqlvibe and compares results.
See `docs/plan-v0.9.15.md` for full design details.

---

## 4. Subsystem-Specific Guidelines

### 4.1 Data Storage (DS) Subsystem

**Assertions to Include**:
- Page validation: type (0x0d, 0x02, 0x05, 0x0a), size bounds, data length
- Cell operations: offset bounds, valid types, payload sizes
- B-Tree operations: key non-empty, cursor validity, path integrity
- Varint operations: buffer size >= VarintLen(value)
- Overflow chains: page numbers > 0, chain integrity

**Example**:
```go
func (bt *BTree) searchPage(page *Page, key []byte) ([]byte, error) {
    util.AssertNotNil(page, "page")
    util.Assert(len(page.Data) >= 12, "page data too small: %d bytes", len(page.Data))
    util.Assert(len(key) > 0, "search key cannot be empty")
    
    pageType := page.Data[0]
    util.Assert(pageType == 0x0d || pageType == 0x02 || pageType == 0x05 || pageType == 0x0a,
        "invalid page type: 0x%02x", pageType)
    // ...
}
```

### 4.2 Virtual Machine (VM) Subsystem

**Assertions to Include**:
- Cursor ID bounds: [0, MaxCursors)
- Register bounds: [0, NumRegs)
- Program counter validity
- Instruction parameter validation
- Context non-nil for subqueries

**Key Constants**:
- `MaxCursors = 256`
- Maximum register count varies by program

**Example**:
```go
case OpOpenRead:
    cursorID := int(inst.P1)
    util.Assert(cursorID >= 0 && cursorID < MaxCursors, 
        "cursor ID %d out of bounds [0, %d)", cursorID, MaxCursors)
    // ...
```

### 4.3 Query Processing (QP) Subsystem

**Assertions to Include**:
- Token array non-nil
- Parser state validity
- AST node structure validation
- Expression type checking

**Example**:
```go
func NewParser(tokens []Token) *Parser {
    util.AssertNotNil(tokens, "tokens")
    return &Parser{tokens: tokens, pos: 0}
}
```

### 4.4 Query Execution (QE) Subsystem

**Assertions to Include**:
- PageManager non-nil
- Table name non-empty
- Schema validation
- Row bounds checking
- Column index validity

**Example**:
```go
func (qe *QueryEngine) RegisterTable(name string, schema map[string]ColumnType) {
    util.Assert(name != "", "table name cannot be empty")
    util.AssertNotNil(schema, "schema")
    // ...
}
```

### 4.5 Transaction Management (TM) Subsystem

**Assertions to Include**:
- PageManager non-nil
- Transaction state validation
- Lock type validity
- WAL integrity checks

### 4.6 Platform Bridges (PB) Subsystem

**Assertions to Include**:
- File offsets non-negative
- Buffer non-nil
- Size parameters valid
- URI non-empty

**Example**:
```go
func (f *vfsFile) ReadAt(p []byte, off int64) (n int, err error) {
    util.AssertNotNil(p, "buffer")
    util.Assert(off >= 0, "offset cannot be negative: %d", off)
    return f.vfsHandle.Read(p, off)
}
```

---

## 5. Testing Guidelines

### 5.1 Unit Testing with Assertions

Assertions should not break valid test cases. Design tests to:
- Use valid inputs within documented bounds
- Test edge cases at boundaries
- Verify assertions catch invalid inputs (use defer/recover to test panics)

**Example**:
```go
func TestBTree_InvalidKey(t *testing.T) {
    defer func() {
        if r := recover(); r == nil {
            t.Error("Expected panic for empty key")
        }
    }()
    bt := NewBTree(pm, 1, true)
    bt.Search([]byte{}) // Should panic
}
```

### 5.2 Test-Friendly Assertions

Some assertions should be relaxed for testing:
- Page sizes: Allow < 512 bytes for unit tests
- Test data: Allow simplified structures
- Mock objects: May have limited implementations

**Example** (from `internal/DS/page.go`):
```go
func NewPage(num uint32, size int) *Page {
    util.Assert(size > 0, "page size %d must be positive", size)
    if size >= MinPageSize {
        util.Assert(IsValidPageSize(size), "page size %d must be power of 2", size)
    }
    // Allows smaller sizes for unit tests
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
│   └── sqlvibe/
│       └── storage/  # Hybrid storage engine (row/columnar)
├── internal/         # Internal packages
│   ├── pb/          # Platform Bridges (VFS)
│   ├── ds/          # Data Storage (B-Tree, pages)
│   ├── qp/          # Query Processing (parser)
│   ├── cg/          # Code Generator
│   ├── vm/          # Virtual Machine
│   ├── tm/          # Transaction Monitor
│   ├── is/          # Information Schema
│   └── ts/          # Test Suites
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

# Run all tests including extensions (recommended for full coverage)
go test -tags SVDB_EXT_JSON,SVDB_EXT_MATH ./...

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
2. Read `docs/plan-v0.8.0.md` for current implementation plan
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
- **Use assertions aggressively** to catch bugs early

---

## 13. Current Status: v0.8.0 - New Columnar Architecture

### 13.1 Completed Features

**Storage Layer (v0.8.0)**:
- ✅ HybridStore: Adaptive row/column storage switching
- ✅ ColumnVector: Typed column storage (int64, float64, string, bytes)
- ✅ RoaringBitmap: Fast bitmap indexes for O(1) filtering
- ✅ SkipList: Ordered data structure
- ✅ Arena allocator: Zero-GC query execution
- ✅ Persistence: New binary format (not SQLite compatible)

**Query Engine (v0.7.x - v0.8.0)**:
- ✅ Plan cache: Skip tokenize/parse/codegen for repeated queries
- ✅ Result cache: Full query result caching (FNV-1a keyed)
- ✅ Predicate pushdown: Evaluate WHERE at Go layer before VM
- ✅ Branch prediction: 2-bit saturating counter in OpNext

**Virtual Machine (VM)**:
- ✅ Cursor management with 256 max cursors
- ✅ Register-based execution (~200 opcodes)
- ✅ Expression evaluation
- ✅ Subquery handling with caching

### 13.2 Performance Achievements (v0.7.8)

| Benchmark | sqlvibe | SQLite Go | Winner |
|-----------|--------:|----------:|--------|
| SELECT all (1K) | 578 ns | 1,015 ns | **1,755x faster** |
| Result cache hit | <1 µs | 138 µs | **>100x faster** |
| GROUP BY | 1.34 µs | 539 µs | **2.5x faster** |
| INSERT single | 11.3 µs | 24.5 µs | **2.2x faster** |

### 13.3 Testing Status

All tests passing:
- ✅ SQL:1999 compatibility (56/56 suites)
- ✅ Unit tests
- ✅ Integration tests
- ✅ SQLite comparison tests
- ✅ Benchmarks

### 13.4 v0.8.0 Breaking Changes

- **SQLite file format compatibility**: REMOVED
- Database files are no longer readable by SQLite tools
- Only SQL interface remains compatible

### 13.5 Next Steps (see docs/plan-v0.8.0.md)

1. Complete remaining storage engine features
2. Add compression (LZ4, RLE)
3. Add encryption support
4. Improve WHERE filtering performance
