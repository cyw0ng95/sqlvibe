# OpenCode Agents Guidance

## Project: sqlvibe - High-Performance Database Engine (Go + C++)

This document provides guidance for OpenCode agents working on the sqlvibe project.

---

## 1. Project Overview

**Goal**: Implement a high-performance database engine with SQL:1999 compatibility using a hybrid Go + C++ architecture.

**Architecture**: C++ core engine with thin Go CGO wrappers
- **C++ Core** (`src/core/`): Query execution, storage, optimization (~15,000 LOC)
- **Go Wrappers** (`internal/`): Thin CGO bindings for type conversion (~600 LOC)
- **Public API** (`pkg/sqlvibe/`): High-level database operations (~800 LOC)

**Key Design Decision**: DS/CG/VM/QP/TM/PB/IS subsystems are **C++ only** with thin Go wrappers. All business logic lives in C++.

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

When PlainFuzzer confirms a bug, record it in `tests/PlainFuzzer/HUNTINGS.md` instead of HISTORY.md to avoid redundancy.

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
2. Add regression test in `tests/Regression/`
3. Commit and push
4. Do NOT add to HISTORY.md - HUNTINGS.md is the source of truth for fuzzer bugs

---

#### 8.4.9 SQLValidator Bug Tracking

When SQLValidator finds a correctness mismatch between sqlvibe and SQLite, record it in
`tests/SQLValidator/HUNTINGS.md` instead of HISTORY.md.

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
1. Add bug entry to `tests/SQLValidator/HUNTINGS.md` with full details
2. Add regression test in `tests/Regression/` or as a `TestSQLValidator_Regression` subcase
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
├── cmd/                    # CLI applications (sv-cli, sv-check)
├── pkg/                    # Public API
│   └── sqlvibe/
│       ├── database.go     # Main database API
│       └── database/       # Database implementation
├── src/                    # C++ Core Engine
│   ├── core/
│   │   ├── DS/            # Data Storage (B-Tree, pages, indexes)
│   │   ├── VM/            # Virtual Machine (opcode execution)
│   │   ├── QP/            # Query Processing (parser, optimizer)
│   │   ├── CG/            # Code Generation (bytecode compiler)
│   │   ├── TM/            # Transaction Management (MVCC, locks)
│   │   ├── PB/            # Platform Bridges (VFS, file I/O)
│   │   ├── IS/            # Information Schema
│   │   ├── SF/            # Standard Functions
│   │   └── svdb/          # Unified C API
│   └── ext/               # Extensions (FTS5, JSON, Math)
├── internal/               # Thin Go CGO Wrappers
│   ├── cgo/               # Main CGO bindings (db, stmt, rows, tx)
│   ├── DS/                # DS CGO wrappers
│   ├── VM/                # VM CGO wrappers
│   ├── QP/                # QP CGO wrappers
│   ├── CG/                # CG CGO wrappers
│   ├── TM/                # TM CGO wrappers
│   ├── PB/                # PB CGO wrappers (VFS)
│   └── IS/                # IS CGO wrappers (metadata)
├── tests/                  # Test suites
│   ├── Benchmark/         # Performance benchmarks
│   ├── Regression/        # Regression tests
│   ├── SQL1999/           # SQL:1999 compatibility tests
│   ├── Vtab/              # Virtual table tests
│   └── PlainFuzzer/       # Fuzz testing
├── docs/                   # Documentation
├── build.sh                # Build/test/benchmark script
├── CMakeLists.txt          # C++ build configuration
├── go.mod                  # Go module definition
└── go.sum                  # Go dependencies
```

**Key Directories**:
- `src/core/`: **C++ implementation** - All business logic, query execution, storage
- `internal/`: **Go CGO wrappers** - Type conversion, error mapping, memory management (NO business logic)
- `pkg/sqlvibe/`: **Public API** - User-facing database operations

---

## 11. Build and Test Commands

**Always use `build.sh` to run tests, benchmarks, fuzzing, and coverage.**
Output is collected under `.build/` (excluded from git).

### Build Process

The build process has two stages:
1. **C++ Build** (cmake): Compiles C++ core engine to `libsvdb.so`
2. **Go Build** (go build): Compiles Go wrappers and public API

```bash
# Build everything (C++ + Go)
./build.sh

# Run all unit tests (default when no flag is given)
./build.sh -t

# Run all unit tests + generate HTML coverage report (.build/coverage.html)
./build.sh -t -c

# Run benchmarks only
./build.sh -b

# Run tests + benchmarks + merged coverage report
./build.sh -t -b -c

# Run fuzz seed corpus (30 s per target by default)
./build.sh -f

# Fuzz for a longer duration per target
./build.sh -f --fuzz-time 5m

# Everything: tests + benchmarks + fuzz + coverage
./build.sh -t -b -f -c

# Verbose output
./build.sh -t -v
```

Options summary:

| Flag | Description |
|------|-------------|
| `-t` | Run unit tests (`go test -tags SVDB_EXT_JSON,SVDB_EXT_MATH ./...`) |
| `-b` | Run benchmarks (`./tests/Benchmark/...`) |
| `-f` | Run fuzz seed corpus for `FuzzSQL` and `FuzzDBFile` |
| `-c` | Collect coverage and produce `.build/coverage.html` |
| `--fuzz-time D` | Duration per fuzz target (e.g. `30s`, `5m`) |
| `-v` | Verbose test output |
| `-h` | Print help |

Direct `go test` commands (for IDE integration or CI):

```bash
# Build C++ first (required before Go build)
cd .build/cmake && cmake ../.. && cmake --build .

# Build the project (requires C++ libraries)
go build -tags SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_ENABLE_CGO ./...

# Run all tests (with extension tags)
go test -tags SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_ENABLE_CGO ./...

# Run specific test
go test -tags SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_ENABLE_CGO -run TestName ./...

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

## 13. Current Status: v0.11.2 — C++ Migration In Progress

### 13.1 Architecture Migration (v0.11.x)

**Major Architectural Change**: Migrating from Go to C++ for all core subsystems.

**Target Architecture**:
- **C++ Core** (`src/core/`): All business logic (~15,000 LOC)
- **Go Wrappers** (`internal/`): Thin CGO bindings (~600 LOC, 89% reduction)
- **Public API** (`pkg/sqlvibe/`): High-level operations (~800 LOC)

**Migration Progress** (see `docs/plan-v0.11.2.md`):

| Subsystem | C++ Status | Go Wrapper | Progress |
|-----------|------------|------------|----------|
| **VM** (Virtual Machine) | ✅ Complete (2000 LOC) | ✅ Thin (500 LOC) | ✅ Phase 1 Complete |
| **IS** (Info Schema) | ✅ Complete (300 LOC) | ✅ Thin (200 LOC) | ✅ Complete |
| **PB** (Platform/VFS) | ✅ Complete (240 LOC) | ✅ Thin (200 LOC) | ✅ Complete |
| **DS** (Data Storage) | ⚠️ Partial (3000 LOC) | ⚠️ Heavy (4000 LOC) | 🔄 Phase 2 In Progress |
| **QP** (Query Processing) | ⚠️ Partial (2000 LOC) | ⚠️ Heavy (4000 LOC) | ⏳ Phase 3 Pending |
| **CG** (Code Generation) | ⚠️ Partial (1500 LOC) | ⚠️ Heavy (2500 LOC) | ⏳ Phase 3 Pending |
| **TM** (Transaction Mgmt) | ⚠️ Partial (500 LOC) | ⚠️ Heavy (1500 LOC) | ⏳ Phase 3 Pending |
| **SF** (Standard Funcs) | ✅ Complete (500 LOC) | ✅ Minimal (200 LOC) | ✅ Complete |

**Overall Progress**: 45% Complete  
**Target Completion**: 2026-05-07 (12 weeks total)

### 13.2 Completed Features (v0.10.x)

**Storage Layer**:
- ✅ HybridStore: Adaptive row/column storage switching
- ✅ ColumnVector: Typed column storage (int64, float64, string, bytes)
- ✅ RoaringBitmap: Fast bitmap indexes for O(1) filtering
- ✅ B-Tree: Balanced tree storage with compression support
- ✅ WAL: Write-ahead logging with checkpoint and truncate
- ✅ PageManager: Memory-mapped page management
- ✅ Arena allocator: Zero-GC query execution

**Query Engine**:
- ✅ Bytecode VM: Register-based execution (200+ opcodes)
- ✅ Query optimizer: Predicate pushdown, constant folding
- ✅ Window functions: ROW_NUMBER, RANK, LAG, LEAD, etc.
- ✅ WINDOW clause: Named window specifications
- ✅ Window frames: ROWS/RANGE BETWEEN support
- ✅ CTEs: Recursive and non-recursive common table expressions
- ✅ Set operations: UNION, INTERSECT, EXCEPT (with ALL)

**Extensions**:
- ✅ FTS5 (v0.10.2): Full-text search with BM25 ranking
- ✅ JSON (v0.9.17): JSON1-compatible functions
- ✅ Math: Advanced math functions (POWER, LOG, trig)
- ✅ Virtual tables: series(), json_each(), etc.

**Storage Enhancements (v0.10.4)**:
- ✅ PRAGMA wal_truncate: Auto-truncate WAL after checkpoint
- ✅ PRAGMA synchronous: OFF/NORMAL/FULL/EXTRA modes
- ✅ PRAGMA memory_stats: Detailed memory usage statistics
- ✅ PRAGMA cache_memory: Page cache memory budget
- ✅ PRAGMA max_rows: Row limit per table
- ✅ BackupToWithCallback: Streaming backup with progress
- ✅ BackupToWriter: Backup to any io.Writer
- ✅ BackupManifest: Enhanced backup metadata

### 13.3 Performance Achievements (v0.10.0)

| Benchmark | sqlvibe | SQLite Go | Winner |
|-----------|--------:|----------:|--------|
| SELECT all (1K) | 182 µs | 292 µs | **1.6x faster** |
| WHERE filter (1K) | 104 µs | 188 µs | **1.8x faster** |
| SUM aggregate (1K) | 20.7 µs | 66.7 µs | **3.2x faster** |
| GROUP BY (1K) | 128 µs | 480 µs | **3.8x faster** |

**v0.11.2 Target Improvements** (after full C++ migration):
- SELECT 1K: 263 µs → <200 µs (24% faster)
- SUM aggregate: 28 µs → <20 µs (29% faster)
- GROUP BY: 148 µs → <100 µs (32% faster)
- INNER JOIN: 1.12 ms → <0.8 ms (29% faster)

### 13.4 Testing Status

All tests passing (except pre-existing datetime tests):
- ✅ SQL:1999 compatibility (84+ test suites)
- ✅ Unit tests (75%+ coverage in core packages)
- ✅ Integration tests
- ✅ SQLite comparison tests
- ✅ Benchmarks
- ⚠️ F051 datetime functions (pre-existing, unrelated)

### 13.5 Recent Releases

| Version | Date | Features |
|---------|------|----------|
| v0.11.2 | 2026-03-04 | Architecture: C++ migration plan, IS/PB moved to internal |
| v0.10.4 | 2026-03-01 | Storage: WAL truncate, memory stats, streaming backup |
| v0.10.3 | 2026-03-01 | Advanced SQL: WINDOW clause, JSON_KEYS |
| v0.10.2 | 2026-03-01 | FTS5: Full-text search with BM25 |
| v0.10.1 | 2026-02-28 | Coverage: +25% in critical packages |
| v0.10.0 | 2026-02-27 | Bytecode VM: Always-on execution engine |

### 13.6 Next Steps (see docs/plan-v0.11.2.md)

**Phase 2: DS Layer** (2026-03-04 to 2026-03-25):
1. [ ] Create DS CGO wrapper (`internal/DS/ds_cgo.go`)
2. [ ] Migrate HybridStore.Insert() to C++
3. [ ] Migrate HybridStore.Scan() to C++
4. [ ] Migrate BTree.Search/Insert to C++
5. [ ] Remove Go DS implementations

**Phase 3: QP/CG/TM** (2026-03-26 to 2026-04-22):
- Migrate query processing to C++
- Migrate code generation to C++
- Migrate transaction management to C++

**Phase 4: Cleanup** (2026-04-23 to 2026-05-07):
- Remove all legacy Go implementations
- Final performance optimization
- Documentation updates
- Tag v0.11.2 release

---

## 14. CI / Security Scanning

### 14.1 CodeQL

**CodeQL is disabled for this repository** (`if: false` in `.github/workflows/codeql.yml`).

**Reasons:**
1. The project builds CGO shared libraries (`.so` files via cmake) before `go build`.
   CodeQL's default autobuild cannot satisfy this dependency, causing false build
   failures and misleading alerts.
2. The C++ sources intentionally use manual memory management and SIMD pointer
   arithmetic. These patterns produce large volumes of false-positive alerts.
3. Go-layer security is reviewed by `go vet` and manual inspection.

**If you need to re-enable CodeQL:**
1. Edit `.github/workflows/codeql.yml` and remove the `if: false` line.
2. Add a custom build step that runs `./build.sh` **before** the CodeQL autobuild
   so that all required `.so` files exist at analysis time.
3. Suppress false positives in C++ sources with `// lgtm[cpp/...]` annotations
   or a `.github/codeql/codeql-config.yml` exclusion list.

**Agents must NOT re-enable CodeQL without following the steps above.**
