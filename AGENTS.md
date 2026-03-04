# sqlvibe Agent Guidelines

## Project Overview

**Goal**: High-performance SQL:1999-compatible database engine.

**Architecture**: C++ core with thin Go CGO wrappers.

```
src/core/           → C++ implementations (DS, VM, QP, CG, TM, PB, IS)
internal/           → Thin Go CGO wrappers (~600 LOC, type conversion only)
pkg/sqlvibe/        → Public API (~800 LOC)
```

**Key Rule**: DS/CG/VM/QP/TM/PB/IS subsystems are **C++ only**. Go wrappers do type conversion only—NO business logic.

---

## Restrictions

### Git
- **NEVER** force push
- Meaningful commit messages
- Commit frequently

### Bugfixes
Format: `#bugfix: Description`

```
#bugfix: Fix correlated subquery JOIN

- Root cause: Correlation variables not propagated
- Fix: Added correlation tracking in binder
- Test: Added TestRegression_CorrelatedSubquery_L1
```

### Code Quality
- **NEVER** suppress types (`any`, `@ts-ignore`)
- **NEVER** empty catch blocks
- **NEVER** delete failing tests
- Fix root causes, not symptoms
- **NO external packages** - Go stdlib + existing codebase only

### Assertions

Use `internal/SF/util/assert.go` for defensive programming:

```go
// Validate inputs
util.Assert(len(key) > 0, "search key cannot be empty")
util.AssertNotNil(page, "page")
util.Assert(pageNum > 0, "page number must be positive: %d", pageNum)

// Validate bounds
util.Assert(cursorID >= 0 && cursorID < MaxCursors,
    "cursor ID %d out of bounds [0, %d)", cursorID, MaxCursors)

// Validate data structures
util.Assert(pageType == 0x0d || pageType == 0x02 || pageType == 0x05 || pageType == 0x0a,
    "invalid page type: 0x%02x", pageType)
```

**When to assert**:
1. Preconditions (non-nil, valid ranges, non-empty)
2. Invariants (array bounds, valid enums, cursor/register bounds)
3. Data integrity (page types, cell offsets, varint sizes)

**Assertion vs Error**:
- **Assertions**: Programming errors (bugs, violated invariants)
- **Errors**: Runtime errors (I/O, user input, resources)

**Key Constants**:
- `MaxCursors = 256` (VM)
- `MinPageSize = 512`, `MaxPageSize = 65536` (DS)
- Page types: `0x0d` (table leaf), `0x02` (index leaf), `0x05` (table interior), `0x0a` (index interior)

---

## Development Workflow

### Before Starting
1. Read `docs/plan-v0.11.2.md` for current tasks
2. Check existing tests for expected behavior
3. Understand C++ ↔ Go boundary for your task

### Implementation Rules
1. **C++ First**: New features in C++, Go wrapper second
2. **Test-Driven**: Write tests alongside implementation
3. **SQLite Compatible**: Test against SQLite frequently

### Verification Checklist
Before marking complete:
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] SQLite comparison tests pass
- [ ] No regressions
- [ ] Code follows conventions

### Commit Timing
**Commit immediately when task completes:**
```bash
go test ./...
git add -A
git commit -m "feat: Implement B-Tree search

- Handles leaf and interior nodes
- Added unit tests"
git push
```

### Track Progress
1. Read `docs/plan-v0.11.2.md` at session start
2. Find next incomplete task
3. Update plan after finishing (mark `[x]`)
4. Commit plan changes:
```bash
git add -A
git commit -m "docs: Update plan-v0.11.2 progress"
git push
```

---

## Subsystem Guidelines

### DS (Data Storage)
**Assertions**: Page types, cell offsets, varint bounds, cursor validity

```go
func (bt *BTree) Search(key []byte) ([]byte, error) {
    util.Assert(len(key) > 0, "search key cannot be empty")
    util.Assert(len(page.Data) >= 12, "page data too small: %d bytes", len(page.Data))
    // ...
}
```

### VM (Virtual Machine)
**Assertions**: Cursor bounds `[0, 256)`, register bounds, PC validity

```go
case OpOpenRead:
    util.Assert(cursorID >= 0 && cursorID < 256,
        "cursor ID %d out of bounds", cursorID)
```

### QP (Query Processing)
**Assertions**: Token arrays non-nil, parser state valid, AST structure

### PB (Platform/VFS)
**Assertions**: Offsets non-negative, buffers non-nil, URIs non-empty

```go
func (f *File) ReadAt(p []byte, off int64) (int, error) {
    util.AssertNotNil(p, "buffer")
    util.Assert(off >= 0, "offset cannot be negative: %d", off)
    // ...
}
```

---

## Testing

### Test Types
- **Unit**: `go test ./internal/...`
- **SQL:1999**: `tests/SQL1999/` (84+ suites)
- **Regression**: `tests/Regression/` (bug prevention)
- **Benchmarks**: `tests/Benchmark/`
- **Fuzz**: `tests/PlainFuzzer/`

### Build Commands

```bash
# Build everything (C++ + Go)
./build.sh

# Run tests
./build.sh -t

# Tests + coverage
./build.sh -t -c

# Benchmarks
./build.sh -b

# All: tests + benchmarks + fuzz + coverage
./build.sh -t -b -f -c
```

### Regression Test Format

```go
// TestRegression_CoalesceNULL_L1
// Bug: COALESCE(NULL, 'default') returned NULL
func TestRegression_CoalesceNULL_L1(t *testing.T) {
    sqlvibePath := ":memory:"
    sqlitePath := ":memory:"
    // ... test code
}
```

**When to add**:
- SQL pattern produces wrong result vs SQLite
- Specific edge case causes panic
- Bug was fixed (prevent regression)

---

## File Structure

```
sqlvibe/
├── cmd/                    # CLI (sv-cli, sv-check)
├── pkg/sqlvibe/            # Public API
│   ├── database.go         # Main API
│   └── database/           # Implementation
├── src/core/               # C++ Core
│   ├── DS/                 # Data Storage
│   ├── VM/                 # Virtual Machine
│   ├── QP/                 # Query Processing
│   ├── CG/                 # Code Generation
│   ├── TM/                 # Transaction Mgmt
│   ├── PB/                 # Platform/VFS
│   ├── IS/                 # Information Schema
│   └── svdb/               # C API
├── internal/               # Go CGO Wrappers
│   ├── cgo/                # Main bindings
│   ├── DS/                 # DS wrappers
│   ├── VM/                 # VM wrappers
│   └── ...                 # Other wrappers
├── tests/                  # Test suites
│   ├── Benchmark/
│   ├── Regression/
│   ├── SQL1999/
│   └── PlainFuzzer/
├── docs/
│   ├── plan-v0.11.2.md     # Current migration plan
│   └── ARCHITECTURE.md
├── build.sh                # Build script
└── CMakeLists.txt          # C++ build
```

---

## Current Status: v0.11.2

**C++ Migration Progress**: 45% complete

| Subsystem | C++ | Go Wrapper | Status |
|-----------|-----|------------|--------|
| VM | ✅ 2000 LOC | ✅ 500 LOC | ✅ Complete |
| IS | ✅ 300 LOC | ✅ 200 LOC | ✅ Complete |
| PB | ✅ 240 LOC | ✅ 200 LOC | ✅ Complete |
| DS | ⚠️ 3000 LOC | ⚠️ 4000 LOC | 🔄 In Progress |
| QP/CG/TM | ⚠️ Partial | ⚠️ Heavy | ⏳ Pending |

**Target**: 89% Go reduction (21,900 → 2,350 LOC) by 2026-05-07

**Next Tasks** (see `docs/plan-v0.11.2.md`):
1. Create DS CGO wrapper
2. Migrate HybridStore to C++
3. Migrate BTree operations to C++

---

## Getting Help

1. `docs/ARCHITECTURE.md` - System design
2. `docs/plan-v0.11.2.md` - Current tasks
3. Existing tests - Expected behavior
4. Ask Oracle agent if stuck

---

## Summary

- **No force push**
- Use `#bugfix` for fixes
- C++ first, Go wrappers second
- Test against SQLite frequently
- Commit frequently with meaningful messages
- Use assertions aggressively
