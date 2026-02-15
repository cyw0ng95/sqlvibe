# Plan v0.3.x

## Goal
Implement JOIN support (INNER, LEFT, CROSS), sqlite_master table, WHERE clause operators, COALESCE/IFNULL, and subqueries for full query capabilities.

## Requirements

### HIGH Priority
- INNER JOIN (from sqlite.reqs.md, sql1999.reqs.md)
- LEFT JOIN (from sqlite.reqs.md, sql1999.reqs.md)
- sqlite_master table (from sqlite.reqs.md)
- Subqueries: Scalar, EXISTS, IN, ALL/ANY, Correlated

### MEDIUM Priority
- CROSS JOIN (from sqlite.reqs.md, sql1999.reqs.md)
- WHERE clause operators: AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL
- COALESCE/IFNULL functions
- :memory: database support (in-memory database)
- TS: Test Suites subsystem for sqlvibe

## Implementation DAG

```mermaid
graph LR
    P[JOIN Parser] --> E[JOIN Engine]
    E --> CJ[CROSS JOIN]
    CJ --> SQ[Subqueries]
    SQ --> SQ_P[Subquery Parser]
    SQ_P --> SQ_E[Subquery Engine]
    SQ_E --> MEM[:memory: Support]
    MEM --> TS[TS: Test Suites]
```

**Notes:**
- sqlite_master is complete
- JOIN requires parser + engine changes
- JOINs share similar nested-loop implementation pattern

## Detailed Design

### 1. sqlite_master Table ✅ DONE
- **Parser changes**: None required (table name in FROM clause)
- **Engine changes**: Add special case in Query() to detect `sqlite_master` table name and return virtual table schema
- **Files affected**: `pkg/sqlvibe/database.go`

### 2. JOIN Parser Changes
- **Goal**: Extend parser to handle JOIN syntax in FROM clause
- **Files affected**: `internal/QP/parser.go`

**Implementation:**
- Parse table aliases (e.g., `users u`)
- Parse JOIN type (INNER, LEFT, CROSS)
- Parse ON condition
- Populate Join struct in TableRef

### 3. INNER JOIN
- **Parser changes**: Extend parseSelect to handle JOIN after table name
- **Engine changes**: Implement nested loop join for INNER JOIN evaluation
- **Files affected**: `pkg/sqlvibe/database.go`

### 4. LEFT JOIN
- **Parser changes**: Same as INNER JOIN
- **Engine changes**: Similar to INNER JOIN but include left rows with NULL for no match
- **Files affected**: `pkg/sqlvibe/database.go`

### 5. CROSS JOIN
- **Parser changes**: Same as INNER JOIN
- **Engine changes**: Cartesian product of two tables
- **Files affected**: `pkg/sqlvibe/database.go`

### 6. Code Refactoring (Optional)
- **Goal**: Split database.go (1100+ lines) into smaller, focused files
- **Proposed structure:**
  - `database.go` - Database struct and connection handling
  - `query.go` - Query() and Exec() methods
  - `join.go` - JOIN implementations
  - `expression.go` - evalWhere, evalExpr, comparison functions
  - `aggregate.go` - computeAggregate, computeGroupBy
- **Files affected**: Create new files in `pkg/sqlvibe/`

### 7. Subqueries
- **Parser changes**: Detect subqueries in WHERE clause and SELECT columns
- **Engine changes**: Execute subqueries and use results in parent query
- **Files affected**: `internal/QP/parser.go`, `pkg/sqlvibe/database.go`

**Implementation:**
- Parse `(SELECT ...)` as SubqueryExpr in WHERE clause
- Detect EXISTS `(SELECT ...)` 
- Detect `IN (SELECT ...)` 
- Detect `ALL/ANY (SELECT ...)`
- Execute subquery, return scalar value or boolean

### 7.1 Scalar Subquery
- `SELECT (SELECT col FROM t2 WHERE ...)`
- Returns single value
- Must return at most one row

### 7.2 EXISTS Subquery
- `WHERE EXISTS (SELECT 1 FROM t2 WHERE ...)`
- Returns TRUE if subquery returns any rows

### 7.3 IN Subquery  
- `WHERE col IN (SELECT col FROM t2 ...)`
- Returns TRUE if value matches any result

### 7.4 ALL/ANY Subquery
- `WHERE col > ALL (SELECT ...)`
- `WHERE col > ANY (SELECT ...)`
- Quantified comparison

### 8. :memory: Database Support
- **Goal**: Support opening an in-memory database with `Open(":memory:")`
- **Files affected**: `pkg/sqlvibe/database.go`

**Implementation:**
- Detect if path == ":memory:" in Open() function
- Skip file operations for :memory: database
- Use in-memory data structures only (no page manager needed)
- Return empty/in-memory database struct

### 9. TS: Test Suites Subsystem
- **Goal**: Create a dedicated test subsystem for sqlvibe tests
- **Files affected**: Create `internal/TS/` directory

**Implementation:**
- Create `internal/TS/` directory following other subsystem patterns (PB, DS, QP, QE, TM)
- Implement test utilities and helpers
- Support sqllogictest format compatibility
- Provide test data management utilities
- Coordinate with existing test files in `pkg/sqlvibe/`

**Directory Structure:**
```
internal/TS/
├── ts.go          # Test suite utilities
├── testdata/      # Test data files
└── helpers.go     # Test helper functions
```

**Naming Convention:**
All tests must use the following naming convention:
- **Testsuite Name**: High-level grouping (e.g., "DML", "Query", "Transaction")
- **Test Case Name**: Specific test scenario (e.g., "InsertSingle", "SelectWhere")
- **Test Level**: Unit, Integration, or Blackbox

**Interface Design:**
```go
type TestCase struct {
    Testsuite string  // e.g., "DML", "Query", "Transaction"
    Name      string  // e.g., "InsertSingle", "SelectWhere" 
    Level     string  // "unit", "integration", "blackbox"
    Fn        func(*testing.T)
}

// Register test case
func RegisterTest(ts, name, level string, fn func(*testing.T))
```

All existing and new test cases must use this framework to group tests consistently.

**Storage Backend:**
- Non-storage related tests MUST use `:memory:` as the storage backend
- Only tests that specifically require file storage (e.g., transaction durability, crash recovery) should use file-based storage
- This ensures test isolation and improves performance

## Success Criteria

- [x] sqlite_master table returns table list
- [x] Parser handles JOIN syntax
- [x] INNER JOIN returns correct matched rows
- [x] LEFT JOIN includes unmatched left rows with NULL
- [x] CROSS JOIN produces Cartesian product
- [x] All JOIN tests pass (TestQueryJoins, TestMultipleTables)
- [x] WHERE clause operators: AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL
- [x] COALESCE/IFNULL functions
- [x] Scalar subquery in SELECT
- [x] EXISTS subquery
- [x] IN subquery
- [x] ALL/ANY subquery
- [x] Correlated subquery
- [ ] :memory: database support
- [ ] TS: Test Suites subsystem created

## Notes
- JOINs share similar nested-loop implementation pattern
- Test with: `go test -run TestQueryJoins ./pkg/sqlvibe`
- Subquery types: Scalar, EXISTS, IN, ALL/ANY, correlated
- Test with: `go test -run TestQuerySubqueries ./pkg/sqlvibe`
- :memory: support - Open(":memory:") should create in-memory database
