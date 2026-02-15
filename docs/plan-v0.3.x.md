# Plan v0.3.x

## Goal
Implement JOIN support (INNER, LEFT, CROSS) and sqlite_master table for basic query capabilities. Also refactor codebase for better maintainability.

## Requirements

### HIGH Priority
- INNER JOIN (from sqlite.reqs.md, sql1999.reqs.md)
- LEFT JOIN (from sqlite.reqs.md, sql1999.reqs.md)
- sqlite_master table (from sqlite.reqs.md)
- Code refactoring: Split database.go into smaller files

### MEDIUM Priority
- CROSS JOIN (from sqlite.reqs.md, sql1999.reqs.md)
- WHERE clause operators: AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL
- COALESCE/IFNULL functions

## Implementation DAG

```mermaid
graph LR
    P[JOIN Parser] --> E[JOIN Engine]
    E --> CJ[CROSS JOIN]
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

## Success Criteria

- [x] sqlite_master table returns table list
- [x] Parser handles JOIN syntax
- [x] INNER JOIN returns correct matched rows
- [x] LEFT JOIN includes unmatched left rows with NULL
- [x] CROSS JOIN produces Cartesian product
- [x] All JOIN tests pass (TestQueryJoins, TestMultipleTables)
- [x] WHERE clause operators: AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL
- [x] COALESCE/IFNULL functions
- [ ] Subqueries (NOT YET IMPLEMENTED)

## Notes
- JOINs share similar nested-loop implementation pattern
- Test with: `go test -run TestQueryJoins ./pkg/sqlvibe`
- Subqueries (scalar, IN, EXISTS, correlated) NOT YET IMPLEMENTED - requires parser and engine changes
