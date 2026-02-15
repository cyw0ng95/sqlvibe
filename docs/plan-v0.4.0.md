# Plan v0.4.0

## Goal
Implement transaction management (BEGIN, COMMIT, ROLLBACK), index support (CREATE INDEX, DROP INDEX), PRAGMA statements, and set operations (UNION, EXCEPT, INTERSECT) for complete SQL:1999 Phase 2 compliance.

## Requirements

### HIGH Priority
- BEGIN/COMMIT/ROLLBACK transactions (from sqlite.reqs.md, sql1999.reqs.md)
- CREATE INDEX (from sqlite.reqs.md)
- DROP INDEX (from sqlite.reqs.md)
- Index usage in query planning (from sqlite.reqs.md)
- UNION/EXCEPT/INTERSECT set operations (from sql1999.reqs.md E071)

### MEDIUM Priority
- PRAGMA statements (pragma_table_info, pragma_index_list, pragma_database_list)
- CASE WHEN expressions (from sqlite.reqs.md)
- DISTINCT improvements (from sqlite.reqs.md)
- LIMIT with OFFSET improvements (from sqlite.reqs.md)
- Date/Time types (from sql1999.reqs.md F051)
- Improved subquery support (ALL/ANY, EXISTS improvements)

## Implementation DAG

```mermaid
graph LR
    TX[Transaction Manager] --> IX[Index Support]
    IX --> PG[PRAGMA Statements]
    PG --> SO[Set Operations]
    SO --> CW[CASE Expressions]
    CW --> DT[Date/Time Types]
    
    TX --> TX_P[Transaction Parser]
    TX_P --> TX_E[Transaction Engine]
    TX_E --> IX_E[Index Engine]
    
    IX --> IX_P[Index Parser]
    IX_P --> IX_E
```

**Notes:**
- Transaction management requires TM (Transaction Monitor) subsystem
- Index support builds on existing DS (Data Storage) B-Tree
- Set operations require query engine modifications

## Detailed Design

### 1. Transaction Manager (TM) Enhancements

#### 1.1 BEGIN Transaction
- **Parser changes**: Detect BEGIN [TRANSACTION] keyword
- **Engine changes**: Implement transaction state machine
- **Files affected**: `internal/TM/transaction.go`, `pkg/sqlvibe/database.go`

**Implementation:**
- Track transaction state (IDLE, ACTIVE, COMMITTED, ROLLBACK)
- Support BEGIN, BEGIN EXCLUSIVE
- Handle savepoints

#### 1.2 COMMIT Transaction
- **Parser changes**: Detect COMMIT [TRANSACTION] keyword
- **Engine changes**: Write changes to database, clear undo log
- **Files affected**: `internal/TM/transaction.go`, `internal/TM/wal.go`

#### 1.3 ROLLBACK Transaction
- **Parser changes**: Detect ROLLBACK [TRANSACTION] [TO SAVEPOINT]
- **Engine changes**: Restore database to pre-transaction state
- **Files affected**: `internal/TM/transaction.go`, `internal/TM/wal.go`

#### 1.4 Savepoint Support
- **Goal**: Support nested transactions with savepoints
- **Files affected**: `internal/TM/transaction.go`

**Implementation:**
- Track savepoint stack
- Support RELEASE SAVEPOINT
- Implement transaction isolation levels

### 2. Index Support

#### 2.1 CREATE INDEX
- **Parser changes**: Parse CREATE [UNIQUE] INDEX statement
- **Engine changes**: Create new B-Tree for index
- **Files affected**: `internal/QP/parser.go`, `internal/DS/btree.go`

**Implementation:**
```
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_name [, ...])
```

#### 2.2 DROP INDEX
- **Parser changes**: Parse DROP INDEX statement
- **Engine changes**: Remove index B-Tree from database
- **Files affected**: `internal/QP/parser.go`, `internal/DS/manager.go`

#### 2.3 Index Usage in Queries
- **Goal**: Use indexes for WHERE clause optimization
- **Files affected**: `internal/QP/planner.go`, `internal/QE/engine.go`

**Implementation:**
- Index selection in query planner
- Index scan operator
- Covering index support

### 3. PRAGMA Statements

#### 3.1 pragma_table_info
- **Goal**: Return table column information
- **Syntax**: `PRAGMA table_info(table_name)`
- **Files affected**: `pkg/sqlvibe/database.go`

**Implementation:**
- Parse PRAGMA statement
- Return columns: cid, name, type, notnull, dflt_value, pk

#### 3.2 pragma_index_list
- **Goal**: Return list of indexes for table
- **Syntax**: `PRAGMA index_list(table_name)`
- **Files affected**: `pkg/sqlvibe/database.go`

#### 3.3 pragma_database_list
- **Goal**: Return list of attached databases
- **Syntax**: `PRAGMA database_list`
- **Files affected**: `pkg/sqlvibe/database.go`

### 4. Set Operations (UNION, EXCEPT, INTERSECT)

#### 4.1 UNION
- **Parser changes**: Detect UNION [ALL] between SELECT statements
- **Engine changes**: Implement set union operator
- **Files affected**: `internal/QP/parser.go`, `internal/QE/operators.go`

**Implementation:**
- Parse: `SELECT ... UNION [ALL] SELECT ...`
- Implement union-all (no deduplication)
- Implement union (with deduplication using hash or sort)

#### 4.2 EXCEPT
- **Parser changes**: Detect EXCEPT between SELECT statements
- **Engine changes**: Implement set difference operator
- **Files affected**: `internal/QP/parser.go`, `internal/QE/operators.go`

#### 4.3 INTERSECT
- **Parser changes**: Detect INTERSECT between SELECT statements
- **Engine changes**: Implement set intersection operator
- **Files affected**: `internal/QP/parser.go`, `internal/QE/operators.go`

### 5. CASE WHEN Expressions

#### 5.1 Simple CASE
- **Goal**: `CASE expr WHEN value THEN result ... [ELSE result] END`
- **Parser changes**: Parse CASE expression
- **Engine changes**: Implement case evaluation in expression evaluator
- **Files affected**: `internal/QP/ast.go`, `internal/QE/expr.go`

#### 5.2 Searched CASE
- **Goal**: `CASE WHEN condition THEN result ... [ELSE result] END`
- **Parser changes**: Same as simple CASE
- **Engine changes**: Evaluate each WHEN condition

### 6. Date/Time Types (F051)

#### 6.1 DATE Type
- **Goal**: Support DATE column type
- **Files affected**: `internal/QP/parser.go`, `internal/DS/page.go`

#### 6.2 TIME Type
- **Goal**: Support TIME column type
- **Files affected**: `internal/QP/parser.go`, `internal/DS/page.go`

#### 6.3 TIMESTAMP Type
- **Goal**: Support TIMESTAMP column type
- **Files affected**: `internal/QP/parser.go`, `internal/DS/page.go`

#### 6.4 Datetime Functions
- **Goal**: CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP
- **Files affected**: `internal/QE/expr.go`

## Success Criteria

- [ ] BEGIN transaction starts transaction state
- [ ] COMMIT writes changes and clears transaction state
- [ ] ROLLBACK restores database to pre-transaction state
- [ ] Savepoints work correctly (nested transactions)
- [ ] CREATE INDEX creates index B-Tree
- [ ] DROP INDEX removes index
- [ ] Query planner uses indexes for WHERE optimization
- [ ] PRAGMA table_info returns column metadata
- [ ] PRAGMA index_list returns indexes for table
- [ ] PRAGMA database_list returns attached databases
- [ ] UNION returns combined results
- [ ] UNION ALL returns combined results without deduplication
- [ ] EXCEPT returns difference of two result sets
- [ ] INTERSECT returns intersection of two result sets
- [ ] Simple CASE expression works correctly
- [ ] Searched CASE expression works correctly
- [ ] DATE type stores dates correctly
- [ ] TIME type stores times correctly
- [ ] TIMESTAMP type stores timestamps correctly
- [ ] Datetime functions (CURRENT_DATE, etc.) work correctly

## Notes

- Transactions: Test with `go test -run TestTransaction ./pkg/sqlvibe`
- Indexes: Test with `go test -run TestIndex ./pkg/sqlvibe`
- PRAGMA: Test with `go test -run TestPragma ./pkg/sqlvibe`
- Set Operations: Test with `go test -run TestSetOperations ./pkg/sqlvibe`
- CASE: Test with `go test -run TestCaseExpression ./pkg/sqlvibe`
- Date/Time: Test with `go test -run TestDateTime ./pkg/sqlvibe`
- Index usage in queries requires planner modifications
