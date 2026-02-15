# Plan v0.4.0 - Detailed Execution Plan

## Goal
Implement index support (CREATE INDEX, DROP INDEX), PRAGMA statements, set operations (UNION, EXCEPT, INTERSECT), CASE WHEN expressions, full E021 character data types support, Date/Time types, and fix E011/F481 failure cases for SQL:1999 Phase 2 compliance.

---

## Execution DAG (Parallel Waves)

```mermaid
graph TD
    %% Wave 1: Critical Bugfixes (All Parallel)
    subgraph W1 ["Wave 1: Critical Bugfixes"]
        direction
        B1[E011-02: Float Math<br/>Abs/Ceil/Floor/Round]
        B2[E011-03: DECIMAL/NUMERIC<br/>Arithmetic]
        B3[E011-04: Arithmetic Ops<br/>Unary Minus/Large#]
        B4[E011-05: Comparison<br/>NULL Predicates]
        B5[E011-06: Numeric<br/>Casting]
        B6[F481: COALESCE<br/>NULL Handling]
    end
    
    %% Wave 2: Parser Extensions
    subgraph W2 ["Wave 2: Parser Extensions"]
        direction TB
        P1[Index Parser<br/>CREATE/DROP INDEX]
        P2[Set Ops Parser<br/>UNION/EXCEPT/INTERSECT]
        P3[CASE Parser<br/>Simple/Searched]
        P4[E021 Parser<br/>CHAR/VARCHAR/Functions]
    end
    
    %% Wave 3: Engine Implementations
    subgraph W3 ["Wave 3: Engine Implementations"]
        direction TB
        E1[Index Engine<br/>B-Tree Operations]
        E2[Set Ops Engine<br/>Union/Except/Intersect]
        E3[CASE Engine<br/>Expression Eval]
        E4[E021 Engine<br/>String Functions]
        E5[PRAGMA Engine<br/>table_info/index_list]
    end
    
    %% Wave 4: Advanced Features
    subgraph W4 ["Wave 4: Advanced Features"]
        direction TB
        A1[Index Usage<br/>Query Planner]
        A2[Date/Time Types<br/>DATE/TIME/TIMESTAMP]
    end
    
    %% Dependencies
    W1 --> W2
    W2 --> W3
    W3 --> W4
    
    %% Within-wave parallel dependencies
    B1 -.-> B2
    B1 -.-> B3
    B1 -.-> B4
    B1 -.-> B5
    B1 -.-> B6
    B2 -.-> B3
    B2 -.-> B4
    B2 -.-> B5
    B2 -.-> B6
    B3 -.-> B4
    B3 -.-> B5
    B3 -.-> B6
    B4 -.-> B5
    B4 -.-> B6
    B5 -.-> B6
```

---

## Wave 1: Critical Bugfixes (Parallel - 6 tasks)

### Task 1.1: E011-02 Float Math Functions
- **Feature**: Abs, Ceil, Floor, Round functions
- **Files**: `internal/QE/expr.go`
- **Issue**: Functions returning NULL instead of correct values
- **Tests**: `TestSQL1999_F301_E01102_L1`

### Task 1.2: E011-03 DECIMAL/NUMERIC Arithmetic
- **Feature**: Decimal arithmetic operations
- **Files**: `internal/QE/expr.go`
- **Issue**: Decimal add/sub/mul/div returning wrong results
- **Tests**: `TestSQL1999_F301_E01103_L1`

### Task 1.3: E011-04 Arithmetic Operators
- **Feature**: Unary minus, large number operations
- **Files**: `internal/QE/expr.go`
- **Issue**: Unary minus on column refs, large number arithmetic
- **Tests**: `TestSQL1999_F301_E01104_L1`

### Task 1.4: E011-05 Comparison & NULL Predicates
- **Feature**: ORDER BY expressions, NULL IS NULL/IS NOT NULL
- **Files**: `internal/QE/expr.go`
- **Issue**: NULL predicates returning NULL instead of 0/1
- **Tests**: `TestSQL1999_F301_E01105_L1`

### Task 1.5: E011-06 Implicit Numeric Casting
- **Feature**: Mixed type arithmetic and comparison
- **Files**: `internal/QE/expr.go`
- **Issue**: INT + REAL, DECIMAL + REAL casting
- **Tests**: `TestSQL1999_F301_E01106_L1`

### Task 1.6: F481 COALESCE Function
- **Feature**: COALESCE with NULL values
- **Files**: `internal/QE/expr.go`
- **Issue**: COALESCE returning NULL when first arg non-NULL
- **Tests**: `TestSQLite_F481_NULLs_L1`

**Wave 1 Verification**: `go test -race -asan ./...`

---

## Wave 2: Parser Extensions (Parallel - 4 tasks)

### Task 2.1: Index Parser
- **Feature**: Parse CREATE [UNIQUE] INDEX, DROP INDEX
- **Syntax**: `CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON t(col)`
- **Files**: `internal/QP/parser.go`
- **Dependencies**: Wave 1 complete

### Task 2.2: Set Operations Parser
- **Feature**: Parse UNION [ALL], EXCEPT, INTERSECT
- **Syntax**: `SELECT ... UNION [ALL] SELECT ...`
- **Files**: `internal/QP/parser.go`, `internal/QP/ast.go`
- **Dependencies**: Wave 1 complete

### Task 2.3: CASE Parser
- **Feature**: Parse CASE WHEN expressions
- **Syntax**: `CASE expr WHEN val THEN res ... END`
- **Files**: `internal/QP/parser.go`, `internal/QP/ast.go`
- **Dependencies**: Wave 1 complete

### Task 2.4: E021 Character Parser
- **Feature**: Parse CHAR, VARCHAR, string functions
- **Syntax**: `CHAR(10)`, `VARCHAR(255)`, `SUBSTRING(...)`, etc.
- **Files**: `internal/QP/parser.go`, `internal/QP/ast.go`
- **Dependencies**: Wave 1 complete

**Wave 2 Verification**: `go test -run TestParser ./internal/QP/...`

---

## Wave 3: Engine Implementations (Parallel - 5 tasks)

### Task 3.1: Index Engine
- **Feature**: Create/drop B-Tree for indexes
- **Files**: `internal/DS/btree.go`, `internal/DS/manager.go`
- **Dependencies**: Task 2.1

### Task 3.2: Set Operations Engine
- **Feature**: Implement union/except/intersect operators
- **Files**: `internal/QE/operators.go`
- **Dependencies**: Task 2.2

### Task 3.3: CASE Engine
- **Feature**: Evaluate CASE expressions
- **Files**: `internal/QE/expr.go`
- **Dependencies**: Task 2.3

### Task 3.4: E021 String Functions Engine
- **Feature**: Implement CHAR_LENGTH, OCTET_LENGTH, SUBSTRING, etc.
- **Files**: `internal/QE/expr.go`
- **Sub-tasks**:
  - CHARACTER_LENGTH (E021-04)
  - OCTET_LENGTH (E021-05)
  - SUBSTRING (E021-06)
  - Character concatenation || (E021-07)
  - UPPER/LOWER (E021-08)
  - TRIM (E021-09)
  - POSITION (E021-11)
- **Dependencies**: Task 2.4

### Task 3.5: PRAGMA Engine
- **Feature**: Implement table_info, index_list, database_list
- **Files**: `pkg/sqlvibe/database.go`
- **Dependencies**: Task 2.1 (needs index metadata)

**Wave 3 Verification**: `go test -run "TestIndex|TestSetOps|TestCase|TestE021|TestPragma" ./...`

---

## Wave 4: Advanced Features

### Task 4.1: Index Usage in Query Planner
- **Feature**: Use indexes for WHERE clause optimization
- **Files**: `internal/QP/planner.go`, `internal/QE/engine.go`
- **Dependencies**: Task 3.1, Task 3.5
- **Sub-tasks**:
  - Index selection in planner
  - Index scan operator
  - Covering index support

### Task 4.2: Date/Time Types (F051)
- **Feature**: DATE, TIME, TIMESTAMP types and functions
- **Files**: `internal/QP/parser.go`, `internal/DS/page.go`, `internal/QE/expr.go`
- **Sub-tasks**:
  - DATE type storage
  - TIME type storage
  - TIMESTAMP type storage
  - CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP
  - LOCALTIME, LOCALTIMESTAMP
- **Dependencies**: Task 2.4 (shares parser infrastructure)

**Wave 4 Verification**: `go test -run "TestPlanner|TestDateTime" ./...`

---

## E021 Character Data Types (Detailed - 12 sections)

| Section | Feature | Parser | Engine |
|---------|---------|--------|--------|
| E021-01 | CHARACTER (CHAR) | Task 2.4 | Task 3.4 |
| E021-02 | CHARACTER VARYING (VARCHAR) | Task 2.4 | Task 3.4 |
| E021-03 | Character literals | Task 2.4 | Task 3.4 |
| E021-04 | CHARACTER_LENGTH | Task 2.4 | Task 3.4 |
| E021-05 | OCTET_LENGTH | Task 2.4 | Task 3.4 |
| E021-06 | SUBSTRING | Task 2.4 | Task 3.4 |
| E021-07 | Concatenation (\|\|) | Task 2.4 | Task 3.4 |
| E021-08 | UPPER/LOWER | Task 2.4 | Task 3.4 |
| E021-09 | TRIM | Task 2.4 | Task 3.4 |
| E021-10 | Implicit casting | Task 2.4 | Task 3.4 |
| E021-11 | POSITION | Task 2.4 | Task 3.4 |
| E021-12 | Character comparison | - | Task 3.4 |

---

## Full Parallel DAG (Detailed View)

```mermaid
graph TD
    %% Wave 1
    B1[E011-02: Float Math]:::wave1
    B2[E011-03: DECIMAL]:::wave1
    B3[E011-04: Arithmetic]:::wave1
    B4[E011-05: Comparison]:::wave1
    B5[E011-06: Casting]:::wave1
    B6[F481: COALESCE]:::wave1
    
    %% Wave 2
    P1[Index Parser]:::wave2
    P2[Set Ops Parser]:::wave2
    P3[CASE Parser]:::wave2
    P4[E021 Parser]:::wave2
    
    %% Wave 3
    E1[Index Engine]:::wave3
    E2[Set Ops Engine]:::wave3
    E3[CASE Engine]:::wave3
    E4[E021 Engine]:::wave3
    E5[PRAGMA Engine]:::wave3
    
    %% Wave 4
    A1[Index Planner]:::wave4
    A2[Date/Time]:::wave4
    
    %% Dependencies
    B1 --> P1 & P2 & P3 & P4
    B2 --> P1 & P2 & P3 & P4
    B3 --> P1 & P2 & P3 & P4
    B4 --> P1 & P2 & P3 & P4
    B5 --> P1 & P2 & P3 & P4
    B6 --> P1 & P2 & P3 & P4
    
    P1 --> E1 & E5
    P2 --> E2
    P3 --> E3
    P4 --> E4
    
    E1 --> A1
    E5 --> A1
    E4 --> A2
    
    %% Styling
    classDef wave1 fill:#ff9999,stroke:#333,stroke-width:2px
    classDef wave2 fill:#99ff99,stroke:#333,stroke-width:2px
    classDef wave3 fill:#9999ff,stroke:#333,stroke-width:2px
    classDef wave4 fill:#ffff99,stroke:#333,stroke-width:2px
```

---

## Success Criteria

### Wave 1 (Bugfixes) - CRITICAL
- [x] E011-02: Float math (Abs, Ceil, Floor, Round) returns correct values
- [ ] E011-03: DECIMAL/NUMERIC arithmetic returns correct results
- [ ] E011-04: Arithmetic operators (unary minus, large numbers) work correctly
- [ ] E011-05: Comparison operators, ORDER BY expressions work correctly
- [ ] E011-05: NULL IS NULL / IS NOT NULL returns 0/1 (not NULL)
- [ ] E011-06: Implicit numeric casting works correctly
- [ ] F481: COALESCE returns first non-NULL argument

### Wave 2 (Parsers)
- [ ] Index parser handles CREATE/DROP INDEX
- [ ] Set operations parser handles UNION/EXCEPT/INTERSECT
- [ ] CASE parser handles Simple and Searched CASE
- [ ] E021 parser handles CHAR/VARCHAR and all functions

### Wave 3 (Engines)
- [ ] Index engine creates/drops B-Tree indexes
- [ ] Set operations engine returns correct results
- [ ] CASE engine evaluates correctly
- [ ] E021 string functions return correct results
- [ ] PRAGMA returns correct metadata

### Wave 4 (Advanced)
- [ ] Query planner uses indexes for optimization
- [ ] DATE/TIME/TIMESTAMP types work correctly
- [ ] Datetime functions work correctly

### E021 Full Coverage (12 sections)
- [ ] E021-01: CHARACTER (CHAR) type
- [ ] E021-02: CHARACTER VARYING (VARCHAR) type
- [ ] E021-03: Character literals
- [ ] E021-04: CHARACTER_LENGTH function
- [ ] E021-05: OCTET_LENGTH function
- [ ] E021-06: SUBSTRING function
- [ ] E021-07: Character concatenation (||)
- [ ] E021-08: UPPER and LOWER functions
- [ ] E021-09: TRIM function
- [ ] E021-10: Implicit casting
- [ ] E021-11: POSITION function
- [ ] E021-12: Character comparison

---

## Test Commands

```bash
# Wave 1: Run all tests with race/asan
go test -race -asan ./...

# Wave 2: Parser tests
go test -run TestParser ./internal/QP/...

# Wave 3: Feature tests
go test -run "TestIndex|TestSetOps|TestCase|TestE021|TestPragma" ./...

# Wave 4: Advanced tests
go test -run "TestPlanner|TestDateTime" ./...

# Full test suite
go test -race -asan ./...
```

---

## Notes

- **Wave 1 is CRITICAL**: All 6 bugfixes must pass before moving to Wave 2
- **Wave 2-4 are INDEPENDENT within each wave**: Can parallelize work across tasks
- **E021 has internal dependencies**: Types → Literals → Casting → Functions
- **Date/Time shares parser infrastructure with E021**: Can be developed in parallel
- **Transaction management**: Deferred to future version
- **Run `go test -race -asan ./...` after each wave to verify**
