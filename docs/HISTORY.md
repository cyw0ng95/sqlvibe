# sqlvibe Release History

## **v0.4.1** (2026-02-16)

### Bug Fixes
- NOT IN operator: Implemented in parser and engine
- NOT BETWEEN operator: Implemented in parser and engine
- LIKE operator: Fixed in SELECT expressions (added to evalValue)
- NOT LIKE operator: Implemented in parser and engine
- GLOB operator: Implemented with pattern matching
- NULL arithmetic: Fixed add, sub, mul, div, mod, concat to return NULL for NULL operands
- NULL comparisons: Fixed 3-valued logic for comparisons with NULL
- AND/OR operators: Fixed in SELECT expressions

### Tests
- E01105: All IN/BETWEEN/NULL comparison tests now pass
- E02112: All LIKE/GLOB/BETWEEN/IN tests now pass

---

## **v0.4.0** (2026-02-16)

### Features
- Index support: CREATE INDEX, DROP INDEX, B-Tree operations
- Set operations: UNION, EXCEPT, INTERSECT
- CASE expressions: Simple and Searched CASE
- Full E021 character data types support
  - CHAR, CHARACTER types
  - VARCHAR, TEXT types
  - Character functions: UPPER, LOWER, LENGTH, SUBSTRING, TRIM, INSTR
  - String concatenation (|| operator)
  - Implicit type casting
- Date/Time types: DATE, TIME, TIMESTAMP
- Date/Time functions: CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, STRFTIME, YEAR, MONTH, DAY
- Query planner optimizations using indexes
- PRAGMA statements: table_info, index_list, database_list
- PlainFuzzer: Go native fuzzing framework for SQL testing

### Known Issues
- Some edge cases in INSTR/POSITION functions may differ from SQLite
- BETWEEN with character types not fully implemented

### Fixed Bugs
- Float math functions (ABS, CEIL, FLOOR, ROUND) now return correct values
- DECIMAL/NUMERIC arithmetic operations fixed
- Unary minus on column references works correctly
- NULL IS NULL / IS NOT NULL returns 0/1 (not NULL)
- Implicit numeric casting between INTEGER/REAL/DECIMAL
- COALESCE returns first non-NULL argument correctly
- PlainFuzzer database reuse issue fixed (commit e51554d)

### Tests
- E011: Comprehensive numeric type tests (~290 test cases)
- E021: Complete character data types tests (251 test cases across 12 sections)
- PlainFuzzer: SQL fuzzing with mutation strategies

---

## **v0.3.0** (2026-02-15)

### Features
- JOIN support (INNER, LEFT, CROSS)
- sqlite_master table
- Subqueries (Scalar, EXISTS, IN, ALL/ANY, Correlated)
- :memory: database support
- TS Test Suites subsystem

### Known Issues
- ABS, CEIL, FLOOR, ROUND functions not implemented
- DECIMAL/NUMERIC type handling incomplete
- IS NULL/IS NOT NULL returns nil instead of 0/1

### Fixed Bugs
- Column ordering in SELECT * queries (commit 316b157)
- Constant expression evaluation (SELECT 10 + 5.0) (commit 316b157)
- Comparison operators return 0/1 instead of nil (commit 316b157)
- Integer division follows SQLite behavior (commit 316b157)

### Tests
- E011-01 through E011-06 numeric type tests added

---

## **v0.2.0** (2026-02-15)

### Features
- WHERE enhancements: AND, OR, NOT evaluation
- IS NULL / IS NOT NULL
- IN operator
- BETWEEN
- LIKE pattern matching

### Known Issues
- COALESCE function not implemented
- IFNULL function not implemented

### Fixed Bugs
- None

### Tests
- 21 passing (+8 from v0.1.0)

---

## **v0.1.0** (2026-02-15)

### Features
- Basic DML: INSERT, UPDATE, DELETE
- Basic Queries: SELECT, WHERE (simple), ORDER BY, LIMIT
- Aggregates: COUNT, SUM, AVG, MIN, MAX
- Transactions: BEGIN, COMMIT, ROLLBACK
- Prepared Statements

### Known Issues
- None

### Fixed Bugs
- None (initial release)

### Tests
- 13 passing (~47 subtests)
