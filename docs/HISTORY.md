# sqlvibe Release History

## **v0.7.0** (2026-02-21)

### Summary
Performance optimization release implementing CG optimizer, page cache, WAL mode, and expanded SQL1999 test coverage.

### Features
- **CG Optimizer**:
  - Constant folding: Pre-compute arithmetic/concat operations at compile time
  - Dead code elimination: Remove unreachable instructions after OpHalt/OpGoto

- **Page Cache (LRU)**:
  - Full LRU cache implementation with hit/miss tracking
  - SQLite-compatible: Negative values = KiB, positive = page count
  - Thread-safe with sync.Mutex

- **WAL Mode**:
  - WAL header and frame format (WALHeader, WALFrame)
  - Write-ahead logging: WriteFrame(), Commit(), Checkpoint()
  - Salt values for frame identification

- **Remove QE Subsystem**:
  - Completely removed redundant QE layer
  - Architecture simplified to: QP → CG → VM → DS

- **SQL1999 Test Suite Expansion**:
  - Expanded from 56 to 64+ test suites
  - 224 → 340+ test cases

### Implementation Scores
| Component | Score |
|-----------|-------|
| CG Optimizer | 85/100 |
| Page Cache (LRU) | 80/100 |
| WAL Mode | 65/100 |
| EXPLAIN QUERY PLAN | 0/100 |
| Benchmark Suite | 100/100 |
| Remove QE Subsystem | 100/100 |
| SQL1999 Tests | 95/100 |

### Testing
- All 64+ SQL1999 test suites passing
- Benchmark suite added with 25+ benchmark tests

---

## **v0.6.0** (2026-02-20)

### Summary
Defensive programming release implementing aggressive assertion validation across the codebase to catch bugs early and prevent data corruption.

### Features
- **DS Subsystem (90% complete)**:
  - btree.go: Page type validation, cell bounds, cursor state (15+ assertions)
  - page.go: Size bounds [512, 65536], power-of-2 validation
  - cell.go: Rowid positivity, buffer validation
  - encoding.go: Varint buffer sizing validation
  - overflow.go: Page chain integrity, PageManager validation
  - freelist.go: PageManager validation

- **VM Subsystem (30% complete)**:
  - cursor.go: Added MaxCursors = 256 constant
  - exec.go: Cursor ID bounds [0, 256), OpOpenRead/OpRewind/OpNext
  - compiler.go: SelectStmt validation

- **QP/QE Subsystems (20% complete)**:
  - parser.go: Token array validation
  - engine.go: PageManager, schema, table registration

- **TM Subsystem (10% complete)**:
  - transaction.go: NewTransactionManager PageManager validation

- **PB Subsystem (60% complete)**:
  - file.go: Offset bounds, buffer validation, URI checks

- **Public API (10% complete)**:
  - database.go: Row scanning bounds

### Assertion Coverage
- Overall: ~35% of critical code paths
- Core data structure validation complete, preventing most B-Tree and page corruption bugs

### Testing
All existing tests pass with current assertions:
- internal/DS/... - All tests passing
- internal/VM/... - All tests passing
- internal/QP/... - All tests passing
- internal/QE/... - All tests passing
- internal/TM/... - All tests passing
- internal/PB/... - All tests passing

---

## **v0.5.2** (2026-02-18)

### Summary
Bug fix release addressing LIKE, GLOB, and SUBSTR issues from v0.5.1.

### Bug Fixes
- **LIKE**: Rewrote pattern matching algorithm, fixed % and _ wildcards
- **LIKE**: Added NOT LIKE support (TokenNotLike)
- **GLOB**: Added OpGlob and globMatch function (case-sensitive)
- **SUBSTR**: Fixed start=0 edge case
- **Numeric comparison**: Added toFloat64 helper for consistent int64/float64 comparison

### Known Issues (Deferred)
- DECIMAL/NUMERIC type ordering (requires DS layer type affinity fix)

---

## **v0.5.1** (2026-02-18)

### Summary
Bug fix release addressing critical issues from v0.5.0.

### Bug Fixes
- **DS Encoding**: Fixed serial type mapping (removed Int24, SQLite doesn't use it)
- **ORDER BY**: Fixed expression evaluation using EvalExpr for non-column references
- **IN/NOT IN**: Fixed NULL propagation in OpBitOr/OpBitAnd operators
- **BETWEEN**: Fixed NULL handling same as IN operators
- **TRIM**: Fixed default characters when P2=0 (now means space)
- **SUBSTR**: Fixed length parameter handling and negative/zero edge cases

### Known Issues (Remaining)
- LIKE/GLOB pattern matching edge cases
- DECIMAL/NUMERIC type handling
- SUBSTR(str, 0, n) edge case

---

## **v0.5.0** (2026-02-18)

### Summary
Major architectural release delivering three core infrastructure components: CG (Code Generator) subsystem, VFS (Virtual File System) architecture, and complete BTree implementation with SQLite-compatible encoding.

### Features
- **CG Subsystem**: Extracted compiler from VM into dedicated Code Generator package for clean separation of concerns (AST → bytecode → execution)
- **VFS Architecture**: Implemented pluggable storage abstraction layer with Unix VFS and Memory VFS implementations
- **Complete BTree**: Full SQLite-compatible BTree encoding (~2500 lines) including:
  - Varint & record encoding
  - Cell formats for all 4 page types (table/index leaf/interior)
  - Overflow page management
  - Page balancing algorithms
  - Freelist management
- **WHERE Operators**: Added OR, AND, IN, BETWEEN, LIKE, IS NULL operators

### Known Issues (Not Fixed in This Release)
- DS encoding tests: int32/int64 serial type mapping incorrect
- ORDER BY expression/ABS handling bugs
- IN/BETWEEN operator bugs
- Varchar TRIM and SUBSTR string operation issues
- LIKE operator 1 edge case (case sensitivity)

### Bug Fixes
- Cell boundary detection: Fixed payload size overflow in BTree
- WHERE operators: 13/14 tests passing (93%)

---

## **v0.4.5** (2026-02-16)

### Summary
Final verification release. Test failures reduced from 72 to 36 (50% improvement).

### Known Issues (Not Fixed)
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these SQL-standard functions
- OCTET_LENGTH: SQLite doesn't support this SQL-standard function
- POSITION: SQLite doesn't support this SQL-standard function
- Unicode case folding: Go and SQLite handle Unicode case conversion differently
- MinInt64 display: -9223372036854775808 displays as float64 (pre-existing)
- ABS on multiple columns: Pre-existing engine issue

### Fixed in Previous Versions
- v0.4.1: NOT IN, NOT BETWEEN, LIKE, GLOB, NULL handling
- v0.4.2: LENGTH (Unicode), INSTR, TRIM, SUBSTR
- v0.4.3: CAST expression
- v0.4.4: ROUND negative precision

---

## **v0.4.4** (2026-02-16)

### Bug Fixes
- ROUND: Fixed handling of negative precision (ROUND(x, -n))

### Known Issues
- ABS on columns: Pre-existing engine issue with multiple column evaluation
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these functions
- OCTET_LENGTH: SQLite doesn't support this function

---

## **v0.4.3** (2026-02-16)

### Bug Fixes
- CAST expression: Implemented CAST(expr AS type) syntax
- Support for CAST to INTEGER, REAL, TEXT, BLOB types
- Most CAST tests now pass (E02110)

### Known Issues
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these functions
- OCTET_LENGTH: SQLite doesn't support this function  
- Unicode case folding differs between Go and SQLite (UPPER/LOWER)

---

## **v0.4.2** (2026-02-16)

### Bug Fixes
- LENGTH: Fixed to count Unicode characters (runes) instead of bytes
- INSTR: Fixed argument order (haystack, needle) and use rune-based indexing
- TRIM/LTRIM/RTRIM: Added support for two-argument form TRIM(str, chars)
- SUBSTR/SUBSTRING: Fixed negative start index, zero start, and Unicode support

### Tests
- E02104: LENGTH_Unicode, LENGTH_Chinese, LENGTH_Emoji now pass
- E02106: Many SUBSTR tests now pass
- E02109: TRIM_Special, LTRIM_Special, RTRIM_Special now pass

### Known Issues
- CHAR_LENGTH, CHARACTER_LENGTH: SQLite doesn't support these functions
- OCTET_LENGTH: SQLite doesn't support this function
- POSITION: SQLite doesn't support this function
- TRIM tabs/newlines: Test data encoding differs between sqlvibe and SQLite

---

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
