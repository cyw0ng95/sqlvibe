# sqlvibe Release History

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
