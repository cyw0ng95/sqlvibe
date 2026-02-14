# SQL:1999 Test Plan for sqlvibe

## How to Use This Test Plan

This document lists all SQL:1999 chapters as TODO items. To check a chapter:

1. **Run the test** for that chapter
2. **Compare with SQLite** using the verification script
3. **Fix any bugs** found
4. **Mark as DONE** in this document

---

## TODO: Chapter Test Status

### 📘 Part I: Data Types

- [ ] **Ch3 - Numbers**: INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE PRECISION
- [ ] **Ch4 - Bit Strings**: BIT, BIT VARYING
- [ ] **Ch5 - Binary Strings**: BINARY, VARBINARY, BLOB
- [ ] **Ch6 - Characters**: CHARACTER, CHAR
- [ ] **Ch7 - Character Strings**: CHAR, VARCHAR, TEXT, CLOB
- [x] **Ch8 - Temporal Values**: DATE, TIME, TIMESTAMP, INTERVAL ✅ DONE
- [ ] **Ch9 - Boolean Values**: BOOLEAN, TRUE, FALSE, UNKNOWN
- [ ] **Ch10 - Collection Types**: ARRAY, MULTISET
- [ ] **Ch11 - Row Types**: ROW, row constructors
- [ ] **Ch12 - Reference Types**: REF, dereference operations

### 📗 Part II: Schema Objects

- [ ] **Ch17 - SQL Schema**: CREATE SCHEMA, DROP SCHEMA
- [x] **Ch18 - Tables & Views**: CREATE TABLE, DROP TABLE, CREATE VIEW, DROP VIEW ✅ DONE
- [ ] **Ch19 - Domains**: CREATE DOMAIN, DROP DOMAIN
- [x] **Ch20 - Constraints**: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL ✅ DONE
- [ ] **Ch21 - Character Set**: CREATE CHARACTER SET
- [ ] **Ch22 - Collation**: CREATE COLLATION
- [ ] **Ch23 - Translation**: CREATE TRANSLATION
- [ ] **Ch24 - Triggers**: CREATE TRIGGER, BEFORE/AFTER, FOR EACH ROW
- [ ] **Ch25-26 - SQL-invoked Routine**: CALL, stored procedures, functions
- [ ] **Ch27 - User-defined Types**: CREATE TYPE, UDT

### 📙 Part III: Query Operations

- [x] **Ch28 - Intro to SQL-data**: SELECT fundamentals ✅ DONE
- [x] **Ch29 - Search Conditions**: WHERE, =, <>, <, >, <=, >=, AND, OR, NOT, LIKE, IN, BETWEEN, IS NULL ✅ DONE
- [ ] **Ch30 - Joins**: INNER JOIN, LEFT/RIGHT/FULL OUTER JOIN, CROSS JOIN, NATURAL JOIN
- [ ] **Ch31 - Subqueries**: SCALAR, TABLE, EXISTS, IN, ALL, ANY
- [ ] **Ch32 - Set Operators**: UNION, INTERSECT, EXCEPT, ALL/DISTINCT
- [x] **Ch33 - Groups**: GROUP BY, HAVING, aggregate functions ✅ DONE
- [x] **Ch34 - Sorting**: ORDER BY, ASC/DESC, NULLS FIRST/LAST ✅ DONE

### 📕 Part IV: Data Modification

- [x] **Ch35 - Changing SQL-data**: INSERT, UPDATE, DELETE, MERGE ✅ DONE

### 📒 Part V: Transactions

- [x] **Ch36 - Transactions**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, auto-commit ✅ DONE
- [ ] **Ch37 - Concurrency**: Lock levels, isolation levels

### 📓 Part VI: Sessions & Security

- [ ] **Ch15 - Authorization**: GRANT, REVOKE, roles
- [ ] **Ch38 - Sessions**: SET, session variables

### 📔 Part VII: Advanced Features

- [ ] **Ch39 - Embedded SQL**: SQL in host languages
- [ ] **Ch40-51 - SQL/CLI**: ODBC/JDBC-style API
- [ ] **Ch52-53 - Module/Style**: Module language

---

## Test Commands by Chapter

### To Check a Chapter (e.g., Ch3):

```bash
# Run tests for specific chapter
cd /workspace && go test -v -run TestCh3 ./pkg/sqlvibe/

# Or run all compat tests and check output
cd /workspace && go test -v ./pkg/sqlvibe/ 2>&1 | grep -i "ch3\|numbers\|integer"
```

### Verification Script:

```bash
#!/bin/bash
# compare_sqlvibe.sh <chapter>

CHAPTER=$1
SQLVIBE_DB="/tmp/test_sqlvibe_$CHAPTER.db"
SQLITE_DB="/tmp/test_sqlite_$CHAPTER.db"

rm -f $SQLVIBE_DB $SQLITE_DB

# Run tests on both databases and compare
echo "Testing Chapter $CHAPTER..."
```

---

## Current Test Files

| Test File | Coverage |
|-----------|----------|
| `pkg/sqlvibe/compat_test.go` | Ch3, Ch6, Ch7, Ch18, Ch20, Ch28-36 |
| `pkg/sqlvibe/benchmark_test.go` | Performance benchmarks |

---

## Running All Tests

```bash
# Run all compatibility tests
cd /workspace && go test -v ./pkg/sqlvibe/

# Run benchmarks
cd /workspace && go test -bench=. -benchtime=1s ./pkg/sqlvibe/

# Run specific test
cd /workspace && go test -v -run TestDMLInsert ./pkg/sqlvibe/
```

---

## Progress Summary

| Status | Count |
|--------|-------|
| ✅ DONE | 11 |
| ❌ NOT STARTED | 39 |
| **Total Chapters** | **50** |

---

## References

- SQL:1999 (ISO/IEC 9075:1999)
- SQL-99 Complete, Really: https://sql-99.readthedocs.io/
- SQLite Documentation: https://www.sqlite.org/lang.html
- Go SQLite (glebarez/go-sqlite): https://github.com/glebarez/go-sqlite
