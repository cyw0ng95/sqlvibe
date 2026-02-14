# SQL:1999 Test Plan for sqlvibe

## TODO: Implementation Status by Chapter

### ✅ IMPLEMENTED / TESTED

| Chapter | Topic | Status | Notes |
|---------|-------|--------|-------|
| Ch3 | Numbers | ✅ DONE | INTEGER, REAL, TEXT in compat_test.go |
| Ch6 | Characters | ✅ DONE | CHAR, VARCHAR, TEXT |
| Ch7 | Character Strings | ✅ DONE | TEXT type |
| Ch18 | Tables & Views | ✅ DONE | CREATE TABLE, DROP TABLE |
| Ch20 | Constraints | ✅ DONE | PRIMARY KEY, NOT NULL |
| Ch28 | SELECT fundamentals | ✅ DONE | Basic SELECT |
| Ch29 | Search Conditions | ✅ DONE | WHERE, =, <>, >, <, LIKE, IN, BETWEEN |
| Ch33 | Groups | ✅ DONE | GROUP BY, HAVING, COUNT, SUM, AVG, MIN, MAX |
| Ch34 | Sorting | ✅ DONE | ORDER BY ASC/DESC |
| Ch35 | INSERT/UPDATE/DELETE | ✅ DONE | DML operations |
| Ch36 | Transactions | ✅ DONE | BEGIN, COMMIT, ROLLBACK |

### ⚠️ PARTIALLY IMPLEMENTED

| Chapter | Topic | Status | Notes |
|---------|-------|--------|-------|
| Ch8 | Temporal Values | ⚠️ PARTIAL | DATE/TIME/TIMESTAMP parsing only |
| Ch30 | Joins | ⚠️ PARTIAL | Basic INNER JOIN, LEFT JOIN |
| Ch31 | Subqueries | ⚠️ PARTIAL | Basic subqueries |
| Ch37 | Concurrency | ⚠️ PARTIAL | Lock manager exists |

### ❌ NOT IMPLEMENTED

| Chapter | Topic | Status |
|---------|-------|--------|
| Ch4 | Bit Strings | ❌ NOT STARTED |
| Ch5 | Binary Strings | ❌ NOT STARTED |
| Ch9 | Boolean Values | ❌ NOT STARTED |
| Ch10 | Collection Types | ❌ NOT STARTED |
| Ch11 | Row Types | ❌ NOT STARTED |
| Ch12 | Reference Types | ❌ NOT STARTED |
| Ch17 | SQL Schema | ❌ NOT STARTED |
| Ch19 | Domains | ❌ NOT STARTED |
| Ch21 | Character Set | ❌ NOT STARTED |
| Ch22 | Collation | ❌ NOT STARTED |
| Ch23 | Translation | ❌ NOT STARTED |
| Ch24 | Triggers | ❌ NOT STARTED |
| Ch25-26 | SQL-invoked Routine | ❌ NOT STARTED |
| Ch27 | User-defined Types | ❌ NOT STARTED |
| Ch32 | Set Operators | ❌ NOT STARTED |
| Ch15 | Authorization | ❌ NOT STARTED |
| Ch38 | Sessions | ❌ NOT STARTED |
| Ch39 | Embedded SQL | ❌ NOT STARTED |
| Ch40-51 | SQL/CLI | ❌ NOT STARTED |
| Ch52-53 | Module/Style | ❌ NOT STARTED |

---

## Overview

This document maps SQL:1999 standard chapters to test cases for sqlvibe SQLite-compatible database engine.

**SQL:1999 Structure** (ISO/IEC 9075):
- Part 1: Framework (SQL/Framework)
- Part 2: Foundation (SQL/Foundation) - ~1050 pages
- Part 3: CLI (SQL/Call-Level Interface)
- Part 4: PSM (Persistent Stored Modules)
- Part 5: Bindings

The standard has **53 chapters** covering all aspects of SQL from data types to transactions.

---

## Chapter-by-Chapter Test Mapping

### 📘 Part I: Data Types (Chapters 3-12)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch3 | Numbers | INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE PRECISION | P0 |
| Ch4 | Bit Strings | BIT, BIT VARYING | P2 |
| Ch5 | Binary Strings | BINARY, VARBINARY, BLOB | P2 |
| Ch6 | Characters | CHARACTER, CHAR | P0 |
| Ch7 | Character Strings | CHAR, VARCHAR, TEXT, CLOB | P0 |
| Ch8 | Temporal Values | DATE, TIME, TIMESTAMP, INTERVAL | P1 |
| Ch9 | Boolean Values | BOOLEAN, TRUE, FALSE, UNKNOWN | P1 |
| Ch10 | Collection Types | ARRAY, MULTISET | P3 |
| Ch11 | Row Types | ROW, row constructors | P3 |
| Ch12 | Reference Types | REF, dereference operations | P3 |

### 📗 Part II: Schema Objects (Chapters 17-27)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch17 | SQL Schema | CREATE SCHEMA, DROP SCHEMA | P1 |
| Ch18 | Tables & Views | CREATE TABLE, DROP TABLE, CREATE VIEW, DROP VIEW | P0 |
| Ch19 | Domains | CREATE DOMAIN, DROP DOMAIN | P2 |
| Ch20 | Constraints | PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL | P0 |
| Ch21 | Character Set | CREATE CHARACTER SET | P3 |
| Ch22 | Collation | CREATE COLLATION | P3 |
| Ch23 | Translation | CREATE TRANSLATION | P3 |
| Ch24 | Triggers | CREATE TRIGGER, BEFORE/AFTER, FOR EACH ROW | P3 |
| Ch25-26 | SQL-invoked Routine | CALL, stored procedures, functions | P3 |
| Ch27 | User-defined Types | CREATE TYPE, UDT | P3 |

### 📙 Part III: Query Operations (Chapters 28-34)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch28 | Intro to SQL-data | SELECT fundamentals | P0 |
| Ch29 | Search Conditions | WHERE, =, <>, <, >, <=, >=, AND, OR, NOT, LIKE, IN, BETWEEN, IS NULL | P0 |
| Ch30 | Joins | INNER JOIN, LEFT/RIGHT/FULL OUTER JOIN, CROSS JOIN, NATURAL JOIN | P0 |
| Ch31 | Subqueries | SCALAR, TABLE, EXISTS, IN, ALL, ANY | P0 |
| Ch32 | Set Operators | UNION, INTERSECT, EXCEPT, ALL/DISTINCT | P0 |
| Ch33 | Groups | GROUP BY, HAVING, aggregate functions | P0 |
| Ch34 | Sorting | ORDER BY, ASC/DESC, NULLS FIRST/LAST | P0 |

### 📕 Part IV: Data Modification (Chapter 35)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch35 | Changing SQL-data | INSERT, UPDATE, DELETE, MERGE | P0 |

### 📒 Part V: Transactions (Chapters 36-37)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch36 | Transactions | BEGIN, COMMIT, ROLLBACK, SAVEPOINT, auto-commit | P0 |
| Ch37 | Concurrency | Lock levels, isolation levels (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE) | P1 |

### 📓 Part VI: Sessions & Security (Chapters 38, 15)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch15 | Authorization | GRANT, REVOKE, roles | P2 |
| Ch38 | Sessions | SET, session variables | P2 |

### 📔 Part VII: Advanced Features (Chapters 39-53)

| Chapter | Topic | Test Cases | Priority |
|---------|-------|------------|----------|
| Ch39 | Embedded SQL | SQL in host languages | N/A |
| Ch40-51 | SQL/CLI | ODBC/JDBC-style API | P2 |
| Ch52-53 | Module/Style | Module language | N/A |

---

## Detailed Test Case Specifications

### Priority 0 (Core - Must Pass)

#### P0.1: Data Types - Numeric
```sql
-- Test INTEGER
CREATE TABLE t1 (a INTEGER, b INT, c SMALLINT);
INSERT INTO t1 VALUES (1, 2, 3), (0, -1, 32767);

-- Test DECIMAL/NUMERIC
CREATE TABLE t2 (a DECIMAL(10,2), b NUMERIC(5,0));
INSERT INTO t2 VALUES (12345.67, 99999);

-- Test FLOAT/REAL/DOUBLE
CREATE TABLE t3 (a FLOAT, b REAL, c DOUBLE PRECISION);
```

#### P0.2: Data Types - Strings
```sql
-- Test CHAR/VARCHAR
CREATE TABLE t4 (a CHAR(10), b VARCHAR(100));
INSERT INTO t4 VALUES ('hello', 'world');

-- Test TEXT (SQLite extension)
CREATE TABLE t5 (a TEXT);
INSERT INTO t5 VALUES ('long text content');
```

#### P0.3: DDL - Tables
```sql
-- CREATE TABLE basic
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
INSERT INTO users VALUES (1, 'Alice', 30);

-- CREATE TABLE with constraints
CREATE TABLE employees (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  salary DECIMAL(10,2) DEFAULT 0,
  dept_id INTEGER REFERENCES departments(id),
  CHECK (salary >= 0)
);

-- DROP TABLE
DROP TABLE IF EXISTS temp_table;
```

#### P0.4: DML - INSERT/UPDATE/DELETE
```sql
-- INSERT basic
INSERT INTO users (name, age) VALUES ('Bob', 25);

-- INSERT multiple rows
INSERT INTO users VALUES (3, 'Charlie', 35), (4, 'Diana', 28);

-- INSERT with subquery
INSERT INTO backup_users SELECT * FROM users;

-- UPDATE with WHERE
UPDATE users SET age = age + 1 WHERE id = 1;

-- DELETE with WHERE
DELETE FROM users WHERE age < 18;
```

#### P0.5: Query - Basic SELECT
```sql
-- SELECT all columns
SELECT * FROM users;

-- SELECT specific columns
SELECT name, age FROM users;

-- SELECT with expressions
SELECT name, age * 12 as age_months FROM users;

-- SELECT with DISTINCT
SELECT DISTINCT age FROM users;
```

#### P0.6: Query - WHERE Clause
```sql
-- Equality
SELECT * FROM users WHERE name = 'Alice';

-- Comparison operators
SELECT * FROM users WHERE age > 25;
SELECT * FROM users WHERE age >= 30;
SELECT * FROM users WHERE age < 30;
SELECT * FROM users WHERE age <= 25;
SELECT * FROM users WHERE age <> 30;

-- LIKE
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE name LIKE '_lice';

-- IN
SELECT * FROM users WHERE name IN ('Alice', 'Bob');

-- BETWEEN
SELECT * FROM users WHERE age BETWEEN 20 AND 30;

-- IS NULL / IS NOT NULL
SELECT * FROM users WHERE email IS NULL;
```

#### P0.7: Query - Joins
```sql
-- CROSS JOIN
SELECT * FROM users CROSS JOIN departments;

-- INNER JOIN
SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id;

-- LEFT OUTER JOIN
SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id;

-- RIGHT OUTER JOIN (if supported)
SELECT u.name, d.name FROM users u RIGHT JOIN departments d ON u.dept_id = d.id;

-- FULL OUTER JOIN (if supported)
SELECT u.name, d.name FROM users u FULL OUTER JOIN departments d ON u.dept_id = d.id;

-- NATURAL JOIN
SELECT * FROM users NATURAL JOIN departments;
```

#### P0.8: Query - Subqueries
```sql
-- Scalar subquery
SELECT name, (SELECT MAX(age) FROM users) as max_age FROM users;

-- EXISTS
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM departments WHERE id = 1);

-- IN subquery
SELECT * FROM users WHERE dept_id IN (SELECT id FROM departments);

-- ALL/ANY
SELECT * FROM users WHERE age > ALL (SELECT age FROM users WHERE dept_id = 1);
```

#### P0.9: Query - Set Operators
```sql
-- UNION
SELECT name FROM employees UNION SELECT name FROM contractors;

-- UNION ALL
SELECT name FROM employees UNION ALL SELECT name FROM contractors;

-- INTERSECT
SELECT name FROM employees INTERSECT SELECT name FROM contractors;

-- EXCEPT
SELECT name FROM employees EXCEPT SELECT name FROM contractors;
```

#### P0.10: Query - GROUP BY & Aggregates
```sql
-- GROUP BY
SELECT dept_id, COUNT(*) as cnt FROM users GROUP BY dept_id;

-- GROUP BY with HAVING
SELECT dept_id, COUNT(*) as cnt FROM users GROUP BY dept_id HAVING COUNT(*) > 1;

-- Aggregate functions
SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users;

-- GROUP BY with multiple columns
SELECT dept_id, gender, COUNT(*) FROM users GROUP BY dept_id, gender;
```

#### P0.11: Query - ORDER BY
```sql
-- ORDER BY single column
SELECT * FROM users ORDER BY age;

-- ORDER BY multiple columns
SELECT * FROM users ORDER BY dept_id, age DESC;

-- ORDER BY with NULLS FIRST/LAST (if supported)
SELECT * FROM users ORDER BY age NULLS FIRST;
```

#### P0.12: Transactions
```sql
-- BEGIN/COMMIT
BEGIN;
INSERT INTO users VALUES (100, 'Test', 25);
COMMIT;

-- BEGIN/ROLLBACK
BEGIN;
INSERT INTO users VALUES (101, 'Test2', 30);
ROLLBACK;

-- SAVEPOINT
BEGIN;
INSERT INTO users VALUES (102, 'Test3', 35);
SAVEPOINT sp1;
INSERT INTO users VALUES (103, 'Test4', 40);
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

-- Auto-commit mode
PRAGMA auto_commit = ON;
```

### Priority 1 (Important)

#### P1.1: Temporal Types
```sql
-- DATE
CREATE TABLE events (id INTEGER, event_date DATE);
INSERT INTO events VALUES (1, '2024-01-15');

-- TIME
CREATE TABLE schedules (id INTEGER, start_time TIME);
INSERT INTO schedules VALUES (1, '09:30:00');

-- TIMESTAMP
CREATE TABLE logs (id INTEGER, log_time TIMESTAMP);
INSERT INTO logs VALUES (1, '2024-01-15 09:30:00');

-- INTERVAL (if supported)
SELECT DATE '2024-01-15' + INTERVAL '1' DAY;
```

#### P1.2: Boolean Type
```sql
-- BOOLEAN
CREATE TABLE flags (id INTEGER, active BOOLEAN);
INSERT INTO flags VALUES (1, TRUE), (2, FALSE), (3, NULL);
SELECT * FROM flags WHERE active = TRUE;
```

#### P1.3: Constraints
```sql
-- PRIMARY KEY
CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT);

-- FOREIGN KEY
CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id));

-- UNIQUE
CREATE TABLE t2 (email TEXT UNIQUE);

-- NOT NULL
CREATE TABLE t3 (name TEXT NOT NULL);

-- CHECK
CREATE TABLE t4 (age INTEGER CHECK (age >= 0));

-- Composite PRIMARY KEY
CREATE TABLE t5 (a INTEGER, b INTEGER, PRIMARY KEY (a, b));
```

#### P1.4: Isolation Levels
```sql
-- SET TRANSACTION
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- PRAGMA for SQLite compatibility
PRAGMA isolation_level;
PRAGMA read_uncommitted = 1;
```

### Priority 2 (Nice to Have)

#### P2.1: Character Sets & Collations
```sql
-- Character set (SQLite uses UTF-8 by default)
PRAGMA encoding;

-- Collation
CREATE TABLE t1 (name TEXT COLLATE NOCASE);
```

#### P2.2: ALTER TABLE
```sql
-- Add column
ALTER TABLE users ADD COLUMN email TEXT;

-- Rename table
ALTER TABLE users RENAME TO customers;

-- Drop column (if supported)
ALTER TABLE users DROP COLUMN email;
```

#### P2.3: Indexes
```sql
-- CREATE INDEX
CREATE INDEX idx_name ON users(name);
CREATE INDEX idx_age ON users(age);

-- UNIQUE INDEX
CREATE UNIQUE INDEX idx_email ON users(email);

-- DROP INDEX
DROP INDEX idx_name;
```

#### P2.4: Views
```sql
-- CREATE VIEW
CREATE VIEW adult_users AS SELECT * FROM users WHERE age >= 18;

-- DROP VIEW
DROP VIEW adult_users;

-- Updatable view (if supported)
CREATE VIEW simple_users AS SELECT id, name FROM users;
INSERT INTO simple_users VALUES (999, 'Test');
```

### Priority 3 (Advanced)

#### P3.1: Triggers
```sql
-- CREATE TRIGGER
CREATE TRIGGER update_age AFTER UPDATE ON users FOR EACH ROW
BEGIN
  INSERT INTO audit_log VALUES (OLD.id, 'update', datetime('now'));
END;

-- DROP TRIGGER
DROP TRIGGER update_age;
```

#### P3.2: Stored Procedures / Functions
```sql
-- SQLite doesn't support stored procedures, but we can document the behavior
-- This would be a compatibility test showing SQLite's limitations
```

#### P3.3: User-defined Types (UDT)
```sql
-- SQLite uses dynamic typing - no strict UDT support
-- Test showing SQLite's type affinity behavior
CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, d BLOB);
INSERT INTO t1 VALUES (1, 'text', 1.5, X'0102');
```

---

## Test Execution Matrix

| Category | Test Count | Status |
|----------|------------|--------|
| P0 - Core | ~50 | TODO |
| P1 - Important | ~20 | TODO |
| P2 - Nice to Have | ~15 | TODO |
| P3 - Advanced | ~10 | TODO |
| **TOTAL** | **~95** | |

---

## Implementation Priorities

### Phase 1: Core SQL (P0)
1. Basic DDL (CREATE/DROP TABLE)
2. Basic DML (INSERT/UPDATE/DELETE)
3. Basic Queries (SELECT, WHERE, ORDER BY)
4. Joins (INNER, LEFT)
5. Subqueries
6. Aggregates (COUNT, SUM, AVG, MIN, MAX)
7. Transactions (BEGIN/COMMIT/ROLLBACK)

### Phase 2: Enhanced SQL (P1)
1. Temporal types (DATE, TIME, TIMESTAMP)
2. More constraint types (FOREIGN KEY, CHECK)
3. Isolation levels
4. Additional operators (LIKE, IN, BETWEEN)

### Phase 3: Advanced SQL (P2-P3)
1. Indexes
2. Views
3. Triggers
4. Character sets and collations

---

## Compatibility Verification Strategy

Each test should:
1. Run the SQL on sqlvibe
2. Run the same SQL on reference SQLite (glebarez/go-sqlite)
3. Compare results
4. Log differences for analysis

Example test structure:
```go
func TestSQL1999_P0_XXX(t *testing.T) {
    testCases := []struct {
        name     string
        sql      string
        expected string // or use reference DB
    }{
        {"Integer Types", "CREATE TABLE t1 (a INTEGER)", "..."},
        // ...
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            // Test sqlvibe
            // Test reference SQLite
            // Compare
        })
    }
}
```

---

## References

- SQL:1999 (ISO/IEC 9075:1999)
- SQL-99 Complete, Really: https://sql-99.readthedocs.io/
- SQLite Documentation: https://www.sqlite.org/lang.html
- Go SQLite (glebarez/go-sqlite): https://github.com/glebarez/go-sqlite
