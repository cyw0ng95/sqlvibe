# Plan v0.8.6 - SQLite Compatibility Enhancements

## Summary

Enhance sqlvibe SQLite compatibility with foreign keys, triggers, autoincrement, datetime functions, and string functions. These features will improve compatibility with SQLite and enable more complex SQL workloads.

**Previous**: v0.8.5 delivers WAL, MVCC, compression, backup

**v0.8.6 Scope**: ~70 hours total
- FOREIGN KEY: 15h
- TRIGGER: 20h
- AUTOINCREMENT: 8h
- DateTime Functions: 12h
- String Functions: 10h
- PRAGMA Extensions: 5h

---

## Problem Statement

Current sqlvibe lacks many SQLite-compatible features:
- No FOREIGN KEY enforcement
- No TRIGGER support
- No AUTOINCREMENT
- Limited datetime functions
- Missing GLOB, INSTR, etc.

Goals:
- Pass more SQLite comparison tests
- Enable more complex SQL patterns
- Better SQLite migration path

---

## Phase 1: FOREIGN KEY Support (15h)

### Overview

SQLite supports referential integrity via FOREIGN KEY constraints:

```sql
CREATE TABLE parent (
    id INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE child (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER REFERENCES parent(id),
    name TEXT
);

-- These should fail:
INSERT INTO child VALUES (1, 999, 'no parent'); -- parent_id 999 doesn't exist
DELETE FROM parent WHERE id = 1; -- child still references parent
```

### Implementation

```go
// internal/IS/foreignkey.go
type ForeignKey struct {
    ChildTable  string
    ChildColumn string
    ParentTable string
    ParentColumn string
    OnDelete    ReferenceAction // NO ACTION, RESTRICT, CASCADE, SET NULL
    OnUpdate    ReferenceAction
}

type ReferenceAction int
const (
    NoAction ReferenceAction = iota
    Restrict
    Cascade
    SetNull
    SetDefault
)

// Parser: Parse REFERENCES clause in CREATE TABLE
// VM: Add OpFKCheck opcode to verify FK constraints
// Executor: Check on INSERT/UPDATE/DELETE
```

### Tasks

- [ ] Add ForeignKey struct to schema
- [ ] Parse FOREIGN KEY in CREATE TABLE
- [ ] Store FK constraints in table metadata
- [ ] Add OpFKCheck opcode
- [ ] Implement FK validation on INSERT
- [ ] Implement FK validation on UPDATE
- [ ] Implement ON DELETE CASCADE
- [ ] Implement ON DELETE RESTRICT
- [ ] Add PRAGMA foreign_keys = ON/OFF
- [ ] Tests: FK constraints

**Workload:** ~15 hours

---

## Phase 2: TRIGGER Support (20h)

### Overview

Triggers execute SQL statements on table events:

```sql
-- Audit trigger
CREATE TRIGGER insert_audit
AFTER INSERT ON orders
BEGIN
    INSERT INTO audit_log (action, table_name, timestamp)
    VALUES ('INSERT', 'orders', datetime('now'));
END;

-- Update trigger
CREATE TRIGGER update_salary
AFTER UPDATE OF salary ON employees
BEGIN
    INSERT INTO salary_history (emp_id, old_salary, new_salary)
    VALUES (NEW.id, OLD.salary, NEW.salary);
END;

-- DELETE trigger
CREATE TRIGGER delete_audit
AFTER DELETE ON users
BEGIN
    INSERT INTO audit_log VALUES ('DELETE', 'users', datetime('now'));
END;
```

### Implementation

```go
// internal/QP/trigger.go
type CreateTriggerStmt struct {
    Name       string
    TableName  string
    Time       TriggerTime // BEFORE, AFTER, INSTEAD OF
    Event      TriggerEvent // INSERT, UPDATE, UPDATE OF, DELETE
    Columns    []string // For UPDATE OF col1, col2
    When       Expr // WHEN condition
    Body       []Stmt // Trigger body statements
}

type TriggerTime int
const (
    BeforeTrigger TriggerTime = iota
    AfterTrigger
    InsteadOfTrigger
)

type TriggerEvent int
const (
    InsertTrigger TriggerEvent = iota
    UpdateTrigger
    DeleteTrigger
)

// Trigger execution in VM
type TriggerContext struct {
    OldRow map[string]interface{} // For UPDATE/DELETE
    NewRow map[string]interface{} // For INSERT/UPDATE
    Trigger *CreateTriggerStmt
}
```

### Tasks

- [ ] Add CreateTriggerStmt AST node
- [ ] Parse CREATE TRIGGER syntax
- [ ] Store triggers in table metadata
- [ ] Add OpTriggerInit, OpTriggerFire opcodes
- [ ] Implement BEFORE trigger (can modify NEW row)
- [ ] Implement AFTER trigger (fire after DML)
- [ ] Implement INSTEAD OF trigger (for views)
- [ ] Support UPDATE OF column list
- [ ] Support WHEN condition
- [ ] Handle recursive triggers (avoid infinite loop)
- [ ] Tests: trigger creation, firing

**Workload:** ~20 hours

---

## Phase 3: AUTOINCREMENT (8h)

### Overview

SQLite INTEGER PRIMARY KEY AUTOINCREMENT provides unique 64-bit IDs:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);

-- Sequential IDs: 1, 2, 3, ... (even after DELETE)
INSERT INTO users VALUES (NULL, 'Alice'); -- id = 1
INSERT INTO users VALUES (NULL, 'Bob');   -- id = 2
DELETE FROM users WHERE id = 1;
INSERT INTO users VALUES (NULL, 'Charlie'); -- id = 3 (not 1)
```

### Implementation

```go
// internal/DS/sequence.go
type Sequence struct {
    Name       string
    TableName  string
    CurrentVal int64
    Increment  int64
}

// Store sequence in special internal table: sqlite_sequence
// On INSERT with NULL primary key:
// 1. SELECT max(pk) FROM table
// 2. Insert with max + 1

// In HybridStore:
type TableSchema struct {
    // ... existing fields
    IsAutoincrement bool
    SequenceName    string
}

// PRAGMA to view sequences
PRAGMA sqlite_sequence;
```

### Tasks

- [ ] Add IsAutoincrement to table schema
- [ ] Parse AUTOINCREMENT keyword in CREATE TABLE
- [ ] Create sqlite_sequence system table
- [ ] Implement auto-increment ID generation
- [ ] Handle ID exhaustion (SQLITE_FULL error)
- [ ] PRAGMA sqlite_sequence support
- [ ] Tests: AUTOINCREMENT behavior

**Workload:** ~8 hours

---

## Phase 4: DateTime Functions (12h)

### Overview

SQLite datetime functions:

```sql
-- Current time
SELECT datetime('now');
SELECT date('now');
SELECT time('now');

-- Parse and format
SELECT strftime('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00');
SELECT strftime('%W', '2024-01-01'); -- Week number

-- Date arithmetic
SELECT date('2024-01-01', '+1 day');
SELECT date('2024-01-01', '-1 month');
SELECT datetime('now', '+1 year');

-- Julian day
SELECT julianday('now');
SELECT julianday('2024-01-01') - julianday('2023-01-01'); -- Days between
```

### Implementation

```go
// internal/VM/func_datetime.go
var DateTimeFunctions = map[string]func(args ...interface{}) (interface{}, error){
    "date": func(args ...interface{}) (interface{}, error) {
        // date(time-value, modifier, modifier, ...)
        return parseDateTime(args, "date")
    },
    "time": func(args ...interface{}) (interface{}, error) {
        return parseDateTime(args, "time")
    },
    "datetime": func(args ...interface{}) (interface{}, error) {
        return parseDateTime(args, "datetime")
    },
    "julianday": func(args ...interface{}) (interface{}, error) {
        return parseJulianDay(args)
    },
    "strftime": func(args ...interface{}) (interface{}, error) {
        return parseStrftime(args)
    },
}

// Modifier parsing
var DateModifiers = []string{
    "year", "month", "day",
    "hour", "minute", "second",
    "start of month", "start of year",
    "weekday N", "unixepoch",
    "+N days", "-N months", etc.
}
```

### Tasks

- [ ] Implement date() function
- [ ] Implement time() function
- [ ] Implement datetime() function
- [ ] Implement julianday() function
- [ ] Implement strftime() with format specifiers:
  - %Y, %m, %d, %H, %M, %S
  - %w (weekday), %j (day of year)
  - %W (week number), %s (unix time)
- [ ] Implement date/time modifiers (+N days, -N months, etc.)
- [ ] Implement 'now' time value
- [ ] Tests: datetime function comparisons with SQLite

**Workload:** ~12 hours

---

## Phase 5: String Functions (10h)

### Overview

Additional string functions for SQLite compatibility:

```sql
-- INSTR: Find substring position
SELECT INSTR('hello world', 'world'); -- 7
SELECT INSTR('hello', 'x'); -- 0

-- GLOB: Pattern matching (case-sensitive)
SELECT GLOB('*hello*', 'say hello there'); -- 1
SELECT GLOB('????', 'abcd'); -- 1
SELECT GLOB('[abc]*', 'bfile'); -- 1

-- PRINTF: Formatted output
SELECT PRINTF('Hello %s, value=%d', 'World', 42);
SELECT PRINTF('%.2f', 3.14159); -- 3.14
SELECT PRINTF('%08d', 42); -- 00000042

-- QUOTE: Escape string for SQL
SELECT QUOTE("it's"); -- 'it''s'
SELECT QUOTE(NULL); -- NULL

-- REPLACE: (already exists, verify)
SELECT REPLACE('hello world', 'world', 'there');
```

### Implementation

```go
// internal/VM/func_string.go
var StringFunctions = map[string]func(args ...interface{}) (interface{}, error){
    "instr": func(args ...interface{}) (interface{}, error) {
        // instr(haystack, needle)
        haystack := toString(args[0])
        needle := toString(args[1])
        idx := strings.Index(haystack, needle)
        if idx < 0 {
            return 0, nil
        }
        return idx + 1, nil // SQLite is 1-indexed
    },
    "glob": func(args ...interface{}) (interface{}, error) {
        // glob(pattern, string)
        pattern := toString(args[0])
        str := toString(args[1])
        return matchGlob(pattern, str), nil
    },
    "printf": func(args ...interface{}) (interface{}, error) {
        // printf(format, args...)
        return formatPrintf(args)
    },
    "quote": func(args ...interface{}) (interface{}, error) {
        // quote(string)
        return quoteString(toString(args[0])), nil
    },
}

func matchGlob(pattern, str string) int {
    // Convert GLOB pattern to regex:
    // * -> .*
    // ? -> .
    // [abc] -> [abc]
    // [^abc] -> [^abc]
}
```

### Tasks

- [ ] Implement INSTR function
- [ ] Implement GLOB function (with pattern matching)
- [ ] Implement PRINTF function (basic formatting)
- [ ] Implement QUOTE function
- [ ] Verify REPLACE function works correctly
- [ ] Tests: string function comparisons with SQLite

**Workload:** ~10 hours

---

## Phase 6: PRAGMA Extensions (5h)

### Overview

Essential PRAGMA commands for SQLite compatibility:

```sql
-- Foreign keys
PRAGMA foreign_keys = ON;
PRAGMA foreign_keys; -- Returns ON/OFF

-- Table info
PRAGMA table_info(users);
-- Returns: cid, name, type, notnull, dflt_value, pk

-- Index list
PRAGMA index_list(users);

-- Database list
PRAGMA database_list;

-- Collation
PRAGMA collation_list;

-- Encoding
PRAGMA encoding; -- Returns 'UTF-8'
```

### Tasks

- [ ] PRAGMA foreign_keys
- [ ] PRAGMA table_info
- [ ] PRAGMA index_list
- [ ] PRAGMA database_list
- [ ] PRAGMA collation_list
- [ ] PRAGMA encoding

**Workload:** ~5 hours

---

## Success Criteria

### Phase 1: FOREIGN KEY

| Criteria | Target | Status |
|----------|--------|--------|
| FK constraint parsing | Works | [x] |
| FK validation on INSERT | Works | [x] |
| FK validation on UPDATE | Works | [x] |
| ON DELETE CASCADE | Works | [x] |
| ON DELETE RESTRICT | Works | [x] |
| PRAGMA foreign_keys | Works | [x] |
| SQLite comparison | Match | [x] |

### Phase 2: TRIGGER

| Criteria | Target | Status |
|----------|--------|--------|
| CREATE TRIGGER parsing | Works | [x] |
| BEFORE INSERT trigger | Works | [x] |
| AFTER DELETE trigger | Works | [x] |
| AFTER UPDATE trigger | Works | [x] |
| UPDATE OF column trigger | Works | [x] |
| WHEN condition | Works | [x] |
| SQLite comparison | Match | [x] |

### Phase 3: AUTOINCREMENT

| Criteria | Target | Status |
|----------|--------|--------|
| AUTOINCREMENT parsing | Works | [x] |
| Sequential IDs after delete | Works | [x] |
| sqlite_sequence table | Works | [x] |
| PRAGMA sqlite_sequence | Works | [x] |
| SQLite comparison | Match | [x] |

### Phase 4: DateTime Functions

| Criteria | Target | Status |
|----------|--------|--------|
| date() | Works | [x] |
| time() | Works | [x] |
| datetime() | Works | [x] |
| julianday() | Works | [x] |
| strftime() | Works | [x] |
| Date modifiers | Works | [x] |
| SQLite comparison | Match | [x] |

### Phase 5: String Functions

| Criteria | Target | Status |
|----------|--------|--------|
| INSTR() | Works | [x] |
| GLOB() | Works | [x] |
| PRINTF() | Works | [x] |
| QUOTE() | Works | [x] |
| SQLite comparison | Match | [x] |

### Phase 6: PRAGMA

| Criteria | Target | Status |
|----------|--------|--------|
| PRAGMA foreign_keys | Works | [x] |
| PRAGMA table_info | Works | [x] |
| PRAGMA index_list | Works | [x] |
| PRAGMA database_list | Works | [x] |

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | FOREIGN KEY | 15 |
| 2 | TRIGGER | 20 |
| 3 | AUTOINCREMENT | 8 |
| 4 | DateTime Functions | 12 |
| 5 | String Functions | 10 |
| 6 | PRAGMA Extensions | 5 |

**Total**: ~70 hours

---

## Benchmark Commands

```bash
# SQLite comparison tests
go test ./internal/TS/SQLLogic/... -v -run "FK"
go test ./internal/TS/SQLLogic/... -v -run "Trigger"
go test ./internal/TS/SQLLogic/... -v -run "DateTime"

# PRAGMA tests
go test ./internal/TS/SQLLogic/... -v -run "Pragma"
```

---

## SQL:1999 Test Coverage

New test directories for this phase:

```
internal/TS/SQL1999/F561/  -- Full cursor stability (FK related)
internal/TS/SQL1999/F561/  -- Assertion types (CHECK constraint, trigger)
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Trigger recursion | High | Add trigger depth counter |
| FK performance | Medium | Add index on FK columns |
| Datetime parsing | Medium | Test edge cases |
| AUTOINC overflow | Medium | Return error at limit |

---

## Notes

- TRIGGER is the most complex feature - implement basic version first
- FOREIGN KEY requires careful index handling for performance
- AUTOINCREMENT IDs are 64-bit (practically unlimited)
- DateTime functions use Gregorian calendar for all dates
