# Plan v0.8.7 - Views, Maintenance & Builtin Functions

## Summary

Enhance sqlvibe with view support, database maintenance commands, additional PRAGMAs, and missing builtin functions for better SQLite compatibility.

**Previous**: v0.8.6 delivers FOREIGN KEY, TRIGGER, AUTOINCREMENT, DateTime, String functions

**v0.8.7 Scope**: ~65 hours total
- VIEW + INSTEAD OF: 20h
- VACUUM: 12h
- ANALYZE: 10h
- Additional PRAGMAs: 8h
- Builtin Functions: 15h

---

## Problem Statement

Current sqlvibe lacks:
- VIEW support (read-only and updatable views)
- Database maintenance (VACUUM, ANALYZE)
- Many PRAGMA commands
- Several builtin functions

Goals:
- Support complex queries via views
- Enable database maintenance
- Complete PRAGMA coverage
- Match SQLite function coverage

---

## Phase 1: VIEW Support (20h)

### Overview

Views are virtual tables based on SQL queries:

```sql
-- Create view
CREATE VIEW active_users AS
SELECT id, name, email FROM users WHERE active = 1;

-- Query view like table
SELECT * FROM active_users WHERE name LIKE 'A%';

-- Drop view
DROP VIEW active_users;
```

### Implementation

```go
// internal/IS/view.go
type ViewInfo struct {
    Name       string
    SQL        string // Original CREATE VIEW statement
    SelectStmt *QP.SelectStmt // Parsed AST
    Columns    []string // Column names
}

// internal/QP/parser.go
// Add to parseCreate():
case "VIEW":
    return p.parseCreateView()

type CreateViewStmt struct {
    Name       string
    Select     *SelectStmt
    Columns    []string // Explicit column names (optional)
}

// Rewrite view queries:
// SELECT * FROM view_name -> 
// SELECT * FROM (SELECT ... FROM underlying_tables)
```

### INSTEAD OF Trigger on Views

```sql
-- Create view with INSTEAD OF trigger
CREATE VIEW orders_view AS
SELECT id, customer_id, total FROM orders;

CREATE TRIGGER orders_view_insert
INSTEAD OF INSERT ON orders_view
BEGIN
    INSERT INTO orders (id, customer_id, total) VALUES (NEW.id, NEW.customer_id, NEW.total);
END;

-- Insert into view
INSERT INTO orders_view VALUES (1, 100, 50.00);
```

### Tasks

- [x] Add ViewInfo struct to schema
- [x] Parse CREATE VIEW syntax
- [x] Store views in schema
- [x] Add OpOpenView opcode
- [x] Implement view query rewriting
- [x] Handle view column expansion
- [x] DROP VIEW support
- [ ] Add INSTEAD OF trigger support (already in v0.8.6)
- [ ] Implement updatable view with INSTEAD OF
- [x] Tests: view creation, query, INSTEAD OF trigger

**Workload:** ~20 hours

---

## Phase 2: VACUUM (12h)

### Overview

VACUUM rebuilds the database file to reclaim space:

```sql
-- Full vacuum
VACUUM;

-- Vacuum into file
VACUUM INTO 'backup.db';

-- Incremental vacuum (SQLite3)
VACUUM;
```

### Implementation

```go
// internal/DS/vacuum.go
type Vacuum struct {
    db       *Database
    destPath string
}

func (v *Vacuum) Run() error {
    // 1. Create new database file
    // 2. Export all data via SQL
    // 3. Recreate indexes
    // 4. Copy schema
    // 5. Swap files
}

func (v *Vacuum) VacuumInto(path string) error {
    // 1. Create new database at path
    // 2. Copy all data
    // 3. Close and return
}

// Command: VACUUM [INTO filename]
```

### Tasks

- [x] Add VACUUM SQL command parser
- [x] Implement VACUUM - rebuild in-place
- [x] Implement VACUUM INTO - rebuild to new file
- [x] Preserve PRAGMA settings
- [ ] Handle large databases (streaming)
- [x] Tests: VACUUM correctness

**Workload:** ~12 hours

---

## Phase 3: ANALYZE (10h)

### Overview

ANALYZE collects statistics for query optimization:

```sql
-- Analyze all tables
ANALYZE;

-- Analyze specific table
ANALYZE table_name;

-- Analyze specific index
ANALYZE index_name;
```

### Implementation

```go
// internal/IS/analyze.go
type TableStats struct {
    TableName   string
    RowCount    int64
    IndexStats  map[string]*IndexStats
}

type IndexStats struct {
    IndexName  string
    DistinctKeys int64
    AvgDepth   float64
    Pages      int
}

// Store in sqlite_stat1 table
// sqlite_stat1: tbl, idx, stat (JSON-like string)

// Use statistics in query planning:
// - Choose index with better selectivity
// - Estimate JOIN row counts
```

### Tasks

- [x] Add ANALYZE SQL command parser
- [x] Implement table statistics collection
- [x] Implement index statistics collection
- [x] Create sqlite_stat1 system table
- [ ] Use statistics in query planner
- [ ] PRAGMA analyze_info
- [x] Tests: ANALYZE correctness

**Workload:** ~10 hours

---

## Phase 4: Additional PRAGMAs (8h)

### Overview

Essential PRAGMAs for SQLite compatibility:

```sql
-- Page size and cache
PRAGMA page_size = 4096;
PRAGMA cache_size = -2000;  -- 2MB negative = KB
PRAGMA mmap_size = 268435456; -- 256MB

-- Locking
PRAGMA locking_mode = NORMAL | EXCLUSIVE;
PRAGMA busy_timeout = 5000;

-- Journal mode
PRAGMA journal_mode = DELETE | WAL | MEMORY | OFF;

-- Synchronous
PRAGMA synchronous = 0 | 1 | 2 | NORMAL | FULL | EXTRA;

-- Auto vacuum
PRAGMA auto_vacuum = NONE | INCREMENTAL | FULL;

-- Query only
PRAGMA query_only = ON | OFF;

-- Temp store
PRAGMA temp_store = DEFAULT | FILE | MEMORY;

-- Read uncommitted
PRAGMA read_uncommitted = 0 | 1;

-- Cache spill
PRAGMA cache_spill = 1 | 0;
```

### Tasks

- [x] PRAGMA page_size (read/set)
- [x] PRAGMA cache_size
- [x] PRAGMA mmap_size
- [x] PRAGMA locking_mode
- [x] PRAGMA busy_timeout
- [x] PRAGMA journal_mode
- [x] PRAGMA synchronous
- [x] PRAGMA auto_vacuum
- [x] PRAGMA query_only
- [x] PRAGMA temp_store
- [x] PRAGMA read_uncommitted
- [x] PRAGMA cache_spill
- [x] Tests: PRAGMA behavior

**Workload:** ~8 hours

---

## Phase 5: Additional Builtin Functions (15h)

### Overview

Missing SQLite builtin functions:

```sql
-- Binary functions
SELECT HEX(256);         -- '100'
SELECT UNHEX('414243');  -- 'ABC'
SELECT LENGTH('ABC');    -- 3 (already)
SELECT LENGTH(X'414243'); -- 3 (blob)

-- Random
SELECT RANDOMBLOB(16);   -- 16 random bytes
SELECT RANDOM();         -- -9223372036854775808 to 9223372036854775807
SELECT ABS(RANDOM());    -- positive random

-- Unicode
SELECT UNICODE('A');     -- 65
SELECT CHAR(65,66,67);  -- 'ABC'

-- Type
SELECT TYPEOF(NULL + 1);     -- 'integer'
SELECT TYPEOF('1' + '1');    -- 'integer' (affinity)
SELECT TYPEOF(CAST(1 AS BLOB)); -- 'blob'

-- Quote/Escape
SELECT QUOTE("it's");    -- 'it''s'
SELECT ESCAPE('a%b', '\'); -- 'a\%b' (for LIKE)

-- Null handling (already have COALESCE, IFNULL, NULLIF)
SELECT II1>F(0, 'yes', 'no'); -- 'yes'

-- Aggregate extras
SELECT GROUP_CONCAT(name, ',') FROM users;
```

### Implementation

```go
// internal/VM/func_binary.go
var BinaryFunctions = map[string]func(args ...interface{}) (interface{}, error){
    "hex": hexFunc,
    "unhex": unhexFunc,
    "unicode": unicodeFunc,
    "char": charFunc,
    "random": randomFunc,
    "randomblob": randomblobFunc,
    "zeroblob": zeroblobFunc,
    "typeof": typeofFunc,
    "quote": quoteFunc,
    "escape": escapeFunc,
    "iif": iifFunc,
}
```

### Tasks

- [ ] HEX() - integer/blob to hex string
- [x] UNHEX() - hex string to blob
- [x] UNICODE() - character to code point
- [x] CHAR() - code points to string
- [x] RANDOM() - random integer
- [x] RANDOMBLOB() - random blob
- [x] ZEROBLOB() - zero-filled blob
- [ ] TYPEOF() - return type name
- [ ] QUOTE() - escape string
- [ ] ESCAPE() - escape LIKE pattern
- [x] IIF() - inline if
- [x] Tests: function comparisons with SQLite

**Workload:** ~15 hours

---

## Success Criteria

### Phase 1: VIEW

| Criteria | Target | Status |
|----------|--------|--------|
| CREATE VIEW | Works | [x] |
| Query view | Works | [x] |
| DROP VIEW | Works | [x] |
| View with columns | Works | [x] |
| INSTEOD OF INSERT | Works | [ ] |
| INSTEAD OF UPDATE | Works | [ ] |
| INSTEAD OF DELETE | Works | [ ] |
| SQLite comparison | Match | [x] |

### Phase 2: VACUUM

| Criteria | Target | Status |
|----------|--------|--------|
| VACUUM (in-place) | Works | [x] |
| VACUUM INTO | Works | [x] |
| Preserve schema | Works | [x] |
| Preserve data | Works | [x] |
| SQLite comparison | Match | [x] |

### Phase 3: ANALYZE

| Criteria | Target | Status |
|----------|--------|--------|
| ANALYZE (all) | Works | [x] |
| ANALYZE table | Works | [x] |
| sqlite_stat1 table | Works | [x] |
| Use stats in planner | Works | [ ] |
| SQLite comparison | Match | [x] |

### Phase 4: PRAGMAs

| Criteria | Target | Status |
|----------|--------|--------|
| PRAGMA page_size | Works | [x] |
| PRAGMA cache_size | Works | [x] |
| PRAGMA locking_mode | Works | [x] |
| PRAGMA busy_timeout | Works | [x] |
| PRAGMA journal_mode | Works | [x] |
| PRAGMA synchronous | Works | [x] |
| PRAGMA auto_vacuum | Works | [x] |
| PRAGMA query_only | Works | [x] |
| PRAGMA temp_store | Works | [x] |

### Phase 5: Builtin Functions

| Criteria | Target | Status |
|----------|--------|--------|
| HEX() | Works | [ ] |
| UNHEX() | Works | [x] |
| RANDOM() | Works | [x] |
| RANDOMBLOB() | Works | [x] |
| ZEROBLOB() | Works | [x] |
| UNICODE() | Works | [x] |
| CHAR() | Works | [x] |
| TYPEOF() | Works | [ ] |
| IIF() | Works | [x] |
| SQLite comparison | Match | [x] |

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | VIEW + INSTEAD OF | 20 |
| 2 | VACUUM | 12 |
| 3 | ANALYZE | 10 |
| 4 | Additional PRAGMAs | 8 |
| 5 | Builtin Functions | 15 |

**Total**: ~65 hours

---

## Benchmark Commands

```bash
# View tests
go test ./internal/TS/SQLLogic/... -v -run "View"

# VACUUM tests
go test ./internal/TS/SQLLogic/... -v -run "Vacuum"

# ANALYZE tests
go test ./internal/TS/SQLLogic/... -v -run "Analyze"

# PRAGMA tests
go test ./internal/TS/SQLLogic/... -v -run "Pragma"

# Function tests
go test ./internal/TS/SQLLogic/... -v -run "Func"
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| VIEW query rewriting complexity | High | Test extensively |
| VACUUM memory usage | Medium | Stream large tables |
| ANALYZE performance | Medium | Background option |
| Function edge cases | Low | Test with SQLite |

---

## Notes

- VIEW is complex - test with various SELECT statements
- VACUUM requires careful file handling
- ANALYZE statistics help query planner but not critical for correctness
- IIF() is new in SQLite 3.32
