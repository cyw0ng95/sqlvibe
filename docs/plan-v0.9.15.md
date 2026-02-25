# Plan v0.9.15 - SQLValidator: Differential SQL Testing Framework

## Summary

This version introduces **SQLValidator**, a deterministic differential-testing tool
inside `internal/TS/SQLValidator/`. Unlike PlainFuzzer which aims to find panics and
crashes, SQLValidator focuses on **correctness**: does sqlvibe return the same result
set and the same error representation as SQLite for any legal SQL statement?

SQLValidator uses a **Linear Congruential Generator (LCG)** to produce a deterministic,
reproducible stream of random SQL statements. It then executes each statement against
both SQLite and sqlvibe, and compares the outcomes. Any discrepancy is recorded in
`internal/TS/SQLValidator/HUNTINGS.md` and a regression test is generated.

---

## Background

The PlainFuzzer approach (mutation-based, uses `testing.F`) is excellent for finding
panics. However, it does not check whether sqlvibe returns *correct* results — it only
ensures we don't crash. A differential validation tool is the logical complement:

- **PlainFuzzer**: finds panics, hangs, and assertion failures
- **SQLValidator**: finds wrong result sets, wrong error codes, wrong NULL handling,
  wrong type conversions

---

## Starter Schema: TPC-C

SQLValidator uses a simplified TPC-C schema to ground generated queries in a realistic
business domain. The schema gives the generator a meaningful set of tables, columns,
foreign keys, and data types to reference.

### TPC-C Tables (Simplified)

```sql
CREATE TABLE warehouse (
    w_id       INTEGER PRIMARY KEY,
    w_name     TEXT    NOT NULL,
    w_street_1 TEXT,
    w_street_2 TEXT,
    w_city     TEXT,
    w_state    TEXT,
    w_zip      TEXT,
    w_tax      REAL    NOT NULL,
    w_ytd      REAL    NOT NULL
);

CREATE TABLE district (
    d_id        INTEGER NOT NULL,
    d_w_id      INTEGER NOT NULL,
    d_name      TEXT    NOT NULL,
    d_street_1  TEXT,
    d_street_2  TEXT,
    d_city      TEXT,
    d_state     TEXT,
    d_zip       TEXT,
    d_tax       REAL    NOT NULL,
    d_ytd       REAL    NOT NULL,
    d_next_o_id INTEGER NOT NULL,
    PRIMARY KEY (d_id, d_w_id)
);

CREATE TABLE customer (
    c_id        INTEGER NOT NULL,
    c_d_id      INTEGER NOT NULL,
    c_w_id      INTEGER NOT NULL,
    c_first     TEXT,
    c_middle    TEXT,
    c_last      TEXT,
    c_street_1  TEXT,
    c_street_2  TEXT,
    c_city      TEXT,
    c_state     TEXT,
    c_zip       TEXT,
    c_phone     TEXT,
    c_credit    TEXT,
    c_credit_lim REAL,
    c_discount  REAL,
    c_balance   REAL    NOT NULL,
    c_ytd_payment REAL,
    c_payment_cnt INTEGER,
    c_delivery_cnt INTEGER,
    c_data      TEXT,
    PRIMARY KEY (c_id, c_d_id, c_w_id)
);

CREATE TABLE orders (
    o_id         INTEGER NOT NULL,
    o_d_id       INTEGER NOT NULL,
    o_w_id       INTEGER NOT NULL,
    o_c_id       INTEGER NOT NULL,
    o_entry_d    TEXT,
    o_carrier_id INTEGER,
    o_ol_cnt     INTEGER NOT NULL,
    o_all_local  INTEGER NOT NULL,
    PRIMARY KEY (o_id, o_d_id, o_w_id)
);

CREATE TABLE order_line (
    ol_o_id        INTEGER NOT NULL,
    ol_d_id        INTEGER NOT NULL,
    ol_w_id        INTEGER NOT NULL,
    ol_number      INTEGER NOT NULL,
    ol_i_id        INTEGER NOT NULL,
    ol_supply_w_id INTEGER NOT NULL,
    ol_delivery_d  TEXT,
    ol_quantity    INTEGER NOT NULL,
    ol_amount      REAL    NOT NULL,
    ol_dist_info   TEXT,
    PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number)
);

CREATE TABLE item (
    i_id    INTEGER PRIMARY KEY,
    i_im_id INTEGER,
    i_name  TEXT    NOT NULL,
    i_price REAL    NOT NULL,
    i_data  TEXT
);

CREATE TABLE stock (
    s_i_id       INTEGER NOT NULL,
    s_w_id       INTEGER NOT NULL,
    s_quantity   INTEGER NOT NULL,
    s_dist_01    TEXT,
    s_dist_02    TEXT,
    s_ytd        REAL,
    s_order_cnt  INTEGER,
    s_remote_cnt INTEGER,
    s_data       TEXT,
    PRIMARY KEY (s_i_id, s_w_id)
);
```

---

## Track A: LCG Random SQL Generator

### A1. LCG Engine (`internal/TS/SQLValidator/lcg.go`)

Implement a minimal LCG for reproducible random streams:

```go
type LCG struct {
    state uint64
}

// NewLCG creates a new LCG with the given seed.
func NewLCG(seed uint64) *LCG

// Next advances the state and returns the next pseudo-random uint64.
func (l *LCG) Next() uint64

// Intn returns a pseudo-random int in [0, n).
func (l *LCG) Intn(n int) int

// Float64 returns a pseudo-random float64 in [0.0, 1.0).
func (l *LCG) Float64() float64

// Choice returns a random element from the slice.
func (l *LCG) Choice(items []string) string
```

**LCG parameters** (Knuth MMIX):
- Multiplier: `6364136223846793005`
- Increment:  `1442695040888963407`
- Modulus:    `2^64` (implicit via uint64 overflow)

### A2. SQL Generator (`internal/TS/SQLValidator/generator.go`)

Generate SQL statements that reference the TPC-C schema:

**Statement types (weighted)**:

| Statement | Weight | Notes |
|-----------|--------|-------|
| `SELECT … FROM single_table` | 30% | projection, WHERE, LIMIT |
| `SELECT … FROM join` | 15% | two-table INNER/LEFT JOIN |
| `SELECT … GROUP BY … HAVING` | 10% | aggregate functions |
| `SELECT … ORDER BY … LIMIT` | 10% | sorting with LIMIT/OFFSET |
| `INSERT INTO … VALUES (…)` | 10% | single-row with random values |
| `UPDATE … SET … WHERE` | 8% | single-column update |
| `DELETE FROM … WHERE` | 7% | range delete |
| `SELECT subquery` | 5% | correlated or uncorrelated |
| `SELECT window function` | 5% | ROW_NUMBER / RANK |

**Literal value generator** (based on column type):
- `INTEGER`: random from small set `{-1, 0, 1, 2, 100, NULL}`
- `REAL`:    random from `{0.0, 1.5, 3.14, NULL}`
- `TEXT`:    random from a fixed domain (e.g. `{'Alice', 'Bob', 'warehouse-1', NULL}`)

### A3. Schema-Aware Generation

The generator knows:
- All table names and column names (from TPC-C schema)
- Column types (for correct literal generation)
- Primary key columns (to generate valid WHERE constraints)
- Foreign key relationships (to generate valid join predicates)

---

## Track B: SQLite Integration

### B1. SQLite Backend (`internal/TS/SQLValidator/sqlite_backend.go`)

Use the same SQLite driver already present in the test suites (check `go.mod` for
available sqlite packages). Wrap `database/sql`:

```go
type SQLiteBackend struct {
    db *sql.DB
}

func NewSQLiteBackend() (*SQLiteBackend, error)
func (b *SQLiteBackend) SetupSchema() error   // creates TPC-C tables + seed data
func (b *SQLiteBackend) Execute(query string) (QueryResult, error)
func (b *SQLiteBackend) Close() error
```

### B2. SQLVibe Backend (`internal/TS/SQLValidator/sqlvibe_backend.go`)

```go
type SQLVibeBackend struct {
    db *sqlvibe.Database
}

func NewSQLVibeBackend() (*SQLVibeBackend, error)
func (b *SQLVibeBackend) SetupSchema() error
func (b *SQLVibeBackend) Execute(query string) (QueryResult, error)
func (b *SQLVibeBackend) Close() error
```

### B3. Result Comparison (`internal/TS/SQLValidator/compare.go`)

```go
type QueryResult struct {
    Columns []string
    Rows    [][]interface{}
    Err     error
}

// Compare returns a Mismatch if the results differ, nil if they match.
func Compare(sqllite, svibe QueryResult) *Mismatch

type Mismatch struct {
    Query         string
    SQLiteResult  QueryResult
    SQLVibeResult QueryResult
    Reason        string
}
```

Comparison rules:
1. If both return an error, compare error *type* (schema error vs syntax error), not the
   exact message — sqlvibe error messages may differ from SQLite's.
2. If one returns error and the other does not, that is a mismatch.
3. If both return rows, compare after sorting rows (to be order-independent) and
   normalising numeric types (`int64` and `float64` with tolerance `1e-9`).
4. NULL equality: `NULL == NULL` is true for comparison purposes.

---

## Track C: Validator Driver

### C1. Main Validator (`internal/TS/SQLValidator/validator.go`)

```go
type Validator struct {
    lcg    *LCG
    gen    *Generator
    sqlite *SQLiteBackend
    svibe  *SQLVibeBackend
}

func NewValidator(seed uint64) (*Validator, error)

// Run executes n randomly-generated SQL statements and returns all mismatches found.
func (v *Validator) Run(n int) ([]Mismatch, error)
```

### C2. Validator Test (`internal/TS/SQLValidator/validator_test.go`)

```go
// TestSQLValidator_TPC_C runs the validator for 1000 statements with seed 42.
// Any mismatch is reported as a test failure with the triggering SQL.
func TestSQLValidator_TPC_C(t *testing.T)

// TestSQLValidator_Regression runs all seeds recorded in HUNTINGS.md as regression cases.
func TestSQLValidator_Regression(t *testing.T)
```

---

## Track D: Bug Tracking

### D1. HUNTINGS.md (`internal/TS/SQLValidator/HUNTINGS.md`)

All discrepancies found by SQLValidator are recorded here. See format below.

### D2. HUNTINGS.md Format

```markdown
### Bug Title

| Attribute | Value |
|-----------|-------|
| **Severity** | High/Medium/Low |
| **Type** | ResultMismatch / ErrorMismatch / NullHandling / TypeConversion |
| **Table(s)** | TPC-C table(s) involved |
| **Trigger SQL** | exact SQL that triggers the mismatch |
| **SQLite Result** | rows / error returned by SQLite |
| **SQLVibe Result** | rows / error returned by SQLVibe |
| **Root Cause** | explanation of the bug |
| **Fix** | how it was fixed |
| **Seed** | LCG seed that reproduces the mismatch |
| **Found By** | SQLValidator |
| **Date** | YYYY-MM-DD |
```

---

## Files to Create

| File | Action |
|------|--------|
| `internal/TS/SQLValidator/lcg.go` | **NEW** — LCG random number generator |
| `internal/TS/SQLValidator/generator.go` | **NEW** — SQL statement generator (TPC-C schema) |
| `internal/TS/SQLValidator/sqlite_backend.go` | **NEW** — SQLite execution backend |
| `internal/TS/SQLValidator/sqlvibe_backend.go` | **NEW** — SQLVibe execution backend |
| `internal/TS/SQLValidator/compare.go` | **NEW** — result comparison logic |
| `internal/TS/SQLValidator/validator.go` | **NEW** — main validator driver |
| `internal/TS/SQLValidator/validator_test.go` | **NEW** — test cases |
| `internal/TS/SQLValidator/HUNTINGS.md` | **NEW** — bug log |
| `docs/plan-v0.9.15.md` | This file |
| `docs/HISTORY.md` | Add v0.9.15 entry |
| `pkg/sqlvibe/version.go` | Bump to `v0.9.15` |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| LCG engine implemented and seeded | Yes | [ ] |
| TPC-C schema loaded in both backends | Yes | [ ] |
| Generator produces 8+ statement types | Yes | [ ] |
| SQLite backend executes without CGO | Yes | [ ] |
| SQLVibe backend executes all generated SQL | Yes | [ ] |
| Comparison handles NULLs, floats, ordering | Yes | [ ] |
| TestSQLValidator_TPC_C passes (1000 stmts) | Yes | [ ] |
| Any mismatches are root-caused and fixed | Yes | [ ] |
| HUNTINGS.md documents all mismatches found | Yes | [ ] |
| Version bumped to v0.9.15 | Yes | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| TestSQLValidator_TPC_C | 1000 random SQL stmts, seed 42 | [ ] |
| TestSQLValidator_Regression | Replays seeds from HUNTINGS.md | [ ] |
