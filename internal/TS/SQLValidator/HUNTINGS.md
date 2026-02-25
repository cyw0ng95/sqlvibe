# SQLValidator Hunting Log

A record of correctness discrepancies discovered through sqlvibe's SQLValidator — a
differential testing tool that compares sqlvibe query results against SQLite using
LCG-generated SQL on the TPC-C schema.

---

## How to Add an Entry

When SQLValidator finds a mismatch:

1. Add an entry to this file following the format below.
2. Add a regression test in `internal/TS/Regression/` (or in `validator_test.go`
   as a `TestSQLValidator_Regression` subcase).
3. Fix the root cause in the engine.
4. Do **NOT** add to `docs/HISTORY.md` — this file is the source of truth for
   SQLValidator bugs.

### Entry Format

```markdown
### Bug Title

| Attribute | Value |
|-----------|-------|
| **Severity** | High / Medium / Low |
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

<!-- New entries go above this line, newest first -->

### ORDER BY before UNION/UNION ALL

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Type** | ErrorMismatch |
| **Table(s)** | Various (UNION queries) |
| **Trigger SQL** | SELECT ... ORDER BY ... UNION SELECT ... |
| **SQLite Result** | error("SQL logic error: ORDER BY clause should come after UNION not before") |
| **SQLVibe Result** | Incorrectly accepted and returned results |
| **Root Cause** | Parser accepted ORDER BY before UNION without returning an error |
| **Fix** | Added check in parser.go after ORDER BY parsing to reject UNION/EXCEPT/INTERSECT |
| **Seed** | 1 |
| **Found By** | SQLValidator |
| **Date** | 2026-02-25 |

### Subquery Column Reference Generator Bug

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Type** | ResultMismatch |
| **Table(s)** | Various (EXISTS subqueries) |
| **Trigger SQL** | SELECT ... WHERE EXISTS (SELECT ... WHERE table_name = ...) |
| **SQLite Result** | error("no such column") |
| **SQLVibe Result** | Empty results (incorrect) |
| **Root Cause** | SQL generator was using table name instead of column name in EXISTS subquery |
| **Fix** | Fixed generator to use actual column names instead of table names |
| **Seed** | 1 |
| **Found By** | SQLValidator |
| **Date** | 2026-02-25 |

### Correlated Subquery Column Resolution

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Type** | ResultMismatch |
| **Table(s)** | Various (correlated subqueries) |
| **Trigger SQL** | SELECT ... WHERE EXISTS (SELECT ... WHERE unqualified_col = ...) |
| **SQLite Result** | Correct rows with correlated data |
| **SQLVibe Result** | Empty results or wrong results |
| **Root Cause** | VM couldn't resolve unqualified column references to outer query in correlated subqueries |
| **Fix** | Modified compiler to emit column reference even when not found in inner table; Modified VM to check outer context when column not found in inner table |
| **Seed** | 1 |
| **Found By** | SQLValidator |
| **Date** | 2026-02-25 |

### IN Subquery Generator Bug

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Type** | ResultMismatch |
| **Table(s)** | Various (IN subqueries) |
| **Trigger SQL** | SELECT ... WHERE col IN (SELECT wrong_col FROM ...) |
| **SQLite Result** | Rows based on correct column |
| **SQLVibe Result** | Wrong rows (using non-existent column in subquery) |
| **Root Cause** | SQL generator was selecting column from outer table instead of inner table in IN subquery |
| **Fix** | Fixed generator to select column from the correct (inner) table |
| **Seed** | 1 |
| **Found By** | SQLValidator |
| **Date** | 2026-02-25 |

## Running the Validator

```bash
# Run the validator test suite (1000 statements, seed 42)
go test -v -run=TestSQLValidator ./internal/TS/SQLValidator/...

# Run with a specific seed for reproduction
go test -v -run=TestSQLValidator_Regression ./internal/TS/SQLValidator/...
```
