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

### DISTINCT Deduplication Fails When ORDER BY References Non-SELECT Columns

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Type** | ResultMismatch |
| **Table(s)** | order_line, stock, orders, customer, district (any multi-row table) |
| **Trigger SQL** | `SELECT DISTINCT ol_d_id FROM order_line WHERE ol_number > 0 ORDER BY ol_o_id ASC, ol_d_id ASC, ol_w_id ASC, ol_number ASC LIMIT 4` |
| **SQLite Result** | `(2 rows)[1\|2]` — correct distinct values |
| **SQLVibe Result** | `(4 rows)[1\|1\|1\|1]` (before fix) / `(1 rows)[1]` (mid-fix with wrong topK) |
| **Root Cause** | When ORDER BY references columns not in the SELECT list, those extra columns are temporarily appended to each projected row so the sort can use them. The `deduplicateRows` function used ALL row columns (including the extra sort columns) as the dedup key, so rows that shared the same SELECT value but differed in the extra ORDER BY columns were NOT deduplicated. Additionally, the `SortRowsTopK` with the LIMIT value was applied before deduplication, pruning rows that should have been seen for proper DISTINCT dedup. |
| **Fix** | (1) Skip early `deduplicateRows` when `extraOrderByCols` is non-empty. (2) Sort ALL rows without a top-K limit when DISTINCT is active. (3) After sort, apply `deduplicateRowsN(results, numSelectCols)` which uses only the first N projected columns as the dedup key. (4) Apply LIMIT after dedup. Added `deduplicateRowsN` helper to `pkg/sqlvibe/vm_exec.go`. Both code paths (`execSelectStmtWithContext` and `execVMQuery`) were fixed. |
| **Seed** | 1, 2, 7, 42 |
| **Found By** | SQLValidator |
| **Date** | 2026-02-25 |

### Subquery Generator Missing ORDER BY (False Positive Fix)

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Type** | ResultMismatch (false positive — not an engine bug) |
| **Table(s)** | Various (IN/EXISTS subquery targets) |
| **Trigger SQL** | `SELECT s_i_id FROM stock WHERE EXISTS (SELECT 1 FROM stock WHERE s_i_id = stock.s_i_id) LIMIT 10` |
| **SQLite Result** | `(10 rows)[1\|1\|2\|2\|3\|3\|4\|4\|5\|5]` (PK-ordered scan) |
| **SQLVibe Result** | `(10 rows)[1\|2\|3\|4\|5\|6\|7\|8\|1\|2]` (insertion-order scan) |
| **Root Cause** | The `genSubquery` generator emitted `LIMIT 10` without an `ORDER BY` clause. Since SQL does not define scan order without ORDER BY, SQLite (PK-ordered) and sqlvibe (insertion-ordered) produced different but equally-valid row subsets. The `compare.go` normalizer sorts rows before comparing, but both databases returned different valid 10-row subsets from a 20-row result, so the sorted multisets differed. This was a validator false-positive, not an engine bug. |
| **Fix** | Added `g.pkOrderBy(tm1, "ASC")` before `LIMIT 10` in both the EXISTS and IN branches of `genSubquery` in `internal/TS/SQLValidator/generator.go`. This forces a deterministic ORDER BY so both databases return the same rows. |
| **Seed** | 1, 2 |
| **Found By** | SQLValidator |
| **Date** | 2026-02-25 |

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

