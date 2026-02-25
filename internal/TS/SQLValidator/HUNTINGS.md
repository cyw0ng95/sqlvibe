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

## Running the Validator

```bash
# Run the validator test suite (1000 statements, seed 42)
go test -v -run=TestSQLValidator ./internal/TS/SQLValidator/...

# Run with a specific seed for reproduction
go test -v -run=TestSQLValidator_Regression ./internal/TS/SQLValidator/...
```
