# Plan v0.9.15 - Final Stability, API Hardening & Release Candidate

## Summary

This is the release-candidate milestone before v1.0.0. The focus is on production
hardening: eliminating remaining panics, stabilising the public API surface, running an
extended fuzzing campaign, and verifying performance is at or better than the v0.9.6
baseline. No new SQL features are added in this release — correctness, stability, and
API clarity are the sole objectives.

---

## Background

At this point the database supports:
- Full parameterized queries and `database/sql` driver (v0.9.11–12)
- `context.Context` cancellation and query timeouts (v0.9.13)
- `ALTER TABLE DROP/RENAME COLUMN`, `FETCH FIRST`, `INTERSECT ALL`, CSV/JSON I/O (v0.9.14)
- 149+ SQL:1999 test suites passing
- WAL + crash recovery, MVCC, FK enforcement, triggers, savepoints, window functions

What remains before v1.0.0:
1. No known panics on any SQL input (fuzzer-verified)
2. Stable, documented public API (no internal symbols in `pkg/sqlvibe`)
3. Correct error codes for all failure modes
4. Passing all existing test suites without regression
5. Performance at least matching the v0.9.6 numbers (60 µs SELECT, 9.4× vs SQLite)

---

## Track A: Extended Fuzzing Campaign

### A1. PlainFuzzer — 10-Minute Run

Run `FuzzSQL` for at least 10 minutes, triage every crash, fix every panic:

```bash
go test -fuzz=FuzzSQL -fuzztime=10m ./internal/TS/PlainFuzzer/...
```

**Bug fix protocol**: for every panic found:
1. Add a minimal reproduction to `internal/TS/PlainFuzzer/testdata/corpus/`
2. Fix the root cause
3. Add a regression test in `internal/TS/Regression/regression_v0.9.15_test.go`
4. Record in `internal/TS/PlainFuzzer/HUNTINGS.md`

### A2. FuzzDBFile — 10-Minute Run

```bash
go test -fuzz=FuzzDBFile -fuzztime=10m ./internal/TS/PlainFuzzer/...
```

Same protocol as A1 for any panics found during file-level fuzzing.

### A3. Zero-Panic Guarantee

After the fuzzing campaign, execute all corpus entries in CI to confirm no panics:

```bash
go test -run=FuzzSQL/. ./internal/TS/PlainFuzzer/...
go test -run=FuzzDBFile/. ./internal/TS/PlainFuzzer/...
```

Add these invocations to the Makefile as `make fuzz-regression`.

---

## Track B: Public API Stabilisation

### B1. API Audit

Review `pkg/sqlvibe/database.go` and identify:
- Methods that should be unexported (helpers leaked as public)
- Methods with inconsistent naming (e.g. `MustExec` vs `Exec`)
- Methods that duplicate functionality (e.g. `ExecWithParams` vs `ExecContextWithParams`)

Document the **stable public API** in `pkg/sqlvibe/API.md`:

```
Open(path) (*Database, error)
Database.Exec(sql) (Result, error)
Database.ExecContext(ctx, sql) (Result, error)
Database.ExecWithParams(sql, params) (Result, error)
Database.ExecContextWithParams(ctx, sql, params) (Result, error)
Database.ExecNamed(sql, params) (Result, error)
Database.MustExec(sql, params...) Result
Database.Query(sql) (*Rows, error)
Database.QueryContext(ctx, sql) (*Rows, error)
Database.QueryWithParams(sql, params) (*Rows, error)
Database.QueryContextWithParams(ctx, sql, params) (*Rows, error)
Database.QueryNamed(sql, params) (*Rows, error)
Database.Prepare(sql) (*Statement, error)
Database.Begin() (*Transaction, error)
Database.Close() error
Database.ClearResultCache()
Database.ImportCSV(table, r, opts) (int, error)
Database.ExportCSV(w, sql, opts) error
Database.ExportJSON(w, sql) error
```

### B2. Deprecation of Stubs

Deprecated stubs that remain for backward compatibility get a `//Deprecated:` doc comment
directing callers to the replacement. No stubs are removed — they stay until v1.1.0.

### B3. `pkg/sqlvibe` Export Leaks

Use `go vet` and `staticcheck` to confirm no unexported type is reachable through
an exported interface.

---

## Track C: Error Code Completeness

### C1. Error Code Audit

Every `fmt.Errorf` in `pkg/sqlvibe/` that is returned to callers should instead use
`NewError(code, ...)` with a specific `ErrorCode`. Audit and convert the top 20
most-common error paths:

| Error Path | Current | Target |
|------------|---------|--------|
| Table not found | raw fmt.Errorf | `SVDB_TABLE_NOT_FOUND` |
| Column not found | raw fmt.Errorf | `SVDB_COLUMN_NOT_FOUND` |
| Constraint violation | raw fmt.Errorf | `SVDB_CONSTRAINT_VIOLATION` |
| Syntax error | raw fmt.Errorf | `SVDB_SYNTAX_ERROR` |
| No active transaction | raw fmt.Errorf | `SVDB_NO_TRANSACTION` |
| Duplicate savepoint | raw fmt.Errorf | `SVDB_DUPLICATE_SAVEPOINT` |
| Parse error | raw fmt.Errorf | `SVDB_PARSE_ERROR` |
| Type mismatch | raw fmt.Errorf | `SVDB_TYPE_MISMATCH` |
| Index already exists | raw fmt.Errorf | `SVDB_INDEX_EXISTS` |
| View not found | raw fmt.Errorf | `SVDB_VIEW_NOT_FOUND` |

### C2. `IsErrorCode` Helper Usage Docs

Add examples in `pkg/sqlvibe/error.go` showing how callers use `IsErrorCode`:

```go
rows, err := db.Query("SELECT * FROM missing_table")
if sqlvibe.IsErrorCode(err, sqlvibe.SVDB_TABLE_NOT_FOUND) {
    // handle gracefully
}
```

---

## Track D: Performance Verification

### D1. Baseline Benchmark Run

Run the full benchmark suite and confirm no regression vs v0.9.6:

```bash
go test ./internal/TS/Benchmark/... -bench=BenchmarkFair_ -benchtime=2s
```

Target (AMD EPYC 7763):
- SELECT all (1K rows): ≤ 70 µs (baseline 60 µs + 15% tolerance)
- SUM aggregate: ≤ 25 µs (baseline 19 µs)
- GROUP BY: ≤ 160 µs (baseline 135 µs)
- Result cache hit: ≤ 2 µs (baseline 1.5 µs)

### D2. New Benchmark: Parameterized Query

Add `BenchmarkFair_PreparedStmt` to `internal/TS/Benchmark/benchmark_v0.9.15_test.go`:

```
Prepare once, then Query 1000× with different ? params.
Measures: param binding overhead on top of pure query execution.
```

### D3. New Benchmark: database/sql Driver Overhead

```
Measure overhead of the driver layer relative to direct API:
  direct: db.Query("SELECT ...") → rows
  driver: sql.DB.Query("SELECT ...") → rows
Target: driver overhead < 5 µs per query.
```

---

## Track E: Final Test Suite Pass

### E1. Run All SQL:1999 Suites

```bash
go test ./internal/TS/SQL1999/... -timeout 120s
```

All 149+ suites must pass. Any new failures introduced by v0.9.11–14 changes must be
fixed before tagging v0.9.15.

### E2. F886 SQL1999 Suite — Release Candidate Smoke Test

Add `internal/TS/SQL1999/F886/01_test.go` as an end-to-end integration test covering
all major features added in v0.9.11–v0.9.14:

- Parameterized `INSERT` + `SELECT`
- `database/sql` driver round-trip
- `ExecContext` with timeout
- `ALTER TABLE DROP COLUMN` + subsequent query
- `ALTER TABLE RENAME COLUMN` + subsequent query
- CSV import → query → CSV export round-trip
- Full transaction: BEGIN → INSERT → SAVEPOINT → rollback to savepoint → COMMIT

### E3. Regression Suite v0.9.15

Add `internal/TS/Regression/regression_v0.9.15_test.go` with all panic reproductions
found during the fuzzing campaign.

---

## Track F: Release Preparation

### F1. Version Bump

```
pkg/sqlvibe/version.go:
  const Version = "v0.9.15"
```

### F2. HISTORY.md

Add v0.9.15 entry documenting:
- Bugs fixed via fuzzing
- API stabilisation changes
- Performance numbers from Track D benchmarks

### F3. README Update

- Update "Stable Releases" table to include v0.9.11–v0.9.15
- Update performance section with v0.9.15 benchmark numbers
- Add "database/sql Driver" section with usage example
- Add "Parameterized Queries" section with `?` and `:name` examples
- Add "Query Timeout" section with `context.WithTimeout` example
- Add "CSV / JSON Import-Export" section

### F4. Tag

```bash
git tag -a v0.9.15 -m "Release v0.9.15: Release Candidate"
git push origin v0.9.15
```

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `internal/TS/PlainFuzzer/testdata/corpus/` | Add any new panic-reproducing corpus entries |
| `internal/TS/PlainFuzzer/HUNTINGS.md` | Document all bugs found in fuzzing campaign |
| `internal/TS/Regression/regression_v0.9.15_test.go` | **NEW** — panic reproductions from fuzzing |
| `internal/TS/SQL1999/F886/01_test.go` | **NEW** — RC integration smoke test |
| `internal/TS/Benchmark/benchmark_v0.9.15_test.go` | **NEW** — prepared stmt + driver overhead benchmarks |
| `pkg/sqlvibe/API.md` | **NEW** — stable public API surface documentation |
| `pkg/sqlvibe/version.go` | Bump to `v0.9.15` |
| `pkg/sqlvibe/error_code.go` | Add missing error codes from audit |
| `pkg/sqlvibe/database.go` | Convert raw `fmt.Errorf` calls to typed errors |
| `Makefile` | Add `fuzz-regression` target |
| `docs/HISTORY.md` | Add v0.9.15 entry |
| `README.md` | Update stable releases, perf numbers, new feature sections |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| Zero panics after 10-min FuzzSQL | Yes | [ ] |
| Zero panics after 10-min FuzzDBFile | Yes | [ ] |
| All 149+ SQL:1999 suites pass | 100% | [ ] |
| F886 RC smoke test passes | 100% | [ ] |
| SELECT all ≤ 70 µs (no regression) | Yes | [ ] |
| Stable public API documented in API.md | Yes | [ ] |
| Top 10 error paths use typed error codes | Yes | [ ] |
| Version bumped to v0.9.15 | Yes | [ ] |
| README updated with v0.9.15 numbers | Yes | [ ] |
| v0.9.15 tag created | Yes | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F886 suite | RC integration smoke test (7+ tests) | [ ] |
| Regression v0.9.15 | Fuzzing-campaign panic reproductions | [ ] |
| BenchmarkFair v0.9.15 | Performance non-regression | [ ] |
| Full SQL:1999 run | All 149+ suites | [ ] |
