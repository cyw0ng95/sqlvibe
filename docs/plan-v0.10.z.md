# Plan v0.10.z - Final Stability, API Hardening & Release Candidate

## Summary

This is the release-candidate milestone before v1.0.0. The focus is on production
hardening: eliminating remaining panics, stabilising the public API surface, running an
extended fuzzing campaign, and verifying performance is at or better than the v0.10.x
baseline. No new SQL features are added in this release — correctness, stability, and
API clarity are the sole objectives.

---

## Background

At this point the database supports:
- Full parameterized queries and `database/sql` driver (v0.9.11–12)
- `context.Context` cancellation and query timeouts (v0.9.13)
- `ALTER TABLE DROP/RENAME COLUMN`, `FETCH FIRST`, `INTERSECT ALL`, CSV/JSON I/O (v0.9.14)
- JSON Extension with table functions and aggregates (v0.9.17)
- Real Bytecode Execution Engine (v0.10.0)
- 149+ SQL:1999 test suites passing
- WAL + crash recovery, MVCC, FK enforcement, triggers, savepoints, window functions
- Extension framework with JSON and Math extensions

What remains before v1.0.0:
1. No known panics on any SQL input (fuzzer-verified)
2. Stable, documented public API (no internal symbols in `pkg/sqlvibe`)
3. Correct error codes for all failure modes
4. Passing all existing test suites without regression
5. Performance at least matching the v0.9.x baseline

---

## Track A: Extended Fuzzing Campaign

### A1. PlainFuzzer — Extended Run

Run `FuzzSQL` for an extended period, triage every crash, fix every panic:

```bash
go test -fuzz=FuzzSQL -fuzztime=30m ./internal/TS/PlainFuzzer/...
```

**Bug fix protocol**: for every panic found:
1. Add a minimal reproduction to `internal/TS/PlainFuzzer/testdata/corpus/`
2. Fix the root cause
3. Add a regression test in `internal/TS/Regression/regression_v0.10.z_test.go`
4. Record in `internal/TS/PlainFuzzer/HUNTINGS.md`

### A2. FuzzDBFile — Extended Run

```bash
go test -fuzz=FuzzDBFile -fuzztime=30m ./internal/TS/PlainFuzzer/...
```

Same protocol as A1 for any panics found during file-level fuzzing.

### A3. SQLValidator Run

Run the SQLValidator to catch any correctness mismatches:

```bash
go test -v -run=TestSQLValidator ./internal/TS/SQLValidator/... -count=5
```

### A4. Zero-Panic Guarantee

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
| Function not found | raw fmt.Errorf | `SVDB_FUNCTION_NOT_FOUND` |
| Extension not loaded | raw fmt.Errorf | `SVDB_EXTENSION_NOT_LOADED` |
| Invalid parameter | raw fmt.Errorf | `SVDB_INVALID_PARAMETER` |

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

Run the full benchmark suite and confirm no regression vs v0.9.x:

```bash
go test ./internal/TS/Benchmark/... -bench=BenchmarkFair_ -benchtime=2s
```

Target (AMD EPYC 7763):
- SELECT all (1K rows): ≤ 70 µs (baseline + 15% tolerance)
- SUM aggregate: ≤ 25 µs
- GROUP BY: ≤ 160 µs
- Result cache hit: ≤ 2 µs

### D2. New Benchmark: Parameterized Query

Add `BenchmarkFair_PreparedStmt`:

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

All suites must pass.

### E2. F887 SQL1999 Suite — Release Candidate Smoke Test

Add `internal/TS/SQL1999/F887/01_test.go` as an end-to-end integration test covering
all major features added in v0.9.11–v0.9.17:

- Parameterized `INSERT` + `SELECT`
- `database/sql` driver round-trip
- `ExecContext` with timeout
- `ALTER TABLE DROP COLUMN` + subsequent query
- `ALTER TABLE RENAME COLUMN` + subsequent query
- CSV import → query → CSV export round-trip
- JSON table functions: `json_each`, `json_tree`
- JSON aggregates: `json_group_array`, `json_group_object`
- Full transaction: BEGIN → INSERT → SAVEPOINT → rollback to savepoint → COMMIT

### E3. Regression Suite v0.10.z

Add `internal/TS/Regression/regression_v0.10.z_test.go` with all panic reproductions
found during the fuzzing campaign.

---

## Track F: Release Preparation

### F1. Version Bump

```
pkg/sqlvibe/version.go:
  const Version = "v0.10.z"
```

### F2. HISTORY.md

Add v0.10.z entry documenting:
- Bugs fixed via fuzzing
- API stabilisation changes
- Performance numbers from Track D benchmarks

### F3. README Update

- Update "Stable Releases" table to include v0.9.x
- Update performance section with v0.10.z benchmark numbers
- Add "database/sql Driver" section with usage example
- Add "Parameterized Queries" section with `?` and `:name` examples
- Add "Query Timeout" section with `context.WithTimeout` example
- Add "CSV / JSON Import-Export" section
- Add "JSON Extension" section with `json_each`, `json_tree` examples

### F4. Tag

```bash
git tag -a v0.10.z -m "Release v0.10.z: Release Candidate"
git push origin v0.10.z
```

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `internal/TS/PlainFuzzer/testdata/corpus/` | Add any new panic-reproducing corpus entries |
| `internal/TS/PlainFuzzer/HUNTINGS.md` | Document all bugs found in fuzzing campaign |
| `internal/TS/Regression/regression_v0.10.z_test.go` | **NEW** — panic reproductions from fuzzing |
| `internal/TS/SQL1999/F887/01_test.go` | **NEW** — RC integration smoke test |
| `internal/TS/Benchmark/benchmark_v0.10.z_test.go` | **NEW** — prepared stmt + driver overhead benchmarks |
| `pkg/sqlvibe/API.md` | **NEW** — stable public API surface documentation |
| `pkg/sqlvibe/version.go` | Bump to `v0.10.z` |
| `pkg/sqlvibe/error_code.go` | Add missing error codes from audit |
| `pkg/sqlvibe/database.go` | Convert raw `fmt.Errorf` calls to typed errors |
| `Makefile` | Add `fuzz-regression` target |
| `docs/HISTORY.md` | Add v0.10.z entry |
| `README.md` | Update stable releases, perf numbers, new feature sections |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| Zero panics after extended FuzzSQL | Yes | [ ] |
| Zero panics after extended FuzzDBFile | Yes | [ ] |
| All SQL:1999 suites pass | 100% | [ ] |
| F887 RC smoke test passes | 100% | [ ] |
| SELECT all ≤ 70 µs (no regression) | Yes | [ ] |
| Stable public API documented in API.md | Yes | [ ] |
| Top 10 error paths use typed error codes | Yes | [ ] |
| Version bumped to v0.10.z | Yes | [ ] |
| README updated with v0.10.z numbers | Yes | [ ] |
| v0.10.z tag created | Yes | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F887 suite | RC integration smoke test (10+ tests) | [ ] |
| Regression v0.10.z | Fuzzing-campaign panic reproductions | [ ] |
| BenchmarkFair v0.10.z | Performance non-regression | [ ] |
| Full SQL:1999 run | All 149+ suites | [ ] |
| SQLValidator | Correctness verification | [ ] |
