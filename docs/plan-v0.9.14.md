# Plan v0.9.14 - ALTER TABLE Enhancements & SQL Compliance

## Summary

This version completes `ALTER TABLE` support (DROP COLUMN, RENAME COLUMN) and closes
the most common SQL compliance gaps found during extended fuzzing and test-suite runs.
It also adds CSV/JSON import and export utilities — lightweight helpers that are natural
for an embedded database without requiring any external dependencies or network access.

---

## Background

The current `AlterTableStmt` (parser.go line 200) only supports `ADD COLUMN` and
`RENAME TO` (table rename). SQLite 3.35+ and PostgreSQL both support `DROP COLUMN` and
`RENAME COLUMN`, and these are frequently needed in schema migrations. In addition,
several SQL compliance gaps have been identified through the expanded test suites in
v0.9.9:

- `FETCH FIRST n ROWS ONLY` (SQL:2003 row limiting)  
- `INTERSECT ALL` / `EXCEPT ALL` set operators  
- `CAST(NULL AS type)` returning typed NULL  
- Recursive CTEs with multiple base cases  
- `VALUES` as a standalone statement  

---

## Track A: ALTER TABLE Extensions

### A1. `ALTER TABLE … DROP COLUMN col`

**Parser** (`internal/QP/parser.go`):
- Add `"DROP_COLUMN"` case in `parseAlterTable`
- Set `stmt.Action = "DROP_COLUMN"`, `stmt.Column = &ColumnDef{Name: colName}`

**Executor** (`pkg/sqlvibe/database.go` — `execAlterTable`):
```
case "DROP_COLUMN":
  1. Validate column exists (return error if not)
  2. Validate column is not part of PRIMARY KEY (return error)
  3. Remove column from db.schema[tableName] column list (preserve order)
  4. Remove column from db.columnTypes, db.columnNotNull, db.columnDefaults,
     db.columnChecks
  5. Project existing rows: for each row in db.tables[tableName], delete the key
  6. Drop any index that covers only the dropped column
  7. Invalidate caches
```

**Restriction**: If the column is used in an index alongside other columns, return
`SVDB_ALTER_CONFLICT` error rather than silently corrupting the index.

### A2. `ALTER TABLE … RENAME COLUMN old TO new`

**Parser**: Add `"RENAME_COLUMN"` case, storing `stmt.Column.Name` (old) and
`stmt.NewName` (new).

**Executor**:
```
case "RENAME_COLUMN":
  1. Validate old column exists
  2. Validate new name is not already taken
  3. Rename key in: schema, columnTypes, columnNotNull, columnDefaults,
     columnChecks, columnOrder
  4. Rename key in every existing row map in db.tables[tableName]
  5. Rename column reference in every index that covers it (rebuild index key)
  6. Invalidate caches
```

### A3. `ALTER TABLE … ADD CONSTRAINT`

**Parser**: Parse `ADD CONSTRAINT name CHECK (expr)` and `ADD CONSTRAINT name UNIQUE (cols)`.

**Executor**: Register into `db.columnChecks` or `db.indexes` as appropriate.

---

## Track B: SQL Compliance Gaps

### B1. `FETCH FIRST n ROWS ONLY` / `FETCH NEXT n ROWS ONLY`

SQL:2003 row limiting syntax (equivalent to `LIMIT n`):

```
internal/QP/parser.go — parseSelectStmt:
  After ORDER BY, accept optional:
    FETCH { FIRST | NEXT } <n> { ROW | ROWS } { ONLY | WITH TIES }
  Map to existing Limit / TopK fields in SelectStmt.
  WITH TIES → set new TiesWithOrderBy bool flag.
```

### B2. `INTERSECT ALL` / `EXCEPT ALL`

Extend the existing set-operation parser (`parseSetOperation`) to accept the `ALL`
qualifier and thread it through `setops.go`:

```
pkg/sqlvibe/setops.go — execIntersect / execExcept:
  Without ALL: deduplicate (current behaviour)
  With ALL: keep duplicate rows (multiset semantics)
```

### B3. `CAST(NULL AS type)` → typed NULL

`CAST(NULL AS INTEGER)` must return `NULL`, not panic or return `0`. Verify and fix
in `internal/VM/exec.go` and `internal/VM/query_engine.go` CAST handling.

### B4. `VALUES` as Standalone Statement

`VALUES (1, 2), (3, 4)` without a preceding `SELECT` should return a result set.
Currently only handled as a derived table source. Detect at the top-level `execStmt`
dispatch and route to `execValuesTable`.

### B5. `GROUP BY` column alias resolution

`SELECT x * 2 AS v FROM t GROUP BY v` — SQLite allows grouping by a column alias
defined in the same SELECT list. Fix the column alias resolution pass in the query
executor.

---

## Track C: Import / Export Utilities

### C1. CSV Import

```go
// pkg/sqlvibe/import.go (new file)
func (db *Database) ImportCSV(tableName string, r io.Reader, opts CSVImportOptions) (int, error)

type CSVImportOptions struct {
    HasHeader     bool   // first row is column names (default: true)
    Comma         rune   // field delimiter (default: ',')
    CreateTable   bool   // CREATE TABLE if not exists (default: false)
    NullString    string // string to treat as NULL (default: "")
}
```

Reads rows from `r`, infers types (int64 → float64 → string), inserts via normal
`INSERT` path so all constraints are enforced.

### C2. CSV Export

```go
// pkg/sqlvibe/export.go (new file)
func (db *Database) ExportCSV(w io.Writer, sql string, opts CSVExportOptions) error

type CSVExportOptions struct {
    WriteHeader bool // write column names as first row (default: true)
    Comma       rune // field delimiter (default: ',')
    NullString  string // representation for NULL (default: "")
}
```

Executes `sql`, streams rows to `w` without materialising the full result set in memory.

### C3. JSON Export

```go
// pkg/sqlvibe/export.go
func (db *Database) ExportJSON(w io.Writer, sql string) error
```

Outputs a JSON array of objects: `[{"col1": val, "col2": val}, ...]`.
Uses standard `encoding/json` — no external dependencies.

---

## Track D: Testing

### D1. F885 SQL1999 Suite

Add `internal/TS/SQL1999/F885/01_test.go`:

- `ALTER TABLE … DROP COLUMN` basic case
- `ALTER TABLE … DROP COLUMN` rejects PK column with error
- `ALTER TABLE … RENAME COLUMN`
- `ALTER TABLE … ADD CONSTRAINT CHECK`
- `FETCH FIRST 3 ROWS ONLY` equivalent to `LIMIT 3`
- `INTERSECT ALL` preserves duplicates
- `EXCEPT ALL` preserves duplicates
- `CAST(NULL AS INTEGER)` returns NULL
- `VALUES (1), (2)` as standalone query
- `GROUP BY` alias resolution

### D2. Regression Suite v0.9.14

Add `internal/TS/Regression/regression_v0.9.14_test.go`:

- DROP COLUMN then INSERT succeeds (dropped column absent from row)
- RENAME COLUMN reflected in subsequent SELECT
- RENAME COLUMN updates index reference
- CSV round-trip: export then import produces identical rows
- JSON export: NULL values use `null` literal

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `internal/QP/parser.go` | Add `DROP_COLUMN`, `RENAME_COLUMN`, `ADD CONSTRAINT` cases; `FETCH FIRST/NEXT`; `INTERSECT/EXCEPT ALL` |
| `pkg/sqlvibe/database.go` | Implement `execAlterTable` DROP/RENAME COLUMN + ADD CONSTRAINT; fix `GROUP BY` alias; `VALUES` top-level dispatch |
| `pkg/sqlvibe/setops.go` | Add ALL qualifier for INTERSECT / EXCEPT |
| `internal/VM/exec.go` | Fix `CAST(NULL …)` |
| `internal/VM/query_engine.go` | Fix `CAST(NULL …)` |
| `pkg/sqlvibe/import.go` | **NEW** — CSV import |
| `pkg/sqlvibe/export.go` | **NEW** — CSV + JSON export |
| `pkg/sqlvibe/error_code.go` | Add `SVDB_ALTER_CONFLICT` |
| `internal/TS/SQL1999/F885/01_test.go` | **NEW** — feature tests |
| `internal/TS/Regression/regression_v0.9.14_test.go` | **NEW** — regressions |
| `docs/HISTORY.md` | Add v0.9.14 entry |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| `ALTER TABLE DROP COLUMN` | Yes | [ ] |
| `ALTER TABLE RENAME COLUMN` | Yes | [ ] |
| `ALTER TABLE ADD CONSTRAINT CHECK` | Yes | [ ] |
| `FETCH FIRST n ROWS ONLY` | Yes | [ ] |
| `INTERSECT ALL` / `EXCEPT ALL` | Yes | [ ] |
| `CAST(NULL AS type)` returns NULL | Yes | [ ] |
| `VALUES` as standalone statement | Yes | [ ] |
| `GROUP BY` alias resolution | Yes | [ ] |
| CSV Import / Export | Yes | [ ] |
| JSON Export | Yes | [ ] |
| F885 suite passes | 100% | [ ] |
| Regression v0.9.14 passes | 100% | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F885 suite | ALTER TABLE + SQL compliance tests (10+ tests) | [ ] |
| Regression v0.9.14 | ALTER TABLE safety + import/export regressions (5+ tests) | [ ] |
