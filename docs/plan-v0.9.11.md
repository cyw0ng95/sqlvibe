# Plan v0.9.11 - Parameterized Queries & Prepared Statement Binding

## Summary

This version implements proper parameterized query support — the foundation of safe,
injection-free embedded database usage. Currently `ExecWithParams` and `QueryWithParams`
silently ignore the supplied parameters. This release fixes that by adding `?` positional
and `:name` / `@name` named-parameter placeholder support end-to-end: tokenizer →
parser → param binder → execution.

---

## Background

SQL injection is a critical security concern. Production-ready embedded databases must
support parameterized queries so callers never embed user-supplied values directly into
SQL strings. The existing `ExecWithParams` / `QueryWithParams` stubs (lines 1408–1414 in
`pkg/sqlvibe/database.go`) accept a `[]interface{}` slice but pass it to the underlying
`Exec` / `Query` methods unchanged, meaning the slice is entirely ignored.

---

## Track A: Tokenizer & Parser — Placeholder Tokens

### A1. New Token Types

Add `TokenPlaceholderPos` (`?`) and `TokenPlaceholderNamed` (`:name`, `@name`) to
`internal/QP/tokenizer.go`.

```
internal/QP/tokenizer.go:
- Add TokenPlaceholderPos  TokenType (after TokenCollate)
- Add TokenPlaceholderNamed TokenType
- In Tokenize(): when '?' is seen emit TokenPlaceholderPos with Literal "?"
- When ':' followed by identifier chars, emit TokenPlaceholderNamed with
  Literal = ":name"
- When '@' followed by identifier chars, emit TokenPlaceholderNamed with
  Literal = "@name"
```

### A2. Placeholder AST Node

Add a `PlaceholderExpr` AST node to `internal/QP/parser.go`:

```go
type PlaceholderExpr struct {
    Positional bool   // true for ?, false for :name / @name
    Name       string // ":foo" or "@foo"; empty for positional
    Index      int    // 0-based positional index (filled during binding)
}

func (p *PlaceholderExpr) NodeType() string { return "PlaceholderExpr" }
```

### A3. Parse Placeholder in Expressions

In `parsePrimaryExpr` (and `parsePrimaryExprWithCollate`): when the current token is
`TokenPlaceholderPos` or `TokenPlaceholderNamed` return a `PlaceholderExpr`:

```go
case TokenPlaceholderPos:
    p.advance()
    return &PlaceholderExpr{Positional: true}, nil
case TokenPlaceholderNamed:
    name := p.current().Literal
    p.advance()
    return &PlaceholderExpr{Positional: false, Name: name}, nil
```

---

## Track B: Parameter Binder

### B1. Binder Package / Function

Add `internal/QP/binder.go`:

```go
// BindParams walks an AST tree and replaces PlaceholderExpr nodes with
// concrete LiteralExpr values from params.
//
// Positional params are bound in left-to-right order; named params are
// looked up in namedParams (key without leading ':' / '@').
func BindParams(node ASTNode, params []interface{}, namedParams map[string]interface{}) (ASTNode, error)
```

Implementation: recursive AST walk that replaces every `PlaceholderExpr` with the
corresponding `LiteralExpr` (using type-appropriate conversion: int64, float64, string,
nil). Returns `ErrMissingParam` if a positional index is out of range or a named key is
not present.

### B2. Wire Into ExecWithParams / QueryWithParams

```
pkg/sqlvibe/database.go — ExecWithParams:
1. Tokenize + parse SQL as usual
2. If len(params) > 0 or namedParams != nil: call QP.BindParams(ast, params, nil)
3. Execute the bound AST directly (skip re-parse)

pkg/sqlvibe/database.go — QueryWithParams:
  Same approach.
```

### B3. Statement.Exec / Statement.Query

Pre-parsed `Statement` structs must also support binding:

```
pkg/sqlvibe/database.go — Statement.Exec(params ...interface{}):
  Clone the pre-parsed AST, bind params, then execute.

pkg/sqlvibe/database.go — Statement.Query(params ...interface{}):
  Same.
```

---

## Track C: Named Parameter Helpers

### C1. `ExecNamed` / `QueryNamed`

Add convenience methods for named parameters:

```go
func (db *Database) ExecNamed(sql string, params map[string]interface{}) (Result, error)
func (db *Database) QueryNamed(sql string, params map[string]interface{}) (*Rows, error)
```

### C2. `MustExec` update

Update `MustExec` to pass variadic params through to `ExecWithParams`.

---

## Track D: Security — SQL Injection Tests

### D1. Regression Suite

Add `internal/TS/Regression/regression_v0.9.11_test.go` with tests that verify:

- `ExecWithParams` with `?` binds correctly and **does not** treat the param as SQL
- Injected SQL in a string param is not executed (e.g. `"'; DROP TABLE t; --"`)
- Named params (`:id`, `@id`) bind correctly
- Missing param returns a clear error (`ErrMissingParam`)
- Too many params is silently ignored (SQLite behaviour)
- `nil` param binds as SQL `NULL`
- Integer, float64, string, and `[]byte` params bind to the right types

### D2. F882 SQL1999 Test Suite

Add `internal/TS/SQL1999/F882/01_test.go` covering:

- `SELECT ? + 1` → correct result
- `INSERT INTO t VALUES (?, ?)` with params
- `SELECT * FROM t WHERE id = ?` with positional param
- `SELECT * FROM t WHERE name = :name` with named param
- `Prepare` + `stmt.Query(param)` round trip
- Multi-row insert with repeated `?` bindings

---

## Files to Modify / Create

| File | Action |
|------|--------|
| `internal/QP/tokenizer.go` | Add `TokenPlaceholderPos`, `TokenPlaceholderNamed`; tokenize `?`, `:name`, `@name` |
| `internal/QP/parser.go` | Add `PlaceholderExpr`; parse placeholder tokens in primary expr |
| `internal/QP/binder.go` | **NEW** — recursive AST param binder |
| `pkg/sqlvibe/database.go` | Fix `ExecWithParams`, `QueryWithParams`; add `ExecNamed`, `QueryNamed`; update `Statement.Exec/Query` |
| `internal/TS/Regression/regression_v0.9.11_test.go` | **NEW** — security / correctness regressions |
| `internal/TS/SQL1999/F882/01_test.go` | **NEW** — parameterized query feature tests |
| `docs/HISTORY.md` | Add v0.9.11 entry |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| `?` positional params work end-to-end | Yes | [ ] |
| `:name` / `@name` named params work | Yes | [ ] |
| `nil` param → SQL NULL | Yes | [ ] |
| `[]byte` param → BLOB | Yes | [ ] |
| Missing param returns error | Yes | [ ] |
| Injected SQL in param is safe | Yes | [ ] |
| `Prepare` + bind round trip | Yes | [ ] |
| `ExecNamed` / `QueryNamed` added | Yes | [ ] |
| F882 suite passes | 100% | [ ] |
| Regression suite passes | 100% | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F882 suite | Parameterized query end-to-end tests (6+ tests) | [ ] |
| Regression v0.9.11 | Injection safety + binding edge cases (8+ tests) | [ ] |
