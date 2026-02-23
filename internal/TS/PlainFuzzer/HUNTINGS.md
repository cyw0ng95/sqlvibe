# PlainFuzzer Hunting Log

A record of bugs discovered through sqlvibe's PlainFuzzer - a SQL-native fuzzer using SQLSmith-style mutation strategies.

---

## v0.9.8 (2026-02-23)

### Parser infinite loop in IN clause

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parseEqExpr` (IN clause value parsing loops) |
| **Trigger SQL** | `SELECT IN(c` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | The loops at `parseEqExpr` had no EOF check, no nil expression check, and no unexpected token recovery. When given malformed input like `SELECT IN(c`, the parser would never advance past the invalid token, causing an infinite loop. |
| **Fix** | Added guards for EOF, nil expressions, and unexpected tokens to break out of the loop |
| **Found By** | PlainFuzzer |
| **Date** | 2026-02-23 |

---

### Empty tableName panic in execVMDML

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Panic) |
| **Type** | Assertion Failure |
| **File** | `pkg/sqlvibe/vm_exec.go` |
| **Function** | `execVMDML` |
| **Trigger SQL** | `UPDATE` |
| **Impact** | Panic: `"Assertion failed: tableName cannot be empty"` |
| **Root Cause** | `execVMDML` used `util.Assert` instead of returning an error. Also, the fuzzer recover block didn't handle non-error panics. |
| **Fix** | Changed to return error instead of asserting; fixed fuzzer recover block to handle all panics |
| **Found By** | PlainFuzzer (60s run, 315K+ executions) |
| **Date** | 2026-02-23 |

---

### Parser infinite loop in function argument parsing

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parsePrimaryExpr` (function argument parsing loops) |
| **Trigger SQL** | `SELECT MAX(0;` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | Function argument parsing loops had no nil expression check and no unexpected token recovery. When given malformed input like `SELECT MAX(0;`, the parser would see `;` inside the parentheses and never advance, causing an infinite loop. |
| **Fix** | Added nil expression check and unexpected token recovery to break out of the loop |
| **Found By** | PlainFuzzer |
| **Date** | 2026-02-23 |

---

## Running the Fuzzer

```bash
# Run fuzzer with timeout
go test -v -run=FuzzSQL -timeout=60s ./internal/TS/PlainFuzzer/...

# Run with corpus minimization
go test -fuzz=FuzzSQL -fuzztime=60s ./internal/TS/PlainFuzzer/...
```

## Fuzzer Corpus

Fuzzer corpus entries are stored in `testdata/`. Each entry represents a unique SQL input that triggers interesting behavior.

## Adding New Bugs

When a new bug is found:

1. Add an entry to this file with all relevant details
2. Add a regression test in `internal/TS/Regression/`
3. Do NOT add to HISTORY.md - HUNTINGS.md is the source of truth for fuzzer bugs
