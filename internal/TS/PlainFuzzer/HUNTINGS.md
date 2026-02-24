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

## v0.9.9 (2026-02-24)

### Parser infinite loop in NOT IN clause

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parseCmpExpr` (NOT IN clause value parsing loops) |
| **Trigger SQL** | `SELECT c0 NOT IN (/` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | The NOT IN parsing loop had no EOF check, no nil expression check, and no unexpected token recovery. Same issue as the IN clause bug but in a different code path. |
| **Fix** | Added guards for EOF, nil expressions, and unexpected tokens to break out of the loop, matching the fix for IN clause |
| **Found By** | PlainFuzzer with SQL1999 integration |
| **Date** | 2026-02-24 |

---

### Parser infinite loop in CTE column list

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parseWithClause` (CTE column list parsing) |
| **Trigger SQL** | `WITH ctLECT (SELECT 1 AS n) SEe AS * FROM cte` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | CTE column list parsing loop had no break for unexpected tokens. When given malformed CTE like `ctLECT` (invalid identifier), the parser would loop infinitely waiting for a valid token. |
| **Fix** | Added else clause with break to exit loop on unexpected tokens |
| **Found By** | PlainFuzzer with SQL1999 integration |
| **Date** | 2026-02-24 |

---

### Parser infinite loop in VALUES row parsing

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parseTableRef` (VALUES row parsing) |
| **Trigger SQL** | `SELECT*FROM(VALUES(0)((),` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | VALUES row parsing loop had no nil expression check and no recovery for unexpected tokens. Malformed input like `(()` caused infinite loop. |
| **Fix** | Added nil expression check and unexpected token recovery (break when not comma or right paren) |
| **Found By** | PlainFuzzer with SQL1999 integration |
| **Date** | 2026-02-24 |

---

### Parser infinite loop in VALUES column list

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parseTableRef` (VALUES column list parsing) |
| **Trigger SQL** | `SELECT*FROM(VALUES(0)((),` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | VALUES column list parsing loop had no break for unexpected tokens. |
| **Fix** | Added else clause with break to exit loop on unexpected tokens |
| **Found By** | PlainFuzzer with SQL1999 integration |
| **Date** | 2026-02-24 |

---

### Parser infinite loop in subquery recovery

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parseTableRef` (subquery recovery) |
| **Trigger SQL** | `SELECT A%FROM(.ET AS x` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | When subquery parsing fails (due to malformed input like `%`), the parser didn't skip tokens to find the closing paren. The recovery code only consumed `)` if it was immediately next, causing infinite loop. |
| **Fix** | Added recovery loop to skip tokens until finding `)`, EOF, or semicolon |
| **Found By** | PlainFuzzer with SQL1999 integration |
| **Date** | 2026-02-24 |

---

### Parser infinite loop in function argument parsing (additional case)

| Attribute | Value |
|-----------|-------|
| **Severity** | High (Denial of Service) |
| **Type** | Infinite Loop |
| **File** | `internal/QP/parser.go` |
| **Function** | `parsePrimaryExpr` (generic keyword function call) |
| **Trigger SQL** | `SELECT A%FROM(.ET AS x` |
| **Impact** | Parser hangs indefinitely |
| **Root Cause** | Generic keyword function argument parsing loop (`DATE(...`, etc.) had no nil argument check and no unexpected token handling. When `parseExpr` returned nil, the loop continued without advancing. |
| **Fix** | Added nil argument check with break, plus unexpected token recovery (break when not comma or right paren) |
| **Found By** | PlainFuzzer with SQL1999 integration |
| **Date** | 2026-02-24 |

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
