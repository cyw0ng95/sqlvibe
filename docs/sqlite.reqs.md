# SQLite Feature Requirements

This document tracks SQLite-specific features to implement in sqlvibe, linked to versions in `ROADMAP.md`.

---

## v0.2.x Requirements

### WHERE Clause Enhancements

| Feature | Syntax | Status | Tests |
|---------|--------|--------|-------|
| AND evaluation | `a AND b` | ✅ Done | - |
| OR evaluation | `a OR b` | ✅ Done | - |
| NOT evaluation | `NOT a` | ✅ Done | - |
| IS NULL | `a IS NULL` | ✅ Done | - |
| IS NOT NULL | `a IS NOT NULL` | ✅ Done | - |
| IN operator | `a IN (1,2,3)` | ✅ Done | - |
| BETWEEN | `a BETWEEN x AND y` | ✅ Done | - |
| LIKE | `a LIKE '%pattern%'` | ✅ Done | - |

### NULL Handling Functions

| Feature | Syntax | Status | Tests |
|---------|--------|--------|-------|
| COALESCE | `COALESCE(a, b, ...)` | ⏳ Pending | - |
| IFNULL | `IFNULL(a, b)` | ⏳ Pending | - |

---

## v0.3.x Requirements

### System Tables

| Feature | Syntax | Status | Tests |
|---------|--------|--------|-------|
| sqlite_master | `SELECT * FROM sqlite_master` | ⏳ Pending | TestMultipleTables |

### JOINs

| Feature | Syntax | Status | Tests |
|---------|--------|--------|-------|
| INNER JOIN | `a INNER JOIN b ON a.id = b.id` | ⏳ Pending | TestQueryJoins |
| LEFT JOIN | `a LEFT JOIN b ON a.id = b.id` | ⏳ Pending | TestQueryJoins |
| CROSS JOIN | `a CROSS JOIN b` | ⏳ Pending | TestQueryJoins |

---

## v1.0.x Requirements

### Transactions (Full)

| Feature | Syntax | Status |
|---------|--------|--------|
| BEGIN | `BEGIN [TRANSACTION]` | ⏳ Pending |
| COMMIT | `COMMIT` | ⏳ Pending |
| ROLLBACK | `ROLLBACK` | ⏳ Pending |
| SAVEPOINT | `SAVEPOINT name` | ⏳ Pending |
| RELEASE SAVEPOINT | `RELEASE SAVEPOINT name` | ⏳ Pending |
| Auto-commit | Implicit on non-transactional | ⏳ Pending |

### Indexes

| Feature | Syntax | Status |
|---------|--------|--------|
| CREATE INDEX | `CREATE INDEX idx ON t(col)` | ⏳ Pending |
| DROP INDEX | `DROP INDEX idx` | ⏳ Pending |
| Index usage | Query planner uses indexes | ⏳ Pending |

### PRAGMA Statements

| Feature | Syntax | Status |
|---------|--------|--------|
| pragma_table_info | `PRAGMA table_info(t)` | ⏳ Pending |
| pragma_index_list | `PRAGMA index_list(t)` | ⏳ Pending |
| pragma_database_list | `PRAGMA database_list` | ⏳ Pending |

### Additional SQL Features

| Feature | Syntax | Status |
|---------|--------|--------|
| CASE WHEN | `CASE WHEN ... THEN ... END` | ⏳ Pending |
| DISTINCT | `SELECT DISTINCT ...` | ⏳ Pending |
| LIMIT with OFFSET | `LIMIT n OFFSET m` | ⏳ Pending |
| CAST | `CAST(expr AS type)` | ⏳ Pending |

---

## Reference

- SQLite Language: https://www.sqlite.org/lang.html
- SQLite Keywords: https://www.sqlite.org/lang_keywords.html
- SQLite Expression Syntax: https://www.sqlite.org/lang_expr.html
