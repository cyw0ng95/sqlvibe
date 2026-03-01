# Plan v0.10.10 - Schema Parser + SQLSTATE + Remaining TODOs

## Summary

Implement Schema Parser and SQLSTATE error codes, refactor parser into subpackage, resolve remaining TODOs.

## Background

### Remaining TODOs to Resolve
| File | TODO | Status |
|------|------|--------|
| setops.go | Complete full VM bytecode compilation | v0.10.11 |
| transaction.go | Implement actual rollback logic | v0.10.11 |
| internal/TS/SQL1999/E171/01_test.go | Check SQLSTATE error code | v0.10.10 |
| internal/IS/registry_test.go | Use proper BTree initialization | v0.10.10 |

### Existing TODOs
- Schema Parser: 2 items (CREATE TABLE/VIEW parsing)
- SQLSTATE: 1 item (error code implementation)

### Current State
- parser.go: 3858 lines (too large)
- .schema command: Not implemented
- SQLSTATE: Not implemented

---

## 1. Features

### 1.1 Schema Parser

| Function | Description |
|----------|-------------|
| ParseTableSchema | Parse CREATE TABLE statement |
| ParseViewSchema | Parse CREATE VIEW statement |
| ExtractColumnDef | Extract column definitions |
| ExtractConstraints | Extract PK/UNIQUE/FK/CHECK |

### 1.2 .schema Command

```sql
.schema              -- Show all schemas
.schema table_name   -- Show specific table
.schema --ddl       -- Show DDL format
```

### 1.3 SQLSTATE Error Codes

| Code | Description |
|------|-------------|
| 22001 | String data right truncated |
| 22003 | Numeric value out of range |
| 23505 | Unique violation |
| 23503 | Foreign key violation |
| 23000 | Integrity constraint violation |

### 1.4 Resolve Remaining TODOs

| Task | Description |
|------|-------------|
| SQLSTATE Test | Update SQL1999/E171 test to check SQLSTATE codes |
| BTree Init | Fix registry_test.go to use proper BTree initialization |

---

## 2. Refactoring

### Goal
Refactor `internal/QP/parser.go` (3858 lines) into subpackage

### Structure

```
internal/QP/
├── parser.go              # Main entry, ~300 lines
└── parser/               # Subpackage
    ├── create.go          # CREATE TABLE/INDEX/VIEW/TRIGGER
    ├── alter.go           # ALTER TABLE
    ├── insert.go          # INSERT
    ├── select.go          # SELECT
    ├── expr.go            # Expression parsing
    ├── util.go            # Utilities
    └── token.go           # Token handling
```

---

## 3. Tests

### Target Coverage
Current: 31.9% → Target: 50%

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| parser/create_test.go | CREATE parsing | ~8 |
| parser/alter_test.go | ALTER parsing | ~8 |
| parser/insert_test.go | INSERT parsing | ~8 |
| parser/select_test.go | SELECT parsing | ~8 |
| parser/expr_test.go | Expression parsing | ~8 |

**Total New Tests**: ~40

---

## 4. Implementation Order

1. Create `internal/QP/parser/` subpackage
2. Move code to subpackage files
3. Add parser/*_test.go files
4. Implement Schema Parser
5. Implement .schema command
6. Implement SQLSTATE errors
7. Run all tests
8. Commit

---

## 5. Success Criteria

- [x] Schema Parser implemented
- [x] .schema command working
- [x] SQLSTATE error codes implemented
- [x] SQL1999/E171 test updated to check SQLSTATE codes
- [x] registry_test.go BTree initialization fixed
- [x] parser/ subpackage created
- [x] parser/*_test.go added (~40 tests)
- [x] All tests pass
- [ ] Coverage: internal/QP → 50%
