# Plan v0.10.9 - Information Schema + Refactoring

## Summary

Complete Information Schema implementation and refactor pragma handling into subpackage.

## Background

### Existing TODOs (18 items)
- 10 Information Schema TODO items
- 8 Schema Extractor TODO items

### Current State
- PRAGMA: 30+ pragmas implemented in 1017-line file
- IS Views: Exist but return empty/incomplete data
- Schema Extractor: Stubs only

---

## 1. Features

### 1.1 Information Schema Views

| View | Status | Implementation |
|------|--------|----------------|
| information_schema.columns | TODO | Return column metadata |
| information_schema.tables | TODO | Return table list |
| information_schema.table_constraints | TODO | Return PK/UNIQUE/FK/CHECK |
| information_schema.referential_constraints | TODO | Return FK relationships |
| information_schema.views | TODO | Return view definitions |

### 1.2 Schema Extractor

| Method | Status | Implementation |
|--------|--------|----------------|
| ExtractTables | TODO | Extract from BTree root page |
| ExtractColumns | TODO | Extract from table page |
| ExtractViews | TODO | Query sqlite_master |
| ExtractConstraints | TODO | Parse CREATE TABLE |
| GetAllConstraints | TODO | Iterate all tables |
| GetReferentialConstraints | TODO | Extract FK relations |

---

## 2. Refactoring

### Goal
Refactor `pkg/sqlvibe/pragma.go` (1017 lines) into subpackage

### Structure

```
pkg/sqlvibe/
├── pragma.go              # Main entry, ~150 lines
└── pragma/                # Subpackage
    ├── cache.go           # cache_size, cache_memory, cache_spill
    ├── storage.go         # page_size, mmap_size, storage_info
    ├── wal.go             # wal_mode, checkpoint, autocheckpoint
    ├── vacuum.go          # auto_vacuum, shrink_memory
    ├── transaction.go      # busy_timeout, isolation_level
    └── compat.go          # encoding, collation_list
```

---

## 3. Tests

### Target Coverage
Current: 23.6% → Target: 50%

### Test Files to Add

| Test File | Coverage | Test Cases |
|-----------|----------|------------|
| pragma/cache_test.go | cache settings | ~6 |
| pragma/storage_test.go | memory/page settings | ~6 |
| pragma/wal_test.go | WAL operations | ~6 |
| pragma/vacuum_test.go | vacuum operations | ~6 |
| pragma/transaction_test.go | transaction settings | ~6 |

**Total New Tests**: ~30

---

## 4. Implementation Order

1. Create `pkg/sqlvibe/pragma/` subpackage
2. Move code to subpackage files
3. Add pragma_*_test.go files
4. Add IS view implementations
5. Add Schema Extractor implementations
6. Run all tests
7. Commit

---

## 5. Success Criteria

- [x] pragma/ subpackage created and working
- [x] SchemaSource interface added to IS package
- [x] SchemaExtractor updated with SchemaSource support
- [x] NewMetadataProviderWithSource added
- [x] FKInfo type added to IS package
- [x] pragma_ctx.go implements pragma.Ctx interface on *Database
- [x] pragma.go rewritten as thin dispatch table (~250 lines)
- [x] All tests pass
- [x] All 18 TODOs resolved
- [x] pragma/*_test.go added (30 tests: cache, storage, wal, vacuum, transaction)
- [ ] Coverage: pkg/sqlvibe → 50%
