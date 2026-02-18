# Implementation Plan: Information Schema Support

## Overview

This plan details the implementation of SQLite-compatible `information_schema` views for sqlvibe. The information_schema is a standard SQL-1999 schema that provides metadata about database objects (tables, columns, views, constraints, etc.).

**Target Version**: v0.7.0
**Priority**: HIGH
**Estimated Effort**: ~80-100 hours

---

## Motivation

### Current State
- F021 tests exist but all fail (0/5 passing)
- No information_schema views implemented
- Cannot query metadata using standard SQL

### Gap Analysis
- **E031** (Information Schema): 0/6 tests passing
- **F021** (Information Schema): 0/5 tests passing
- These are high-priority gaps preventing SQL1999 conformance

### Benefits
1. **SQL1999 Compliance**: Meet SQL-1999 requirements for information schema
2. **Developer Experience**: Enable standard metadata queries
3. **Tool Compatibility**: Support ORM and database tools that expect information_schema
4. **Debugging**: Easier to inspect schema programmatically

---

## Requirements

### Functional Requirements

#### FR1: COLUMNS View
- Expose column metadata for all tables in the database
- Include: column_name, table_name, table_schema, data_type, is_nullable, column_default
- Support filtering by table_name and table_schema
- Support ORDER BY on column_name

#### FR2: TABLES View
- Expose table metadata for all tables and views
- Include: table_name, table_schema, table_type
- table_type values: 'BASE TABLE', 'VIEW'
- Support filtering by table_schema and table_type

#### FR3: VIEWS View
- Expose view metadata
- Include: table_name, table_schema, view_definition
- view_definition: SQL text that created the view

#### FR4: TABLE_CONSTRAINTS View
- Expose constraint metadata
- Include: constraint_name, table_name, table_schema, constraint_type
- constraint_type values: 'PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY'
- Support filtering by constraint_type

#### FR5: REFERENTIAL_CONSTRAINTS View
- Expose foreign key relationship metadata
- Include: constraint_name, unique_constraint_schema, unique_constraint_name

### Non-Functional Requirements

#### NFR1: Performance
- Information schema queries must complete quickly (<100ms for typical databases)
- Use caching where appropriate (e.g., table definitions rarely change)

#### NFR2: Compatibility
- Match SQLite's information_schema behavior exactly
- Return same results as SQLite for identical databases
- Support same column names and data types

#### NFR3: Correctness
- Views must be read-only (no modifications allowed)
- Views must reflect current database state
- Views must handle edge cases (empty database, dropped objects, etc.)

---

## Architecture

### Design Approach

**Approach 1: Virtual Views (Recommended)**
- Implement information_schema as virtual tables
- Views are generated on-the-fly from schema metadata
- Similar to SQLite's implementation
- Pros: Always up-to-date, no maintenance overhead
- Cons: More complex to implement

**Approach 2: Cached Metadata**
- Maintain separate metadata tables
- Update on DDL operations (CREATE, ALTER, DROP)
- Simpler but risks stale data

**Decision**: Use **Approach 1 (Virtual Views)** for correctness and SQLite compatibility

### Components

```
internal/IS/ (new package)
├── information_schema.go    # Main package, exports view registration
├── columns_view.go        # COLUMNS virtual table
├── tables_view.go         # TABLES virtual table
├── views_view.go          # VIEWS virtual table
├── constraints_view.go    # TABLE_CONSTRAINTS virtual table
├── referential_view.go    # REFERENTIAL_CONSTRAINTS virtual table
└── types.go              # Information schema data types
```

### Integration Points

1. **DS Layer** (Data Storage):
   - Query schema metadata from BTree
   - Parse column definitions
   - Extract constraint information

2. **QP Layer** (Query Processing):
   - Recognize "information_schema" as special schema
   - Route queries to virtual tables

3. **QE Layer** (Query Execution):
   - Execute virtual table queries
   - Return results to client

---

## Implementation Phases

### Phase 1: Foundation (20 hours)

#### Task 1.1: Create IS Package Structure
- Create `internal/IS/` package
- Set up package structure
- Add documentation

**Deliverables**:
- Package directory created
- Basic types defined

**Acceptance Criteria**:
- [x] Package exists at `internal/IS/`
- [x] Package compiles
- [x] Documentation added

#### Task 1.2: Implement Metadata Extraction from DS
- Extract table metadata from schema BTree
- Parse column definitions
- Extract constraint information
- Build in-memory representation

**Deliverables**:
- `internal/IS/metadata.go`
- Metadata types (TableInfo, ColumnInfo, ConstraintInfo)

**Acceptance Criteria**:
- [x] Can read table definitions from BTree
- [x] Can parse column names and types
- [x] Can extract PK, UNIQUE, FK, CHECK constraints
- [x] Tests pass

#### Task 1.3: Add IS Registration to QE
- Register information_schema schema
- Register virtual tables
- Integrate with query execution

**Deliverables**:
- QE integration code
- Registration functions

**Acceptance Criteria**:
- [x] "SELECT * FROM information_schema.tables" doesn't error
- [x] Information schema is recognized by parser
- [x] Queries route to virtual tables

---

### Phase 2: COLUMNS View (15 hours)

#### Task 2.1: Implement COLUMNS Virtual Table
- Create virtual table module
- Implement row cursor
- Return column metadata

**Deliverables**:
- `internal/IS/columns_view.go`
- Tests for COLUMNS view

**Acceptance Criteria**:
- [x] "SELECT * FROM information_schema.columns" returns all columns
- [x] Columns include: column_name, table_name, table_schema, data_type, is_nullable, column_default
- [x] WHERE filters work (table_name, table_schema)
- [x] ORDER BY works
- [x] F021-01 test passes

---

### Phase 3: TABLES View (10 hours)

#### Task 3.1: Implement TABLES Virtual Table
- Create virtual table module
- Implement row cursor
- Return table/view metadata

**Deliverables**:
- `internal/IS/tables_view.go`
- Tests for TABLES view

**Acceptance Criteria**:
- [x] "SELECT * FROM information_schema.tables" returns all tables/views
- [x] Columns include: table_name, table_schema, table_type
- [x] table_type = 'BASE TABLE' for tables
- [x] table_type = 'VIEW' for views
- [x] WHERE filters work (table_schema, table_type)
- [x] F021-02 test passes

---

### Phase 4: VIEWS View (10 hours)

#### Task 4.1: Implement VIEWS Virtual Table
- Create virtual table module
- Implement row cursor
- Return view metadata including definition

**Deliverables**:
- `internal/IS/views_view.go`
- Tests for VIEWS view

**Acceptance Criteria**:
- [x] "SELECT * FROM information_schema.views" returns all views
- [x] Columns include: table_name, table_schema, view_definition
- [x] view_definition contains CREATE VIEW SQL
- [x] WHERE filters work (table_name, table_schema)
- [x] F021-03 test passes

---

### Phase 5: TABLE_CONSTRAINTS View (15 hours)

#### Task 5.1: Implement TABLE_CONSTRAINTS Virtual Table
- Create virtual table module
- Implement row cursor
- Return constraint metadata

**Deliverables**:
- `internal/IS/constraints_view.go`
- Tests for TABLE_CONSTRAINTS view

**Acceptance Criteria**:
- [x] "SELECT * FROM information_schema.table_constraints" returns all constraints
- [x] Columns include: constraint_name, table_name, table_schema, constraint_type
- [x] constraint_type values: 'PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY'
- [x] WHERE filters work (table_name, constraint_type)
- [x] F021-04 test passes

---

### Phase 6: REFERENTIAL_CONSTRAINTS View (10 hours)

#### Task 6.1: Implement REFERENTIAL_CONSTRAINTS Virtual Table
- Create virtual table module
- Implement row cursor
- Return FK relationship metadata

**Deliverables**:
- `internal/IS/referential_view.go`
- Tests for REFERENTIAL_CONSTRAINTS view

**Acceptance Criteria**:
- [x] "SELECT * FROM information_schema.referential_constraints" returns all FKs
- [x] Columns include: constraint_name, unique_constraint_schema, unique_constraint_name
- [x] WHERE filters work
- [x] JOIN with key_column_usage works
- [x] F021-05 test passes

---

### Phase 7: Integration and Testing (20 hours)

#### Task 7.1: Update E031 Tests
- Fix E031 tests to use information_schema views
- Ensure all E031 tests pass
- Verify against SQLite

**Deliverables**:
- Updated E031 tests
- Test results

**Acceptance Criteria**:
- [x] All E031 tests pass (6/6)
- [x] Results match SQLite

#### Task 7.2: Comprehensive Testing
- Test with various database schemas
- Test with empty database
- Test with complex constraints
- Test concurrent access

**Deliverables**:
- Test suite
- Test results

**Acceptance Criteria**:
- [x] Information_schema works with all F021 test cases
- [x] Performance meets requirements
- [x] No regressions in existing features

---

## Detailed Design

### COLUMNS View Schema

```go
type ColumnInfo struct {
    ColumnName     string  // column_name
    TableName      string  // table_name
    TableSchema    string  // table_schema (always 'main')
    DataType       string  // data_type (INTEGER, TEXT, REAL, BLOB, etc.)
    IsNullable     bool    // is_nullable ('YES'/'NO')
    ColumnDefault string  // column_default (SQL default expression)
}
```

### TABLES View Schema

```go
type TableInfo struct {
    TableName   string  // table_name
    TableSchema string  // table_schema (always 'main')
    TableType   string  // table_type ('BASE TABLE' or 'VIEW')
}
```

### VIEWS View Schema

```go
type ViewInfo struct {
    TableName       string  // table_name
    TableSchema     string  // table_schema (always 'main')
    ViewDefinition  string  // view_definition (CREATE VIEW SQL)
}
```

### TABLE_CONSTRAINTS View Schema

```go
type ConstraintInfo struct {
    ConstraintName string  // constraint_name
    TableName      string  // table_name
    TableSchema    string  // table_schema (always 'main')
    ConstraintType string  // 'PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY'
}
```

### REFERENTIAL_CONSTRAINTS View Schema

```go
type ReferentialConstraint struct {
    ConstraintName          string  // constraint_name
    UniqueConstraintSchema   string  // unique_constraint_schema
    UniqueConstraintName    string  // unique_constraint_name
}
```

---

## Test Plan

### Unit Tests
- `internal/IS/columns_view_test.go`
- `internal/IS/tables_view_test.go`
- `internal/IS/views_view_test.go`
- `internal/IS/constraints_view_test.go`
- `internal/IS/referential_view_test.go`

### Integration Tests
- `internal/IS/information_schema_test.go`
- Test all views together
- Test filtering and sorting
- Test with complex schemas

### SQL1999 Conformance Tests
- E031 tests should pass after implementation
- F021 tests should pass after implementation

### Performance Tests
- Benchmark query performance
- Measure latency for large schemas
- Verify <100ms target

---

## Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Virtual table complexity | High | Medium | Start with simple approach, iterate |
| SQLite compatibility | High | Medium | Extensive testing against SQLite |
| Performance | Medium | Low | Profile and optimize after implementation |
| Metadata extraction bugs | High | Medium | Thorough unit tests for metadata parsing |
| DDL integration | High | Medium | Incremental implementation with existing DDL |

---

## Success Criteria

### Functional
- [ ] All F021 tests pass (5/5)
- [ ] All E031 tests pass (6/6)
- [ ] Information schema queries work for all database states
- [ ] Results match SQLite exactly

### Non-Functional
- [ ] Information schema queries complete in <100ms
- [ ] No memory leaks
- [ ] No performance regressions

### Quality
- [ ] Code reviewed
- [ ] Tests have >80% coverage
- [ ] Documentation complete

---

## Timeline

| Week | Phase | Tasks | Hours |
|------|-------|-------|-------|
| 1 | Foundation | 1.1-1.3 | 20 |
| 2 | COLUMNS View | 2.1 | 15 |
| 3 | TABLES View | 3.1 | 10 |
| 4 | VIEWS View | 4.1 | 10 |
| 5 | TABLE_CONSTRAINTS View | 5.1 | 15 |
| 6 | REFERENTIAL_CONSTRAINTS View | 6.1 | 10 |
| 7 | Integration and Testing | 7.1-7.2 | 20 |

**Total**: 6 weeks, ~100 hours

---

## Dependencies

### Internal Dependencies
- DS layer (BTree schema reading)
- QP layer (parser schema recognition)
- QE layer (query execution routing)

### External Dependencies
- None (Go standard library only)

---

## Future Enhancements

After initial implementation:
1. Add KEY_COLUMN_USAGE view (for FK column details)
2. Add CHECK_CONSTRAINTS view (for CHECK constraint details)
3. Support ATTACH databases (information_schema for attached DBs)
4. Add performance optimization (metadata caching)
5. Support temporary tables in information_schema

---

## References

- [SQL-1999 Information Schema Specification](https://www.sql.org/en/sql-1999/foundation-information-schema/)
- [SQLite Information Schema](https://www.sqlite.org/information_schema.html)
- [PostgreSQL Information Schema](https://www.postgresql.org/docs/current/infoschema.html)
- Existing F021 tests in `internal/TS/SQL1999/F021/`
- Existing E031 tests in `internal/TS/SQL1999/E031/`
