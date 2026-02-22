# Plan v0.8.2 - SQL Coverage & Storage Refactor

## Summary

Add missing SQL:1999 test suites and refactor storage layer to internal/DS subsystem.

**Previous**: v0.8.1 delivers columnar VM, CG optimizations, multi-core, DAG execution

---

## Phase 1: SQL:1999 Test Suites

Add missing test suites for full SQL:1999 compliance:

### 1.1 E071 - Basic Query Expressions

| Test File | Description |
|-----------|-------------|
| E071_01 | Column references in query expressions |
| E071_02 | Table references in query expressions |
| E071_03 | Query expression syntax |

### 1.2 F221 - Explicit Defaults

| Test File | Description |
|-----------|-------------|
| F221_01 | DEFAULT value in INSERT |
| F221_02 | DEFAULT in UPDATE |
| F221_03 | DEFAULT in column definition |

### 1.3 F471 - Scalar Subquery Values

| Test File | Description |
|-----------|-------------|
| F471_01 | Scalar subquery in SELECT |
| F471_02 | Scalar subquery in WHERE |
| F471_03 | Scalar subquery in ORDER BY |

### 1.4 F812 - Basic Flagging

| Test File | Description |
|-----------|-------------|
| F812_01 | Flag functionality tests |

### 1.5 F032 - CASCADE Drop Behavior

| Test File | Description |
|-----------|-------------|
| F032_01 | CASCADE for DROP TABLE |
| F032_02 | CASCADE for DROP INDEX |
| F032_03 | CASCADE with dependencies |

### 1.6 F033 - ALTER TABLE DROP COLUMN

| Test File | Description |
|-----------|-------------|
| F033_01 | DROP COLUMN basic |
| F033_02 | DROP COLUMN with constraints |
| F033_03 | DROP COLUMN with data |

### 1.7 F034 - Extended REVOKE

| Test File | Description |
|-----------|-------------|
| F034_01 | REVOKE on table |
| F034_02 | REVOKE on column |
| F034_03 | REVOKE with GRANT OPTION |

### 1.8 F111 - Isolation Levels

| Test File | Description |
|-----------|-------------|
| F111_01 | READ UNCOMMITTED |
| F111_02 | READ COMMITTED |
| F111_03 | REPEATABLE READ |

### 1.9 F121 - Basic Diagnostics Management

| Test File | Description |
|-----------|-------------|
| F121_01 | GET DIAGNOSTICS basic |
| F121_02 | Diagnostics with row count |

### Tasks

- [ ] Implement E071 test files
- [ ] Implement F221 test files
- [ ] Implement F471 test files
- [ ] Implement F812 test files
- [ ] Implement F032 test files
- [ ] Implement F033 test files
- [ ] Implement F034 test files
- [ ] Implement F111 test files
- [ ] Implement F121 test files
- [ ] Run all new tests
- [ ] Fix any failures

---

## Phase 2: Wire Up HybridStore Aggregates

### Problem

HybridStore has optimized aggregate functions but they're NOT being used by SQL queries:

```go
// In pkg/sqlvibe/storage/parallel.go - OPTIMIZED but UNUSED
func (hs *HybridStore) ParallelCount() int64  // O(1)
func (hs *HybridStore) ParallelSum(colName string) int64  // O(n) vectorized
```

Current flow (SLOW):
```
SELECT COUNT(*) FROM t → execVMQuery() → VM → OpAggregate → row-by-row scan
```

Expected flow (FAST):
```
SELECT COUNT(*) FROM t → HybridStore.ParallelCount() → O(1)
```

### Solution

Add aggregate fast path in database.go:

```go
// Add to execVMQuery() or new function
func (db *Database) tryAggregateFastPath(stmt *SelectStmt) (*Rows, error) {
    // Only for simple aggregates without JOIN
    if hasJoin(stmt) {
        return nil, nil  // Fall through to VM
    }
    
    hs := db.GetHybridStore(tableName)
    
    // COUNT(*) - O(1)
    if stmt.IsCountStar() && stmt.GroupBy == nil {
        count := hs.ParallelCount()
        return &Rows{Columns: []string{"COUNT(*)"}, Data: [][]interface{}{{count}}}, nil
    }
    
    // SUM(col) - O(n) vectorized
    if stmt.IsSimpleSum() && stmt.GroupBy == nil {
        sum := hs.ParallelSum(colName)
        return &Rows{Columns: []string{fmt.Sprintf("SUM(%s)", colName)}}, nil
    }
    
    return nil, nil  // Fall through to VM
}
```

### Tasks

- [ ] Implement isSimpleAggregate() detector
- [ ] Add tryAggregateFastPath() in database.go
- [ ] Wire up COUNT(*) fast path
- [ ] Wire up SUM(col) fast path
- [ ] Wire up MIN/MAX fast paths
- [ ] Benchmark COUNT(*) - target < 50 ns (from 663 ns)
- [ ] Benchmark SUM - target < 100 ns (from 673 ns)

### Benchmark Requirements

| Benchmark | Current | Target | Speedup |
|-----------|---------|--------|---------|
| COUNT(*) | 663 ns | < 50 ns | 13x |
| SUM(col) | 673 ns | < 100 ns | 6.7x |
| MIN/MAX | 675 ns | < 100 ns | 6.7x |

---

## Phase 3: Move pkg/sqlvibe/storage to internal/DS

Refactor storage layer to internal/DS subsystem:

### 2.1 Current Structure

```
pkg/sqlvibe/storage/
├── arena.go
├── column_store.go
├── column_vector.go
├── hybrid_store.go
├── index_engine.go
├── persistence.go
├── roaring_bitmap.go
├── row.go
├── row_store.go
├── skip_list.go
└── value.go
```

### 2.2 Target Structure

```
internal/DS/
├── arena.go              # NEW: from pkg/sqlvibe/storage
├── column_store.go      # NEW: from pkg/sqlvibe/storage
├── column_vector.go     # NEW: from pkg/sqlvibe/storage
├── hybrid_store.go      # NEW: from pkg/sqlvibe/storage
├── index_engine.go     # NEW: from pkg/sqlvibe/storage
├── persistence.go      # NEW: from pkg/sqlvibe/storage
├── roaring_bitmap.go   # NEW: from pkg/sqlvibe/storage
├── row.go             # NEW: from pkg/sqlvibe/storage
├── row_store.go       # NEW: from pkg/sqlvibe/storage
├── skip_list.go       # NEW: from pkg/sqlvibe/storage
├── value.go           # NEW: from pkg/sqlvibe/storage
└── ...

# Remove legacy (done in v0.8.1)
# btree.go, page.go, encoding.go, etc. (should be deleted)
```

### 2.3 Update Imports

```bash
# Update all imports from pkg/sqlvibe/storage to internal/DS
# Find and replace in all .go files:
github.com/sqlvibe/sqlvibe/pkg/sqlvibe/storage → github.com/sqlvibe/sqlvibe/internal/DS
```

### 2.4 Update Package Exports

```go
// internal/DS/ds.go - Public exports
package DS

// Re-export storage types for backward compatibility
type HybridStore = storage.HybridStore
type ColumnVector = storage.ColumnVector
type Value = storage.Value
type RoaringBitmap = storage.RoaringBitmap

// Keep compatible API
func Open(path string) (*HybridStore, error)
func OpenMemory() (*HybridStore, error)
```

### Tasks

- [ ] Move all files from pkg/sqlvibe/storage to internal/DS
- [ ] Update package declarations
- [ ] Update all imports in codebase
- [ ] Add backward compatibility shim
- [ ] **Set HybridStore as default storage for all queries**
- [ ] **Remove old row-based storage from database.go**
- [ ] **Move pkg/sqlvibe source files to internal/ subsystems**
- [ ] Delete legacy DS files (btree.go, page.go, etc. from v0.8.1)
- [ ] Run tests to verify
- [ ] Update pkg/sqlvibe to use internal/DS

---

## Files to Modify

```
internal/VM/                    # NEW files from pkg/sqlvibe
├── vm_exec.go               # FROM pkg/sqlvibe
├── vm_context.go            # FROM pkg/sqlvibe
└── window.go               # FROM pkg/sqlvibe

internal/QE/                  # NEW files from pkg/sqlvibe
├── exec_columnar.go         # FROM pkg/sqlvibe
├── hash_join.go             # FROM pkg/sqlvibe
├── setops.go                # FROM pkg/sqlvibe
└── window.go               # FROM pkg/sqlvibe

internal/IS/                  # NEW files from pkg/sqlvibe
├── pragma.go                # FROM pkg/sqlvibe
└── explain.go              # FROM pkg/sqlvibe

internal/TS/                  # NEW tests from pkg/sqlvibe
├── benchmark_test.go       # FROM pkg/sqlvibe
├── compat_test.go          # FROM pkg/sqlvibe
├── index_test.go           # FROM pkg/sqlvibe
└── ...

pkg/sqlvibe/                  # MINIMAL after refactor
├── database.go             # KEEP: public API
└── version.go              # KEEP: version info
```
internal/DS/                    # REPLACE with storage layer
├── arena.go                   # FROM pkg/sqlvibe/storage
├── column_store.go            # FROM pkg/sqlvibe/storage
├── column_vector.go           # FROM pkg/sqlvibe/storage
├── hybrid_store.go            # FROM pkg/sqlvibe/storage
├── index_engine.go           # FROM pkg/sqlvibe/storage
├── persistence.go            # FROM pkg/sqlvibe/storage
├── roaring_bitmap.go         # FROM pkg/sqlvibe/storage
├── row.go                   # FROM pkg/sqlvibe/storage
├── row_store.go             # FROM pkg/sqlvibe/storage
├── skip_list.go             # FROM pkg/sqlvibe/storage
├── value.go                 # FROM pkg/sqlvibe/storage
└── ds_compat.go             # NEW: backward compatibility

pkg/sqlvibe/                  # UPDATE imports
├── storage/                  # DELETE after move
├── database.go              # UPDATE: import internal/DS
└── ...

### 2.4 Move pkg/sqlvibe/*.go to Internal Subsystems

Current pkg/sqlvibe/*.go files categorized by destination:

```
pkg/sqlvibe/*.go → internal/SUBSYSTEM/
========================================

VM (Virtual Machine):
├── vm_exec.go           → internal/VM/
├── vm_context.go        → internal/VM/
└── window.go           → internal/VM/

QE (Query Execution):
├── exec_columnar.go    → internal/QE/
├── hash_join.go        → internal/QE/
├── setops.go           → internal/QE/
└── window.go            → internal/QE/

IS (Information Schema):
├── pragma.go           → internal/IS/
└── explain.go          → internal/IS/

DS (Data Storage):
└── (storage moved in Phase 2)

Tests:
├── benchmark_test.go    → internal/TS/Benchmark/
├── compat_test.go      → internal/TS/
├── index_test.go       → internal/TS/
├── index_usage_test.go → internal/TS/
├── pragma_test.go      → internal/TS/
└── exec_columnar_test.go → internal/TS/

Keep in pkg/sqlvibe:
├── version.go          # Public API version
└── database.go        # Public API entry point (update imports)
```

### 2.5 Import Updates

```bash
# Update imports in moved files
github.com/sqlvibe/sqlvibe/pkg/sqlvibe/storage → github.com/sqlvibe/sqlvibe/internal/DS
github.com/sqlvibe/sqlvibe/pkg/sqlvibe → github.com/sqlvibe/sqlvibe/internal/VM
github.com/sqlvibe/sqlvibe/pkg/sqlvibe → github.com/sqlvibe/sqlvibe/internal/QE
github.com/sqlvibe/sqlvibe/pkg/sqlvibe → github.com/sqlvibe/sqlvibe/internal/IS
```

### Tasks for File Moves

- [ ] Move VM files (vm_exec.go, vm_context.go) → internal/VM/
- [ ] Move QE files (exec_columnar.go, hash_join.go, setops.go) → internal/QE/
- [ ] Move IS files (pragma.go, explain.go) → internal/IS/
- [ ] Move tests to internal/TS/
- [ ] Update imports in all moved files
- [ ] Update database.go to use new internal/ paths
- [ ] Run tests to verify

---

## Files to Modify

## Phase 4: IS (Information Schema) Optimizations

### 4.1 Schema Cache

Cache schema to avoid repeated rebuilding:

```go
type SchemaCache struct {
    mu sync.RWMutex
    tables    map[string]*TableSchema
    version   int64
    invalidated bool
}

var globalSchemaCache SchemaCache

func (is *Registry) GetTables() []TableInfo {
    globalSchemaCache.RLock()
    if !globalSchemaCache.invalidated {
        return globalSchemaCache.cached // Return cached
    }
    globalSchemaCache.RUnlock()
    
    // Build and cache
    tables := is.buildTables()
    globalSchemaCache.tables = tables
    globalSchemaCache.invalidated = false
    return tables
}

func (db *Database) InvalidateSchema() {
    globalSchemaCache.Lock()
    globalSchemaCache.invalidated = true
    globalSchemaCache.Unlock()
}
```

### 4.2 Incremental Update

Only update changed parts:

```go
func (is *Registry) InvalidateTable(table string) {
    globalSchemaCache.Lock()
    delete(globalSchemaCache.tables, table)
    globalSchemaCache.Unlock()
}

func (is *Registry) AddTable(schema *TableSchema) {
    globalSchemaCache.Lock()
    globalSchemaCache.tables[schema.Name] = schema
    globalSchemaCache.Unlock()
}
```

### 4.3 Simplified Schema Model

Replace complex nested maps with flat structures:

```go
// Before: map[string]map[string]string
// After: Flat arrays + index

type SchemaIndex struct {
    tables      []TableSchema
    byName      map[string]int      // table name → index
    byColumn    map[string]int     // column name → index
}

func (si *SchemaIndex) FindTable(name string) *TableSchema {
    if idx, ok := si.byName[name]; ok {
        return &si.tables[idx]
    }
    return nil
}

func (si *SchemaIndex) FindColumn(table, col string) *ColumnSchema {
    if tidx, ok := si.byName[table]; ok {
        if cidx, ok := si.tables[tidx].colIndex[col]; ok {
            return &si.tables[tidx].columns[cidx]
        }
    }
    return nil
}
```

### Tasks

- [ ] Implement SchemaCache with sync.RWMutex
- [ ] Add InvalidateSchema() on DDL
- [ ] Add InvalidateTable() on table changes
- [ ] Simplify SchemaIndex with flat arrays
- [ ] Benchmark schema queries

### Benchmark Requirements

| Benchmark | Target | vs Before |
|-----------|--------|------------|
| SELECT * FROM information_schema.tables | < 1 ms | 10x faster |
| Schema query after cache | < 100 µs | 100x faster |

---

## Success Criteria

| Criteria | Target |
|----------|--------|
| SQL:1999 test suites | 65+ suites (was 56) |
| New test coverage | E071, F221, F471, F812, F032, F033, F034, F111, F121 |
| **COUNT(*) wired to HybridStore** | < 50 ns (13x faster) |
| **SUM(col) wired to HybridStore** | < 100 ns (6.7x faster) |
| **HybridStorage default** | **100% of queries use HybridStore** |
| pkg/sqlvibe/*.go moved | All to internal/ subsystems |
| Storage layer moved | All files to internal/DS |
| Legacy DS removed | No B-Tree/page files remain |
| Schema cache | < 100 µs (100x faster) |
| All tests pass | 100% |

---

## Timeline Estimate

| Phase | Tasks | Hours |
|-------|-------|-------|
| 1 | SQL:1999 Test Suites | 20 |
| 2 | Wire Up HybridStore Aggregates | 10 |
| 3 | Storage Refactor + HybridStore | 15 |
| 4 | Move pkg/sqlvibe to internal/ | 10 |
| 5 | IS Optimizations | 10 |

**Total**: ~65 hours

---

## Notes

- Focus on high-value SQL:1999 tests first
- Ensure backward compatibility after storage move
- Delete pkg/sqlvibe/storage only after all imports updated
