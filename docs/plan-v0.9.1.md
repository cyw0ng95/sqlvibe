# Plan v0.9.1 - Performance Optimizations Round 2

## Summary

Implement eight high-impact optimizations focused on OLTP workloads (TPC-C compatible):

**Storage & Index Layer**:
- **Covering Index**: Avoid table lookup for index-only queries (OLTP point queries)
- **Column Projection**: Reduce memory for wide-table queries
- **Index Skip Scan**: Enable index usage when leading column is unconstrained

**Memory Management**:
- **Slab Allocator**: Reduce GC pressure for high-throughput workloads

**Query Interface**:
- **Prepared Statement Pool**: Reuse compiled query plans for parameterized queries

**Fundamental Optimizations (QP+CG+VM)**:
- **Direct Threaded VM**: Eliminate switch dispatch overhead in VM execution loop
- **Query Compilation Pipeline**: Single-pass compilation, eliminate intermediate AST
- **Expression Bytecode**: Compiled expressions with vectorized evaluation support

**Previous**: v0.9.0 delivers Extension Framework, Fast Hash JOIN, BETWEEN pushdown, Early Termination

**v0.9.1 Scope**:
- Covering Index: 6h
- Column Projection: 4h
- Index Skip Scan: 8h
- Slab Allocator: 8h
- Prepared Statement Pool: 2h
- Direct Threaded VM: 4h
- Query Compilation Pipeline: 8h
- Expression Bytecode: 6h
- Testing & Benchmarks: 4h

**Total**: ~50 hours

**TPC-C Impact Analysis**:

| Transaction | Frequency | Key Optimizations |
|-------------|-----------|-------------------|
| New-Order | 45% | Slab Allocator, Prepared Statement Pool, Direct Threaded VM |
| Payment | 43% | Covering Index, Prepared Statement Pool, Expression Bytecode |
| Order-Status | 4% | Covering Index, Column Projection, Direct Threaded VM |
| Delivery | 4% | Column Projection, Expression Bytecode |
| Stock-Level | 4% | Index Skip Scan, Column Projection, Query Pipeline |

---

## Phase 1: Covering Index (6h)

### Overview

A covering index contains all columns needed by a query, allowing the engine to satisfy the query from the index alone without accessing the base table (no "table lookup").

**Example**:
```sql
CREATE INDEX idx_covering ON users(age, name);

-- Before: Index lookup + table lookup
SELECT name FROM users WHERE age = 25;

-- After: Index-only scan (covering)
-- All needed columns (age, name) are in the index
```

### Current Behavior

1. Index lookup returns row IDs
2. For each row ID, fetch full row from table
3. Extract required columns

### Optimized Behavior

1. Index lookup returns row IDs
2. Check if index columns cover all SELECT columns
3. If covered, return values directly from index (no table lookup)

### Implementation

#### 1.1 Index Metadata Enhancement

```go
// internal/DS/index_engine.go

type IndexMeta struct {
    Name        string
    TableName   string
    Columns     []string   // Ordered columns in the index
    IsPrimary   bool
    IsUnique    bool
    IsCovering  bool       // NEW: marks if index can cover common queries
}

// CoversColumns checks if this index can satisfy a query without table lookup
func (im *IndexMeta) CoversColumns(requiredCols []string) bool {
    colSet := make(map[string]bool)
    for _, c := range im.Columns {
        colSet[c] = true
    }
    for _, req := range requiredCols {
        if !colSet[req] {
            return false
        }
    }
    return true
}
```

#### 1.2 Query Optimizer Integration

```go
// internal/QP/optimizer.go

// FindCoveringIndex finds an index that covers all required columns
func FindCoveringIndex(indexes []*IndexMeta, requiredCols []string) *IndexMeta {
    for _, idx := range indexes {
        if idx.CoversColumns(requiredCols) {
            return idx
        }
    }
    return nil
}

// SelectBestIndex chooses between covering and non-covering indexes
func SelectBestIndex(indexes []*IndexMeta, filterCol string, requiredCols []string) *IndexMeta {
    // Priority 1: Covering index on filter column
    for _, idx := range indexes {
        if len(idx.Columns) > 0 && idx.Columns[0] == filterCol && idx.CoversColumns(requiredCols) {
            return idx
        }
    }
    // Priority 2: Any index on filter column
    for _, idx := range indexes {
        if len(idx.Columns) > 0 && idx.Columns[0] == filterCol {
            return idx
        }
    }
    // Priority 3: Covering index (for full scan without filter)
    return FindCoveringIndex(indexes, requiredCols)
}
```

#### 1.3 Index-Only Scan Execution

```go
// internal/VM/exec.go

func (vm *VM) executeIndexScan(cursor *Cursor, requiredCols []string) error {
    // Check if this is a covering scan
    if cursor.Index != nil && cursor.Index.CoversColumns(requiredCols) {
        return vm.executeCoveringScan(cursor, requiredCols)
    }
    // Fall back to normal index + table lookup
    return vm.executeIndexWithLookup(cursor)
}

func (vm *VM) executeCoveringScan(cursor *Cursor, requiredCols []string) error {
    // Iterate index entries
    for cursor.IndexIterator.Next() {
        entry := cursor.IndexIterator.Entry()
        // Extract values directly from index entry
        row := make([]interface{}, len(requiredCols))
        for i, col := range requiredCols {
            row[i] = entry.GetValue(col)
        }
        vm.emitRow(row)
    }
    return nil
}
```

### Tasks

- [ ] Add `IndexMeta` struct with `CoversColumns` method to `internal/DS/index_engine.go`
- [ ] Add `FindCoveringIndex` and `SelectBestIndex` to `internal/QP/optimizer.go`
- [ ] Modify `OpOpenRead` to detect covering index opportunity
- [ ] Implement `executeCoveringScan` in VM
- [ ] Add `idx_covering` virtual column to track index entries
- [ ] Add benchmark comparing covering vs non-covering index

### Expected Impact

| Query Type | Before | After | Improvement |
|------------|--------|-------|-------------|
| SELECT indexed_col WHERE indexed_col = val | 2 lookups | 1 lookup | 30% faster |
| SELECT covering_cols WHERE filter_col = val | 2 lookups | 1 lookup | 30% faster |

---

## Phase 2: Column Projection (4h)

### Overview

Only fetch and materialize columns that are actually needed by the query, reducing memory allocation and data movement for wide tables.

**Example**:
```sql
-- Table with 50 columns
CREATE TABLE wide_table (col1, col2, ..., col50);

-- Query only needs 2 columns
SELECT col1, col5 FROM wide_table WHERE col10 = 'value';

-- Before: Fetch all 50 columns, project to 2
-- After: Fetch only col1, col5, col10 (for filter)
```

### Current Behavior

1. Scan fetches all columns from storage
2. WHERE filter applied on full rows
3. SELECT clause projects to required columns

### Optimized Behavior

1. Analyze query to determine required columns (SELECT + WHERE + ORDER BY + GROUP BY)
2. Pass required columns to storage layer
3. Storage layer returns only required columns
4. Reduced memory allocation and data movement

### Implementation

#### 2.1 Required Columns Analysis

```go
// internal/QP/analyzer.go

// RequiredColumns extracts all columns needed by a statement
func RequiredColumns(stmt *SelectStmt) []string {
    required := make(map[string]bool)
    
    // SELECT clause
    for _, col := range extractColumnRefs(stmt.Columns) {
        required[col] = true
    }
    
    // WHERE clause
    for _, col := range extractColumnRefsFromExpr(stmt.Where) {
        required[col] = true
    }
    
    // ORDER BY clause
    for _, col := range extractColumnRefsFromOrderBy(stmt.OrderBy) {
        required[col] = true
    }
    
    // GROUP BY clause
    for _, col := range stmt.GroupBy {
        required[col] = true
    }
    
    // JOIN conditions
    for _, join := range stmt.Joins {
        for _, col := range extractColumnRefsFromExpr(join.On) {
            required[col] = true
        }
    }
    
    return mapKeys(required)
}
```

#### 2.2 Storage Layer Projection

```go
// internal/DS/hybrid_store.go

// ScanProjected returns only the requested columns for each row
func (hs *HybridStore) ScanProjected(requiredCols []string) [][]Value {
    colIndices := make([]int, len(requiredCols))
    for i, col := range requiredCols {
        colIndices[i] = hs.ColIndex(col)
    }
    
    indices := hs.rowStore.ScanIndices()
    out := make([][]Value, 0, len(indices))
    
    for _, rowIdx := range indices {
        row := hs.rowStore.Get(rowIdx)
        vals := make([]Value, len(requiredCols))
        for i, colIdx := range colIndices {
            vals[i] = row.Get(colIdx)
        }
        out = append(out, vals)
    }
    return out
}

// ScanProjectedWhere returns filtered rows with only requested columns
func (hs *HybridStore) ScanProjectedWhere(colName string, val Value, requiredCols []string) [][]Value {
    // First, get row IDs from index or scan
    var rowIDs []uint32
    if hs.indexEngine.HasBitmapIndex(colName) {
        rb := hs.indexEngine.LookupEqual(colName, val)
        if rb != nil {
            rowIDs = rb.ToSlice()
        }
    }
    
    if rowIDs == nil {
        // Linear scan with early projection
        colIdx := hs.ColIndex(colName)
        for _, i := range hs.rowStore.ScanIndices() {
            row := hs.rowStore.Get(i)
            if row.Get(colIdx).Equal(val) {
                rowIDs = append(rowIDs, uint32(i))
            }
        }
    }
    
    // Project only required columns
    return hs.projectRows(rowIDs, requiredCols)
}
```

#### 2.3 ColumnVector Projection

```go
// internal/DS/column_vector.go

// Project returns a new ColumnVector with only the specified indices
func (cv *ColumnVector) Project(indices []int) *ColumnVector {
    result := NewColumnVector(cv.Name, cv.Type)
    result.nulls = make([]bool, len(indices))
    
    switch cv.Type {
    case TypeInt, TypeBool:
        result.ints = make([]int64, len(indices))
        for i, idx := range indices {
            result.nulls[i] = cv.nulls[idx]
            result.ints[i] = cv.ints[idx]
        }
    case TypeFloat:
        result.floats = make([]float64, len(indices))
        for i, idx := range indices {
            result.nulls[i] = cv.nulls[idx]
            result.floats[i] = cv.floats[idx]
        }
    case TypeString:
        result.strings = make([]string, len(indices))
        for i, idx := range indices {
            result.nulls[i] = cv.nulls[idx]
            result.strings[i] = cv.strings[idx]
        }
    }
    return result
}
```

### Tasks

- [ ] Create `internal/QP/analyzer.go` with `RequiredColumns` function
- [ ] Add `ScanProjected` and `ScanProjectedWhere` to `HybridStore`
- [ ] Modify `OpColumn` to use projected column data
- [ ] Update query execution to pass required columns to storage
- [ ] Add benchmark for wide-table queries

### Expected Impact

| Table Width | Query Width | Before | After | Improvement |
|-------------|-------------|--------|-------|-------------|
| 10 columns | 2 columns | 10 cols fetched | 2 cols fetched | 40% less memory |
| 50 columns | 3 columns | 50 cols fetched | 3 cols fetched | 60% less memory |
| 100 columns | 5 columns | 100 cols fetched | 5 cols fetched | 80% less memory |

---

## Phase 3: Index Skip Scan (8h)

### Overview

Allow index usage when the leading column(s) of a composite index are not constrained, by "skipping" through distinct values of the leading column.

**Example**:
```sql
CREATE INDEX idx_comp ON orders(region, status);

-- Query without constraint on leading column
SELECT * FROM orders WHERE status = 'pending';

-- Before: Full table scan (can't use index)
-- After: Skip scan - iterate distinct regions, use index for each
```

### Algorithm

For index `(A, B, C)` and query `WHERE B = val`:

1. Find all distinct values of A
2. For each distinct A value:
   - Use index to find rows where A = distinct_val AND B = val
   - Union all results

This is efficient when A has low cardinality (few distinct values).

### Implementation

#### 3.1 Skip Scan Detection

```go
// internal/QP/optimizer.go

// CanSkipScan checks if a skip scan is possible and cost-effective
func CanSkipScan(index *IndexMeta, filterCols []string, tableStats *TableStats) bool {
    // Skip scan requires:
    // 1. Index has more columns than filter columns
    // 2. Filter columns are a suffix of index columns
    // 3. Leading column has low cardinality
    
    if len(index.Columns) <= len(filterCols) {
        return false
    }
    
    // Check if filter columns are a suffix of index columns
    offset := len(index.Columns) - len(filterCols)
    for i, fc := range filterCols {
        if index.Columns[offset+i] != fc {
            return false
        }
    }
    
    // Check cardinality of leading column
    leadingCol := index.Columns[0]
    cardinality := tableStats.DistinctCount(leadingCol)
    rowCount := tableStats.RowCount
    
    // Skip scan is efficient when cardinality is low
    // Heuristic: cardinality < rowCount / 10
    return cardinality < rowCount/10 || cardinality < 100
}
```

#### 3.2 Skip Scan Execution

```go
// internal/DS/index_engine.go

// SkipScan performs a skip scan on a composite index
func (ie *IndexEngine) SkipScan(indexName string, filterCol string, filterVal Value) *RoaringBitmap {
    index := ie.getIndex(indexName)
    if index == nil {
        return nil
    }
    
    leadingCol := index.Columns[0]
    leadingColIdx := ie.getColumnIndex(leadingCol)
    
    // Get distinct values of leading column
    distinctVals := ie.getDistinctValues(leadingCol)
    
    result := NewRoaringBitmap()
    
    for _, distinctVal := range distinctVals {
        // Build composite key prefix
        prefix := buildCompositeKey([]Value{distinctVal, filterVal})
        
        // Lookup in index
        if rb := ie.lookupComposite(indexName, prefix); rb != nil {
            result.UnionInPlace(rb)
        }
    }
    
    return result
}

// getDistinctValues returns all distinct values for a column from its bitmap index
func (ie *IndexEngine) getDistinctValues(colName string) []Value {
    if !ie.hasBitmap[colName] {
        return nil
    }
    
    var vals []Value
    for key := range ie.bitmaps[colName] {
        vals = append(vals, ParseValue(key))
    }
    return vals
}
```

#### 3.3 Composite Index Support

```go
// internal/DS/index_engine.go

// CompositeIndex stores values for multiple columns
type CompositeIndex struct {
    Name    string
    Columns []string
    Tree    *SkipList  // Ordered by composite key
}

// CompositeKey represents a concatenated key for multi-column index
type CompositeKey struct {
    Values []Value
}

func (ck CompositeKey) Compare(other CompositeKey) int {
    for i := range ck.Values {
        cmp := Compare(ck.Values[i], other.Values[i])
        if cmp != 0 {
            return cmp
        }
    }
    return 0
}

func (ck CompositeKey) String() string {
    var parts []string
    for _, v := range ck.Values {
        parts = append(parts, v.String())
    }
    return strings.Join(parts, "\x00")
}
```

### Tasks

- [ ] Add `CompositeIndex` struct to `internal/DS/index_engine.go`
- [ ] Implement `SkipScan` method
- [ ] Add `CanSkipScan` to optimizer
- [ ] Modify index selection to consider skip scan
- [ ] Track column cardinality in `ANALYZE`
- [ ] Add benchmark for skip scan queries

### Expected Impact

| Query Pattern | Before | After | Improvement |
|---------------|--------|-------|-------------|
| WHERE col2 = val on index(col1, col2) | Full scan | Skip scan | 50% faster |
| WHERE col3 = val on index(col1, col2, col3) | Full scan | Skip scan | 40% faster |

---

## Phase 4: Slab Allocator (8h)

### Overview

Replace per-query allocations with a slab-based allocator that reuses memory across queries, reducing GC pressure and allocation overhead.

### Current Behavior

Each query allocates:
- Row buffers
- Result slices
- Intermediate computation values
- String values

All freed at end of query, causing GC pressure.

### Optimized Behavior

1. Pre-allocate large slabs of memory
2. Allocate from slabs using bump pointer
3. Reset slabs between queries (no GC)
4. Reuse slabs across queries

### Implementation

#### 4.1 Slab Allocator

```go
// internal/DS/slab.go

const (
    slabSize     = 64 * 1024  // 64 KB per slab
    maxSlabs     = 16         // Max 1 MB total per allocator
    smallSlab    = 4 * 1024   // 4 KB for small allocations
)

// SlabAllocator manages memory in fixed-size chunks
type SlabAllocator struct {
    slabs     [][]byte
    current   []byte
    offset    int
    smallPool *sync.Pool
    stats     SlabStats
}

type SlabStats struct {
    TotalAllocs   int64
    SlabHits      int64  // Allocations served from slab
    PoolHits      int64  // Allocations served from pool
    Misses        int64  // Allocations that went to heap
    BytesAllocated int64
}

// NewSlabAllocator creates a new slab allocator
func NewSlabAllocator() *SlabAllocator {
    return &SlabAllocator{
        slabs:   make([][]byte, 0, maxSlabs),
        current: make([]byte, slabSize),
        smallPool: &sync.Pool{
            New: func() interface{} {
                return make([]byte, smallSlab)
            },
        },
    }
}

// Alloc returns a slice of size bytes from the slab
func (sa *SlabAllocator) Alloc(size int) []byte {
    sa.stats.TotalAllocs++
    sa.stats.BytesAllocated += int64(size)
    
    // Small allocations use pool
    if size <= smallSlab/4 {
        buf := sa.smallPool.Get().([]byte)
        sa.stats.PoolHits++
        return buf[:size]
    }
    
    // Medium allocations use slab
    if sa.offset+size <= len(sa.current) {
        buf := sa.current[sa.offset : sa.offset+size]
        sa.offset += size
        sa.stats.SlabHits++
        return buf
    }
    
    // Large allocations or slab full: new slab
    if size > slabSize {
        sa.stats.Misses++
        return make([]byte, size)
    }
    
    // Allocate new slab
    if len(sa.slabs) >= maxSlabs-1 {
        // Reset to first slab if too many
        sa.Reset()
        sa.stats.Misses++
        return make([]byte, size)
    }
    
    newSlab := make([]byte, slabSize)
    sa.slabs = append(sa.slabs, sa.current)
    sa.current = newSlab
    sa.offset = size
    sa.stats.SlabHits++
    return newSlab[:size]
}

// Reset clears all slabs for reuse
func (sa *SlabAllocator) Reset() {
    sa.offset = 0
    // Keep slabs for reuse
}

// Release returns pooled memory
func (sa *SlabAllocator) Release(buf []byte) {
    if cap(buf) == smallSlab {
        sa.smallPool.Put(buf[:smallSlab])
    }
}
```

#### 4.2 Typed Allocators

```go
// internal/DS/slab.go

// AllocIntSlice allocates an int64 slice from the slab
func (sa *SlabAllocator) AllocIntSlice(n int) []int64 {
    size := n * 8
    buf := sa.Alloc(size)
    return unsafe.Slice((*int64)(unsafe.Pointer(&buf[0])), n)
}

// AllocFloatSlice allocates a float64 slice from the slab
func (sa *SlabAllocator) AllocFloatSlice(n int) []float64 {
    size := n * 8
    buf := sa.Alloc(size)
    return unsafe.Slice((*float64)(unsafe.Pointer(&buf[0])), n)
}

// AllocStringSlice allocates a string slice (pointers only)
func (sa *SlabAllocator) AllocStringSlice(n int) []string {
    size := n * 16  // string header size
    buf := sa.Alloc(size)
    return unsafe.Slice((*string)(unsafe.Pointer(&buf[0])), n)
}

// AllocInterfaceSlice allocates an interface{} slice
func (sa *SlabAllocator) AllocInterfaceSlice(n int) []interface{} {
    size := n * 16  // interface header size
    buf := sa.Alloc(size)
    return unsafe.Slice((*interface{})(unsafe.Pointer(&buf[0])), n)
}
```

#### 4.3 VM Integration

```go
// internal/VM/engine.go

type VM struct {
    // ... existing fields
    
    slab *DS.SlabAllocator  // Per-VM slab allocator
}

func NewVM(program *Program) *VM {
    return &VM{
        // ... existing initialization
        slab: DS.NewSlabAllocator(),
    }
}

// Reset clears VM state for query reuse
func (vm *VM) Reset() {
    vm.slab.Reset()
    // ... other reset logic
}
```

### Tasks

- [ ] Create `internal/DS/slab.go` with `SlabAllocator`
- [ ] Implement typed allocators (int, float, string, interface)
- [ ] Integrate slab allocator into VM
- [ ] Replace row buffer allocations with slab allocations
- [ ] Add allocator stats to `PRAGMA storage_info`
- [ ] Add benchmark showing reduced GC pressure

### Expected Impact

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Heap allocations per query | 1000+ | 10-50 | 95% reduction |
| GC pause time (10K queries) | 50ms | 5ms | 90% reduction |
| Memory fragmentation | High | Low | Better locality |

---

## Phase 5: Prepared Statement Pool (2h)

### Overview

Cache and reuse compiled query plans for parameterized queries, avoiding the overhead of tokenization, parsing, and code generation on repeated executions.

**TPC-C Relevance**: All TPC-C transactions use parameterized queries (e.g., `SELECT * FROM customer WHERE c_id = ?`). Reusing compiled plans provides significant throughput improvement.

**Example**:
```go
// Before: Each call parses and compiles
for i := 0; i < 1000; i++ {
    db.Query("SELECT balance FROM customer WHERE c_id = ?", i)
}

// After: Parse and compile once, execute 1000 times
stmt := db.Prepare("SELECT balance FROM customer WHERE c_id = ?")
for i := 0; i < 1000; i++ {
    stmt.Query(i)
}
```

### Current Behavior

1. Each `Query()` call tokenizes SQL
2. Parser builds AST
3. Compiler generates VM program
4. VM executes program

All steps repeated for identical SQL with different parameters.

### Optimized Behavior

1. First call: tokenize → parse → compile → cache program
2. Subsequent calls: lookup cache → bind parameters → execute

### Implementation

#### 5.1 Prepared Statement Interface

```go
// pkg/sqlvibe/statement.go

// PreparedStatement represents a compiled query ready for parameter binding
type PreparedStatement struct {
    sql       string
    program   *VM.Program
    db        *Database
    paramCount int
}

// Prepare compiles a SQL statement for repeated execution
func (db *Database) Prepare(sql string) (*PreparedStatement, error) {
    // Check plan cache first
    if prog, ok := db.planCache.Get(sql); ok {
        return &PreparedStatement{
            sql:     sql,
            program: prog,
            db:      db,
        }, nil
    }
    
    // Compile and cache
    prog, err := compile(sql)
    if err != nil {
        return nil, err
    }
    db.planCache.Put(sql, prog)
    
    return &PreparedStatement{
        sql:     sql,
        program: prog,
        db:      db,
    }, nil
}

// Bind sets parameter values for the prepared statement
func (stmt *PreparedStatement) Bind(args ...interface{}) error {
    // Bind parameters to VM registers
    for i, arg := range args {
        stmt.program.BindParam(i, arg)
    }
    return nil
}

// Query executes the prepared statement with current bindings
func (stmt *PreparedStatement) Query(args ...interface{}) ([][]interface{}, error) {
    if err := stmt.Bind(args...); err != nil {
        return nil, err
    }
    return stmt.db.executeProgram(stmt.program)
}

// Exec executes a prepared statement that doesn't return rows
func (stmt *PreparedStatement) Exec(args ...interface{}) (int64, error) {
    if err := stmt.Bind(args...); err != nil {
        return 0, err
    }
    return stmt.db.execProgram(stmt.program)
}

// Close releases resources
func (stmt *PreparedStatement) Close() error {
    stmt.program = nil
    return nil
}
```

#### 5.2 Statement Pool with LRU Eviction

```go
// pkg/sqlvibe/statement_pool.go

const defaultPoolSize = 100

// StatementPool manages a pool of prepared statements
type StatementPool struct {
    mu      sync.RWMutex
    stmts   map[string]*PreparedStatement
    lru     []string  // LRU order
    maxSize int
    db      *Database
}

// NewStatementPool creates a statement pool
func NewStatementPool(db *Database, maxSize int) *StatementPool {
    if maxSize <= 0 {
        maxSize = defaultPoolSize
    }
    return &StatementPool{
        stmts:   make(map[string]*PreparedStatement),
        lru:     make([]string, 0, maxSize),
        maxSize: maxSize,
        db:      db,
    }
}

// Get retrieves or creates a prepared statement
func (sp *StatementPool) Get(sql string) (*PreparedStatement, error) {
    sp.mu.RLock()
    if stmt, ok := sp.stmts[sql]; ok {
        sp.mu.RUnlock()
        // Move to front of LRU
        sp.touch(sql)
        return stmt, nil
    }
    sp.mu.RUnlock()
    
    sp.mu.Lock()
    defer sp.mu.Unlock()
    
    // Double-check after acquiring write lock
    if stmt, ok := sp.stmts[sql]; ok {
        return stmt, nil
    }
    
    // Evict LRU if full
    if len(sp.stmts) >= sp.maxSize {
        sp.evictLRU()
    }
    
    // Create new prepared statement
    stmt, err := sp.db.Prepare(sql)
    if err != nil {
        return nil, err
    }
    
    sp.stmts[sql] = stmt
    sp.lru = append(sp.lru, sql)
    return stmt, nil
}

func (sp *StatementPool) touch(sql string) {
    sp.mu.Lock()
    defer sp.mu.Unlock()
    sp.touchLocked(sql)
}

func (sp *StatementPool) touchLocked(sql string) {
    for i, s := range sp.lru {
        if s == sql {
            sp.lru = append(sp.lru[:i], sp.lru[i+1:]...)
            sp.lru = append(sp.lru, sql)
            return
        }
    }
}

func (sp *StatementPool) evictLRU() {
    if len(sp.lru) == 0 {
        return
    }
    oldest := sp.lru[0]
    if stmt, ok := sp.stmts[oldest]; ok {
        stmt.Close()
    }
    delete(sp.stmts, oldest)
    sp.lru = sp.lru[1:]
}

// Clear removes all cached statements
func (sp *StatementPool) Clear() {
    sp.mu.Lock()
    defer sp.mu.Unlock()
    for _, stmt := range sp.stmts {
        stmt.Close()
    }
    sp.stmts = make(map[string]*PreparedStatement)
    sp.lru = sp.lru[:0]
}
```

#### 5.3 Parameter Binding in VM

```go
// internal/VM/program.go

// BindParam sets a parameter value at the given index
func (p *Program) BindParam(index int, value interface{}) {
    // Parameters are stored in reserved registers starting at P1 of first instruction
    // Or use a dedicated parameter array
    if p.params == nil {
        p.params = make([]interface{}, p.paramCount)
    }
    if index >= 0 && index < len(p.params) {
        p.params[index] = value
    }
}

// GetParam retrieves a parameter value
func (p *Program) GetParam(index int) interface{} {
    if p.params != nil && index >= 0 && index < len(p.params) {
        return p.params[index]
    }
    return nil
}
```

### Tasks

- [ ] Create `pkg/sqlvibe/statement.go` with `PreparedStatement` struct
- [ ] Create `pkg/sqlvibe/statement_pool.go` with `StatementPool`
- [ ] Add `Prepare()` method to `Database`
- [ ] Add parameter binding support to VM
- [ ] Add `?` placeholder parsing in tokenizer
- [ ] Add benchmark for prepared vs non-prepared queries
- [ ] Update Database API documentation

### Expected Impact

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Parse + compile overhead per query | ~50 µs | 0 µs (cached) | 100% reduction |
| TPC-C throughput (queries/sec) | Baseline | +20-30% | For parameterized queries |
| Memory per repeated query | Full AST | Cached program only | 50% reduction |

### TPC-C Benchmark Scenario

```go
// internal/TS/Benchmark/benchmark_tpc_c_test.go

func BenchmarkPaymentTransaction(b *testing.B) {
    db := setupTPCCDatabase(b)
    defer db.Close()
    
    // TPC-C Payment transaction
    sql := `UPDATE customer SET c_balance = c_balance - ? 
            WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
    
    // With prepared statement
    stmt := db.Prepare(sql)
    defer stmt.Close()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        stmt.Exec(
            float64(i%1000),  // amount
            i%10,             // w_id
            i%10,             // d_id
            i%3000,           // c_id
        )
    }
}

func BenchmarkNewOrderTransaction(b *testing.B) {
    // New-Order inserts multiple rows - test batch insert performance
}
```

---

## Phase 6: Direct Threaded VM (4h)

### Overview

Replace the interpreter's switch-based dispatch with direct function calls via a dispatch table. This eliminates branch misprediction in the main execution loop.

**Current Problem**:
```go
// Current: switch dispatch (branch misprediction)
for {
    inst := program[pc]
    switch inst.Op {
    case OpAdd:
        // ...
    case OpSub:
        // ...
    // 200+ cases...
    }
    pc++
}
```

**Optimized Solution**:
```go
// Direct threaded: function pointer dispatch
var dispatchTable = [256]func(*VM, *Instruction){
    OpAdd:    execOpAdd,
    OpSub:    execOpSub,
    // ... one function per opcode
}

for pc < len(program) {
    inst := &program[pc]
    dispatchTable[inst.Op](vm, inst)
    pc++
}
```

### Implementation

#### 6.1 Dispatch Table

```go
// internal/VM/dispatch.go

type OpHandler func(*VM, *Instruction) (advance bool)

var dispatchTable [256]OpHandler

func init() {
    // Control flow
    dispatchTable[OpNull] = execNull
    dispatchTable[OpGoto] = execGoto
    dispatchTable[OpGosub] = execGosub
    dispatchTable[OpReturn] = execReturn
    dispatchTable[OpInit] = execInit
    
    // Arithmetic
    dispatchTable[OpAdd] = execAdd
    dispatchTable[OpSubtract] = execSubtract
    dispatchTable[OpMultiply] = execMultiply
    dispatchTable[OpDivide] = execDivide
    
    // Comparison
    dispatchTable[OpEq] = execEq
    dispatchTable[OpNe] = execNe
    dispatchTable[OpLt] = execLt
    dispatchTable[OpLe] = execLe
    dispatchTable[OpGt] = execGt
    dispatchTable[OpGe] = execGe
    
    // ... all other opcodes
}

// Individual handlers return true to advance PC, false if handler modified PC
func execAdd(vm *VM, inst *Instruction) bool {
    lhs := vm.registers[inst.P1]
    rhs := vm.registers[inst.P2]
    dst := inst.P4.(int)
    
    l, lok := toFloat64(lhs)
    r, rok := toFloat64(rhs)
    if lok && rok {
        vm.registers[dst] = l + r
    }
    return true
}

func execGoto(vm *VM, inst *Instruction) bool {
    vm.pc = int(inst.P2)
    return false  // Don't auto-advance PC
}
```

#### 6.2 New Execution Loop

```go
// internal/VM/engine.go

func (vm *VM) Exec(ctx interface{}) error {
    vm.Reset()
    
    for vm.pc < len(vm.program.Instructions) {
        inst := &vm.program.Instructions[vm.pc]
        
        handler := dispatchTable[inst.Op]
        if handler == nil {
            return fmt.Errorf("unknown opcode: %d", inst.Op)
        }
        
        if handler(vm, inst) {
            vm.pc++
        }
        // If handler returned false, it already set vm.pc
    }
    
    return vm.err
}
```

#### 6.3 Inline Critical Handlers

```go
// For hot-path opcodes, use inline-able functions

//go:noinline
func execColumn(vm *VM, inst *Instruction) bool {
    cursor := vm.cursors.Get(int(inst.P1))
    if cursor == nil || !cursor.Valid {
        vm.registers[inst.P3] = nil
        return true
    }
    
    colIdx := int(inst.P2)
    row := cursor.CurrentRow()
    if colIdx < len(row) {
        vm.registers[inst.P3] = row[colIdx]
    } else {
        vm.registers[inst.P3] = nil
    }
    return true
}
```

### Tasks

- [ ] Create `internal/VM/dispatch.go` with dispatch table
- [ ] Extract all opcode handlers to individual functions
- [ ] Refactor `Exec()` to use dispatch table
- [ ] Benchmark dispatch vs switch performance
- [ ] Ensure all tests pass

### Expected Impact

| Metric | Before (switch) | After (dispatch) | Improvement |
|--------|-----------------|------------------|-------------|
| Branch misprediction rate | ~15% | ~3% | 80% reduction |
| Instructions per opcode | ~10 | ~6 | 40% faster |
| Overall VM throughput | Baseline | 2-3x faster | 200-300% |

---

## Phase 7: Query Compilation Pipeline (8h)

### Overview

Integrate QP (Parser) and CG (Compiler) into a single-pass compilation pipeline, eliminating intermediate AST construction for common query patterns.

**Current Flow**:
```
SQL → []Token → AST → Program
     Tokenize   Parse  Compile
     (alloc)    (alloc) (traverse)
```

**Optimized Flow**:
```
SQL → Program
     DirectCompile
     (single pass, minimal alloc)
```

### Implementation

#### 7.1 Direct Compiler Structure

```go
// internal/CG/direct_compiler.go

type DirectCompiler struct {
    program     *VM.Program
    ra          *VM.RegisterAllocator
    lexer       *QP.Lexer
    current     QP.Token
    tables      map[string]*TableInfo
    columnIdx   map[string]int
    cursorMap   map[string]int
    nextCursor  int
}

func NewDirectCompiler() *DirectCompiler {
    return &DirectCompiler{
        program:    VM.NewProgram(),
        ra:         VM.NewRegisterAllocator(16),
        tables:     make(map[string]*TableInfo),
        columnIdx:  make(map[string]int),
        cursorMap:  make(map[string]int),
        nextCursor: 0,
    }
}

func (dc *DirectCompiler) Compile(sql string) (*VM.Program, error) {
    dc.lexer = QP.NewLexer(sql)
    dc.advance()
    
    switch dc.current.Type {
    case QP.TokenSelect:
        return dc.compileSelect()
    case QP.TokenInsert:
        return dc.compileInsert()
    case QP.TokenUpdate:
        return dc.compileUpdate()
    case QP.TokenDelete:
        return dc.compileDelete()
    default:
        return nil, fmt.Errorf("unexpected token: %v", dc.current)
    }
}
```

#### 7.2 Single-Pass SELECT Compilation

```go
func (dc *DirectCompiler) compileSelect() (*VM.Program, error) {
    dc.advance() // consume SELECT
    
    dc.program.Emit(VM.OpInit)
    initJump := dc.program.Emit(VM.OpGoto)
    
    // Parse and compile column list
    var colRegs []int
    for {
        reg, err := dc.compileColumnExpr()
        if err != nil {
            return nil, err
        }
        colRegs = append(colRegs, reg)
        
        if dc.current.Type != QP.TokenComma {
            break
        }
        dc.advance()
    }
    
    // FROM clause
    if dc.current.Type != QP.TokenFrom {
        return nil, fmt.Errorf("expected FROM")
    }
    dc.advance()
    
    tableName := dc.current.Value.(string)
    dc.advance()
    
    cursorID := dc.nextCursor
    dc.cursorMap[tableName] = cursorID
    dc.nextCursor++
    
    dc.program.EmitOpenTable(cursorID, tableName)
    rewindPos := len(dc.program.Instructions)
    dc.program.Emit(VM.OpRewind, cursorID, 0)
    
    // WHERE clause
    if dc.current.Type == QP.TokenWhere {
        dc.advance()
        if err := dc.compileWhereInline(); err != nil {
            return nil, err
        }
    }
    
    // Result row
    dc.program.EmitResultRow(colRegs)
    
    // Loop back
    dc.program.Emit(VM.OpNext, cursorID, 0)
    haltPos := len(dc.program.Instructions)
    dc.program.Emit(VM.OpHalt)
    
    // Fixup jumps
    dc.program.FixupWithPos(initJump, rewindPos+1)
    dc.program.FixupWithPos(rewindPos, haltPos)
    
    return dc.program, nil
}

func (dc *DirectCompiler) compileWhereInline() error {
    // Direct compilation: no AST construction
    leftCol := dc.current.Value.(string)
    dc.advance()
    
    op := dc.current.Type
    dc.advance()
    
    rightVal := dc.current.Value
    dc.advance()
    
    // Emit opcodes directly
    lreg := dc.ra.Alloc()
    rreg := dc.ra.Alloc()
    
    dc.program.Emit(VM.OpColumn, 0, dc.columnIdx[leftCol], lreg)
    dc.program.EmitLoadConst(rreg, rightVal)
    
    skipPos := dc.program.Emit(VM.OpEq, lreg, rreg, 0)
    dc.program.MarkFixup(skipPos)
    
    return nil
}
```

#### 7.3 Hybrid Approach (Fallback to AST)

```go
func (dc *DirectCompiler) Compile(sql string) (*VM.Program, error) {
    // Try fast path first
    if dc.canFastPath(sql) {
        prog, err := dc.compileDirect(sql)
        if err == nil {
            return prog, nil
        }
        // Fall back to AST path on error
    }
    
    // Standard AST path for complex queries
    tokens := QP.Tokenize(sql)
    ast := QP.Parse(tokens)
    return CG.NewCompiler().Compile(ast), nil
}

func (dc *DirectCompiler) canFastPath(sql string) bool {
    // Fast path for simple queries:
    // - Single table SELECT
    // - No subqueries
    // - No CTEs
    // - No window functions
    // - Simple WHERE predicates
    return dc.hasSimplePattern(sql)
}
```

### Tasks

- [ ] Create `internal/CG/direct_compiler.go`
- [ ] Implement single-pass SELECT compilation
- [ ] Implement single-pass INSERT/UPDATE/DELETE
- [ ] Add fast-path detection (`canFastPath`)
- [ ] Integrate with existing Database API
- [ ] Benchmark direct vs AST compilation
- [ ] Ensure all tests pass

### Expected Impact

| Metric | Before (AST) | After (Direct) | Improvement |
|--------|--------------|----------------|-------------|
| Compilation allocations | ~50 objects | ~5 objects | 90% reduction |
| Compilation time (simple query) | ~50 µs | ~10 µs | 5x faster |
| Compilation time (complex query) | Baseline | Same | N/A (fallback) |

---

## Phase 8: Expression Bytecode (6h)

### Overview

Compile complex expressions into a compact bytecode format that can be evaluated efficiently, with optional vectorized execution for columnar data.

**Current Problem**:
```go
// Each expression node becomes a separate VM opcode
// a + b * c - d  →  OpMul + OpAdd + OpSub (3 opcodes, 3 dispatches)
```

**Optimized Solution**:
```go
// Compile expression to bytecode, evaluate in one call
// a + b * c - d  →  ExprBytecode{ops: [Load, Load, Load, Load, Mul, Add, Sub]}
```

### Implementation

#### 8.1 Expression Bytecode Structure

```go
// internal/VM/expr_bytecode.go

type ExprOp uint8

const (
    EOpNop ExprOp = iota
    
    // Load operations
    EOpLoadColumn    // Load column value
    EOpLoadConst     // Load constant
    EOpLoadParam     // Load parameter (for prepared statements)
    
    // Arithmetic
    EOpAdd
    EOpSub
    EOpMul
    EOpDiv
    EOpMod
    
    // Comparison
    EOpEq
    EOpNe
    EOpLt
    EOpLe
    EOpGt
    EOpGe
    
    // Logical
    EOpAnd
    EOpOr
    EOpNot
    
    // Functions
    EOpFunc1  // Unary function
    EOpFunc2  // Binary function
)

type ExprBytecode struct {
    ops      []ExprOp
    args     []int16   // Column indices, const indices, param indices
    consts   []interface{}
    funcIdx  []int     // Function indices for EOpFunc*
}

func NewExprBytecode() *ExprBytecode {
    return &ExprBytecode{
        ops:    make([]ExprOp, 0, 16),
        args:   make([]int16, 0, 32),
        consts: make([]interface{}, 0, 8),
    }
}

func (eb *ExprBytecode) Emit(op ExprOp, args ...int16) {
    eb.ops = append(eb.ops, op)
    eb.args = append(eb.args, args...)
}

func (eb *ExprBytecode) AddConst(v interface{}) int16 {
    eb.consts = append(eb.consts, v)
    return int16(len(eb.consts) - 1)
}
```

#### 8.2 Expression Compiler

```go
// internal/CG/expr_compiler.go

type ExprCompiler struct {
    bytecode   *VM.ExprBytecode
    colIndices map[string]int
}

func CompileExpr(expr QP.Expr, colIndices map[string]int) *VM.ExprBytecode {
    ec := &ExprCompiler{
        bytecode:   VM.NewExprBytecode(),
        colIndices: colIndices,
    }
    ec.compile(expr)
    return ec.bytecode
}

func (ec *ExprCompiler) compile(expr QP.Expr) {
    switch e := expr.(type) {
    case *QP.ColumnRef:
        idx := ec.colIndices[e.Name]
        ec.bytecode.Emit(VM.EOpLoadColumn, int16(idx))
        
    case *QP.Literal:
        idx := ec.bytecode.AddConst(e.Value)
        ec.bytecode.Emit(VM.EOpLoadConst, idx)
        
    case *QP.BinaryExpr:
        ec.compile(e.Left)
        ec.compile(e.Right)
        
        switch e.Op {
        case QP.TokenPlus:
            ec.bytecode.Emit(VM.EOpAdd)
        case QP.TokenMinus:
            ec.bytecode.Emit(VM.EOpSub)
        case QP.TokenStar:
            ec.bytecode.Emit(VM.EOpMul)
        case QP.TokenSlash:
            ec.bytecode.Emit(VM.EOpDiv)
        case QP.TokenEq:
            ec.bytecode.Emit(VM.EOpEq)
        // ... other operators
        }
        
    case *QP.FuncCall:
        // Compile arguments first
        for _, arg := range e.Args {
            ec.compile(arg)
        }
        funcIdx := getFuncIndex(e.Name)
        ec.bytecode.Emit(VM.EOpFunc2, int16(funcIdx), int16(len(e.Args)))
    }
}
```

#### 8.3 Bytecode Evaluator

```go
// internal/VM/expr_eval.go

func (eb *ExprBytecode) Eval(row []interface{}) interface{} {
    var stack []interface{}
    argIdx := 0
    
    for _, op := range eb.ops {
        switch op {
        case EOpLoadColumn:
            colIdx := eb.args[argIdx]
            argIdx++
            stack = append(stack, row[colIdx])
            
        case EOpLoadConst:
            constIdx := eb.args[argIdx]
            argIdx++
            stack = append(stack, eb.consts[constIdx])
            
        case EOpAdd:
            b := stack[len(stack)-1]
            a := stack[len(stack)-2]
            stack = stack[:len(stack)-1]
            stack[len(stack)-1] = addValues(a, b)
            
        case EOpMul:
            b := stack[len(stack)-1]
            a := stack[len(stack)-2]
            stack = stack[:len(stack)-1]
            stack[len(stack)-1] = mulValues(a, b)
            
        case EOpEq:
            b := stack[len(stack)-1]
            a := stack[len(stack)-2]
            stack = stack[:len(stack)-1]
            if compareValues(a, b) == 0 {
                stack[len(stack)-1] = int64(1)
            } else {
                stack[len(stack)-1] = int64(0)
            }
            
        // ... other operations
        }
    }
    
    if len(stack) > 0 {
        return stack[0]
    }
    return nil
}
```

#### 8.4 Vectorized Evaluation

```go
// internal/VM/expr_vectorized.go

func (eb *ExprBytecode) EvalVectorized(cols []*DS.ColumnVector) []interface{} {
    n := cols[0].Len()
    results := make([]interface{}, n)
    
    // Stack for each row
    stacks := make([][]interface{}, n)
    for i := range stacks {
        stacks[i] = make([]interface{}, 0, 8)
    }
    
    argIdx := 0
    for _, op := range eb.ops {
        switch op {
        case EOpLoadColumn:
            colIdx := eb.args[argIdx]
            argIdx++
            col := cols[colIdx]
            for i := 0; i < n; i++ {
                stacks[i] = append(stacks[i], col.Get(i))
            }
            
        case EOpLoadConst:
            constIdx := eb.args[argIdx]
            argIdx++
            c := eb.consts[constIdx]
            for i := 0; i < n; i++ {
                stacks[i] = append(stacks[i], c)
            }
            
        case EOpAdd:
            for i := 0; i < n; i++ {
                s := &stacks[i]
                b := (*s)[len(*s)-1]
                a := (*s)[len(*s)-2]
                *s = (*s)[:len(*s)-1]
                (*s)[len(*s)-1] = addValues(a, b)
            }
            
        // ... other operations
        }
    }
    
    for i := 0; i < n; i++ {
        results[i] = stacks[i][0]
    }
    
    return results
}

// SIMD-optimized path for simple integer expressions
func (eb *ExprBytecode) EvalInt64SIMD(cols []*DS.ColumnVector) []int64 {
    n := cols[0].Len()
    results := make([]int64, n)
    
    // Detect simple pattern: colA op colB or colA op const
    if len(eb.ops) == 3 && eb.ops[0] == EOpLoadColumn && eb.ops[1] == EOpLoadColumn {
        colA := cols[eb.args[0]].Ints()
        colB := cols[eb.args[1]].Ints()
        
        switch eb.ops[2] {
        case EOpAdd:
            for i := 0; i < n; i++ {
                results[i] = colA[i] + colB[i]
            }
        case EOpMul:
            for i := 0; i < n; i++ {
                results[i] = colA[i] * colB[i]
            }
        case EOpSub:
            for i := 0; i < n; i++ {
                results[i] = colA[i] - colB[i]
            }
        }
        return results
    }
    
    // Fall back to general evaluation
    res := eb.EvalVectorized(cols)
    for i, v := range res {
        results[i] = v.(int64)
    }
    return results
}
```

#### 8.5 VM Integration

```go
// New VM opcode for expression bytecode
case VM.OpExprEval:
    eb := inst.P4.(*ExprBytecode)
    row := vm.getCurrentRow()
    result := eb.Eval(row)
    vm.registers[inst.P1] = result
```

### Tasks

- [ ] Create `internal/VM/expr_bytecode.go` with bytecode structure
- [ ] Create `internal/CG/expr_compiler.go` with expression compiler
- [ ] Create `internal/VM/expr_eval.go` with evaluator
- [ ] Create `internal/VM/expr_vectorized.go` with vectorized evaluation
- [ ] Add `OpExprEval` opcode to VM
- [ ] Modify CG to use expression bytecode for complex expressions
- [ ] Add benchmark comparing bytecode vs individual opcodes
- [ ] Ensure all tests pass

### Expected Impact

| Expression Type | Before (Opcodes) | After (Bytecode) | Improvement |
|-----------------|------------------|------------------|-------------|
| Simple: `a + b` | 3 opcodes | 1 bytecode eval | 20% faster |
| Medium: `a + b * c` | 5 opcodes | 1 bytecode eval | 40% faster |
| Complex: `a*b + c*d - e/f` | 9 opcodes | 1 bytecode eval | 60% faster |
| Vectorized (1000 rows) | 1000x dispatch | 1 vectorized call | 200% faster |

---

## Phase 9: Testing & Benchmarks (4h)

### Test Files

```
internal/TS/Benchmark/
├── benchmark_v0.9.1_test.go    # New benchmarks for v0.9.1 features
├── benchmark_compare_test.go   # SQLite comparison benchmarks
├── benchmark_tpc_c_test.go     # TPC-C style OLTP benchmarks
├── benchmark_dispatch_test.go  # Direct threaded VM benchmarks
├── benchmark_pipeline_test.go  # Query compilation pipeline benchmarks
└── benchmark_bytecode_test.go  # Expression bytecode benchmarks

internal/TS/SQL1999/F873/       # New test suite for optimization features
├── 01_covering_index_test.go
├── 02_column_projection_test.go
├── 03_skip_scan_test.go
├── 04_slab_allocator_test.go
├── 05_prepared_statement_test.go
├── 06_direct_threaded_test.go
├── 07_query_pipeline_test.go
└── 08_expr_bytecode_test.go
```

### Benchmarks

```go
// internal/TS/Benchmark/benchmark_v0.9.1_test.go

func BenchmarkCoveringIndex(b *testing.B) {
    // Table with 10 columns, index on (col1, col2)
    // Query: SELECT col2 FROM t WHERE col1 = val
    // Compare: covering index vs table lookup
}

func BenchmarkColumnProjection(b *testing.B) {
    // Wide table with 50 columns
    // Query: SELECT col1, col2 FROM t
    // Compare: full projection vs selective projection
}

func BenchmarkSkipScan(b *testing.B) {
    // Index on (region, status), 10 distinct regions
    // Query: SELECT * FROM t WHERE status = 'pending'
    // Compare: skip scan vs full scan
}

func BenchmarkSlabAllocator(b *testing.B) {
    // Measure GC pressure with/without slab allocator
    // 10K queries, track allocation count and GC time
}

func BenchmarkPreparedStatement(b *testing.B) {
    // Compare prepared vs ad-hoc queries
    // Query: SELECT * FROM t WHERE id = ?
    // 1000 iterations, measure parse + compile overhead
}

func BenchmarkDirectThreadedVM(b *testing.B) {
    // Compare switch dispatch vs function pointer dispatch
    // Run same program 10K times, measure throughput
}

func BenchmarkQueryPipeline(b *testing.B) {
    // Compare AST-based vs direct compilation
    // Simple SELECT, measure compile time
}

func BenchmarkExprBytecode(b *testing.B) {
    // Compare individual opcodes vs bytecode evaluation
    // Complex expression: a*b + c*d - e/f
}
```

### Tasks

- [ ] Create benchmark tests for each optimization
- [ ] Create SQL1999 F873 test suite
- [ ] Compare performance against SQLite
- [ ] Update README with new benchmark results
- [ ] Update docs/HISTORY.md

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | Covering Index | 6 |
| 2 | Column Projection | 4 |
| 3 | Index Skip Scan | 8 |
| 4 | Slab Allocator | 8 |
| 5 | Prepared Statement Pool | 2 |
| 6 | Direct Threaded VM | 4 |
| 7 | Query Compilation Pipeline | 8 |
| 8 | Expression Bytecode | 6 |
| 9 | Testing & Benchmarks | 4 |

**Total:** ~50 hours

---

## Success Criteria

### Phase 1: Covering Index

| Criteria | Target | Status |
|----------|--------|--------|
| IndexMeta.CoversColumns | Works | [ ] |
| Optimizer selects covering index | Works | [ ] |
| Index-only scan execution | Works | [ ] |
| Benchmark shows improvement | 30% faster | [ ] |

### Phase 2: Column Projection

| Criteria | Target | Status |
|----------|--------|--------|
| RequiredColumns analyzer | Works | [ ] |
| ScanProjected storage API | Works | [ ] |
| Wide-table query improvement | 40-60% less memory | [ ] |

### Phase 3: Index Skip Scan

| Criteria | Target | Status |
|----------|--------|--------|
| SkipScan implementation | Works | [ ] |
| CanSkipScan cost estimation | Works | [ ] |
| Benchmark shows improvement | 40-50% faster | [ ] |

### Phase 4: Slab Allocator

| Criteria | Target | Status |
|----------|--------|--------|
| SlabAllocator implementation | Works | [ ] |
| VM integration | Works | [ ] |
| GC pressure reduction | 90% less | [ ] |

### Phase 5: Prepared Statement Pool

| Criteria | Target | Status |
|----------|--------|--------|
| PreparedStatement implementation | Works | [ ] |
| StatementPool with LRU eviction | Works | [ ] |
| Parameter binding (? placeholder) | Works | [ ] |
| Benchmark shows improvement | 20-30% faster | [ ] |

### Phase 6: Direct Threaded VM

| Criteria | Target | Status |
|----------|--------|--------|
| Dispatch table implementation | Works | [ ] |
| All opcode handlers extracted | Works | [ ] |
| New execution loop | Works | [ ] |
| Benchmark shows improvement | 2-3x faster | [ ] |

### Phase 7: Query Compilation Pipeline

| Criteria | Target | Status |
|----------|--------|--------|
| DirectCompiler implementation | Works | [ ] |
| Single-pass SELECT compilation | Works | [ ] |
| Fast-path detection | Works | [ ] |
| Benchmark shows improvement | 30% compile time | [ ] |

### Phase 8: Expression Bytecode

| Criteria | Target | Status |
|----------|--------|--------|
| ExprBytecode structure | Works | [ ] |
| Expression compiler | Works | [ ] |
| Vectorized evaluation | Works | [ ] |
| Benchmark shows improvement | 40% for complex expr | [ ] |

### Phase 9: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| All benchmark tests pass | 100% | [ ] |
| SQL1999 F873 suite passes | 100% | [ ] |
| README updated | Done | [ ] |
| HISTORY.md updated | Done | [ ] |

---

## Expected Results After v0.9.1

### Storage & Index Layer

| Operation | Before (v0.9.0) | After (v0.9.1) | Improvement |
|-----------|-----------------|----------------|-------------|
| SELECT indexed_col WHERE indexed_col = val | 2 lookups | 1 lookup | 30% faster |
| SELECT 2 cols FROM 50-col table | 50 cols fetched | 2 cols fetched | 60% less memory |
| WHERE col2=val on index(col1,col2) | Full scan | Skip scan | 50% faster |

### Memory Management

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| 10K queries GC time | 50ms | 5ms | 90% reduction |
| Per-query allocations | ~50 objects | ~5 objects | 90% reduction |

### Query Interface

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Repeated parameterized query | Parse + compile each | Cached program | 20-30% faster |

### Fundamental Optimizations (QP+CG+VM)

| Optimization | Before | After | Improvement |
|--------------|--------|-------|-------------|
| VM dispatch (switch) | ~15% branch miss | ~3% branch miss | 2-3x throughput |
| Query compilation | 3-pass (tokenize/parse/compile) | 1-pass direct | 30% faster compile |
| Complex expression eval | N opcodes + N dispatches | 1 bytecode eval | 40-60% faster |
| Vectorized expression | N/A | Batch evaluation | 200% faster |

---

## TPC-C Expected Performance

| Transaction Type | Key Optimizations Applied | Expected Improvement |
|------------------|---------------------------|---------------------|
| New-Order (45%) | Slab Allocator, Prepared Statement, Direct Threaded VM | +30-40% throughput |
| Payment (43%) | Covering Index, Prepared Statement, Expression Bytecode | +35-45% throughput |
| Order-Status (4%) | Covering Index, Column Projection, Direct Threaded VM | +40% latency |
| Delivery (4%) | Column Projection, Expression Bytecode | +25% throughput |
| Stock-Level (4%) | Index Skip Scan, Column Projection, Query Pipeline | +50% latency |

---

## Implementation Priority

### High Priority (Core Performance)
1. **Direct Threaded VM** - Maximum impact on all queries
2. **Slab Allocator** - Essential for OLTP throughput
3. **Prepared Statement Pool** - Required for TPC-C

### Medium Priority (Targeted Improvements)
4. **Covering Index** - Point query optimization
5. **Expression Bytecode** - Complex expression speedup
6. **Column Projection** - Memory efficiency

### Lower Priority (Specialized)
7. **Index Skip Scan** - Specific query patterns
8. **Query Compilation Pipeline** - Compile-time optimization

---

## Notes

- All optimizations are compatible with existing query semantics
- No breaking changes to public API
- Optimizations are transparent to users
- Benchmarks use same methodology as v0.9.0 (cache bypass, SQLite comparison)
- TPC-C benchmarks simulate OLTP workload patterns
- F2/F3/F4 fundamental optimizations can be enabled incrementally
- Hybrid approach for F3: fast path for simple queries, AST fallback for complex ones
