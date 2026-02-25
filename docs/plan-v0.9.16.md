# Plan v0.9.16 - Performance Optimization

## Summary

This version focuses entirely on **performance optimization** to close the gaps where SQLite
is still faster. The goal is to achieve parity or better on WHERE filtering (currently 3× slower)
and JOIN operations (currently 2.4× slower).

---

## Background

### Current Performance (v0.9.6, AMD EPYC 7763)

| Benchmark | sqlvibe | SQLite | Winner |
|-----------|--------|--------|--------|
| SELECT all (1K rows) | 60 µs | 571 µs | **sqlvibe 9.5×** |
| SUM aggregate | 19 µs | 66 µs | **sqlvibe 3.5×** |
| GROUP BY | 135 µs | 499 µs | **sqlvibe 3.7×** |
| ORDER BY | 197 µs | 299 µs | **sqlvibe 1.5×** |
| Result cache hit | 1.5 µs | N/A | **sqlvibe** |
| SELECT WHERE | 285 µs | 91 µs | **SQLite 3.1×** |
| JOIN (100×500) | 559 µs | 230 µs | **SQLite 2.4×** |

### Root Cause Analysis

| Gap | Root Cause |
|-----|------------|
| WHERE slower | Full table scan with per-row map allocation; predicate pushdown only handles simple cases; no index-only scan |
| JOIN slower | Hash join builds map with `[][]DS.Value` slices; allocates merged row per match; no bloom filter pre-filtering |

### Existing Optimizations (v0.9.0–v0.9.3)

- Predicate pushdown (Go-layer filter before VM)
- SIMD vectorized sum/min/max for int64/float64
- Dispatch table for common opcodes (arithmetic, string, comparison)
- Early termination for LIMIT without ORDER BY
- Result cache + plan cache
- Pre-sized result slices

---

## Performance Targets

| Benchmark | Current | Target | Improvement |
|-----------|---------|--------|-------------|
| SELECT WHERE | 285 µs | ≤ 80 µs | 3.5× faster |
| JOIN (100×500) | 559 µs | ≤ 200 µs | 2.8× faster |
| SELECT all (1K) | 60 µs | ≤ 55 µs | maintain |
| GROUP BY | 135 µs | ≤ 120 µs | 12% faster |

---

## Track A: Index-Only Scan (Covering Index)

### A1. Detect Covering Index in Query Planner

Extend `internal/QP/optimizer.go`:

```go
// FindCoveringIndexForColumns finds an index that contains all required columns.
// Returns the index name and whether it covers the filter column as the leading column.
func FindCoveringIndexForColumns(indexes []*IndexMetaQP, required []string, filterCol string) (name string, filterFirst bool)
```

**Logic**:
1. Extract required columns from SELECT, WHERE, ORDER BY
2. Check if any index covers all required columns
3. Prefer index where filter column is the leading column

### A2. Implement Index-Only Scan in Query Engine

Extend `pkg/sqlvibe/database.go`:

```go
// execIndexOnlyScan reads rows directly from the index without table lookup.
// Only valid when the index covers all required columns.
func (db *Database) execIndexOnlyScan(table, indexName string, requiredCols []string, filterExpr Expr) ([]map[string]interface{}, error)
```

**Key changes**:
- Store index data as `map[string]map[string]interface{}` (key → full row)
- When covering index detected, read from index instead of table
- Eliminates the PK → table lookup step

### A3. Benchmark

```go
func BenchmarkFair_IndexOnlyScan(b *testing.B) {
    // CREATE TABLE t (id INT, name TEXT, score INT)
    // CREATE INDEX idx_covering ON t(id, name, score)
    // SELECT name, score FROM t WHERE id = 500
    // Expected: ~10 µs (direct index lookup, no table scan)
}
```

---

## Track B: Bloom Filter for Hash Join

### B1. Bloom Filter Implementation

Add `internal/DS/bloom_filter.go`:

```go
type BloomFilter struct {
    bits    []uint64
    k       int     // number of hash functions
    n       int     // number of elements
}

func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter
func (bf *BloomFilter) Add(key interface{})
func (bf *BloomFilter) MightContain(key interface{}) bool
```

**Parameters**:
- Use 2 hash functions (k=2) for simplicity
- Size = ceil(-n * ln(p) / ln(2)^2) bits
- Hash: use FNV-1a with different seeds for k hashes

### B2. Integrate Bloom Filter into Hash Join

Modify `pkg/sqlvibe/exec_columnar.go`:

```go
func ColumnarHashJoinBloom(left, right *DS.HybridStore, leftCol, rightCol string) [][]DS.Value {
    // Build phase: insert all right keys into bloom filter + hash table
    bloom := DS.NewBloomFilter(right.RowCount(), 0.01)
    hash := make(map[interface{}][][]DS.Value)
    for _, rRow := range right.Scan() {
        key := joinHashKey(rRow[rci])
        bloom.Add(key)
        hash[key] = append(hash[key], rRow)
    }
    
    // Probe phase: check bloom filter first (avoids hash lookup for non-matches)
    var out [][]DS.Value
    for _, lRow := range left.Scan() {
        key := joinHashKey(lRow[lci])
        if !bloom.MightContain(key) {
            continue  // Skip hash lookup entirely
        }
        matches, ok := hash[key]
        // ...
    }
    return out
}
```

### B3. Benchmark

```go
func BenchmarkFair_JoinBloomFilter(b *testing.B) {
    // 1000 users × 5000 orders, only 10% match
    // Expected: ~30% faster than current hash join
}
```

---

## Track C: Vectorized WHERE Filter

### C1. Column-Aware Filter Execution

When table is in columnar mode (`HybridStore.IsColumnar()`), execute WHERE predicates
directly on `ColumnVector` data using SIMD-optimized comparison:

Add `pkg/sqlvibe/exec_columnar.go`:

```go
// VectorizedFilterSIMD applies a comparison filter to a ColumnVector using
// SIMD-optimized batch comparison. Returns a RoaringBitmap of matching indices.
func VectorizedFilterSIMD(col *DS.ColumnVector, op string, val DS.Value) *DS.RoaringBitmap {
    switch col.Type() {
    case DS.TypeInt:
        return vectorizedFilterInt64(col.Ints(), op, val.Int)
    case DS.TypeFloat:
        return vectorizedFilterFloat64(col.Floats(), op, val.Float)
    case DS.TypeString:
        return vectorizedFilterString(col.Strings(), op, val.Str)
    }
    return VectorizedFilter(col, op, val)  // fallback
}

func vectorizedFilterInt64(data []int64, op string, val int64) *DS.RoaringBitmap {
    rb := DS.NewRoaringBitmap()
    // 4-way unrolled comparison for SIMD auto-vectorization
    n := len(data)
    i := 0
    for ; i <= n-4; i += 4 {
        // Process 4 elements at once
        if op == "=" {
            if data[i] == val { rb.Add(uint32(i)) }
            if data[i+1] == val { rb.Add(uint32(i+1)) }
            if data[i+2] == val { rb.Add(uint32(i+2)) }
            if data[i+3] == val { rb.Add(uint32(i+3)) }
        }
        // ... other ops
    }
    // Handle remainder
    for ; i < n; i++ {
        // ...
    }
    return rb
}
```

### C2. Integrate with Query Execution

Modify `pkg/sqlvibe/database.go` to detect columnar tables and route to vectorized path:

```go
func (db *Database) execSelectWithWhere(stmt *SelectStmt) ([]map[string]interface{}, error) {
    // Check if table is columnar and WHERE is pushable
    if hs.IsColumnar() && IsPushableExpr(stmt.Where) {
        // Use vectorized filter
        bm := VectorizedFilterSIMD(col, op, val)
        return materializeRows(hs, bm, requiredCols)
    }
    // Fallback to row-by-row execution
}
```

### C3. Benchmark

```go
func BenchmarkFair_VectorizedWhere(b *testing.B) {
    // 10K rows, columnar mode, WHERE int_col = 5000
    // Expected: 3-5× faster than row-by-row
}
```

---

## Track D: Reduce Allocations in Hot Paths

### D1. Row Map Pool

Add `pkg/sqlvibe/row_pool.go`:

```go
var rowPool = sync.Pool{
    New: func() interface{} {
        return make(map[string]interface{}, 16)
    },
}

func getRowMap() map[string]interface{} {
    m := rowPool.Get().(map[string]interface{})
    // Clear map
    for k := range m {
        delete(m, k)
    }
    return m
}

func putRowMap(m map[string]interface{}) {
    rowPool.Put(m)
}
```

**Apply to**: `execSelectStmt`, `execInsertStmt`, cursor iteration.

### D2. Preallocate Result Slices

In query execution, preallocate result slices based on table stats:

```go
func (db *Database) execSelectStmt(...) {
    estimatedRows := db.tableStats[tableName]
    if estimatedRows == 0 {
        estimatedRows = 1000
    }
    results := make([]map[string]interface{}, 0, estimatedRows)
}
```

### D3. String Builder for Row Key Construction

Replace `fmt.Sprintf` key building with `strings.Builder` in hot paths:

```go
// Before: key := fmt.Sprintf("%v|%v", key1, key2)
// After:
var sb strings.Builder
sb.Grow(32)
sb.WriteString(key1.String())
sb.WriteByte(0)
sb.WriteString(key2.String())
key := sb.String()
```

### D4. Benchmark

```go
func Benchmark_AllocationOverhead(b *testing.B) {
    // SELECT * FROM t WHERE id = ? (parameterized, repeated 10000×)
    // Measure allocations per iteration
    // Target: < 5 allocations per query (down from ~20)
}
```

---

## Track E: Query Optimizer Improvements

### E1. Selectivity-Based Index Selection

Extend `internal/QP/optimizer.go`:

```go
// IndexSelectivity estimates the fraction of rows matching a predicate.
// Uses ANALYZE statistics if available; otherwise uses heuristics.
func IndexSelectivity(idx *IndexMetaQP, expr Expr, tableStats *TableStats) float64

// SelectBestIndexByCost chooses the index with lowest estimated cost.
// Cost = (selectivity * table_rows) + index_lookup_cost
func SelectBestIndexByCost(indexes []*IndexMetaQP, expr Expr, stats *TableStats) *IndexMetaQP
```

**Heuristics** (when no stats available):
- `col = const`: selectivity = 0.01 (1% match)
- `col > const`: selectivity = 0.33
- `col BETWEEN a AND b`: selectivity = 0.10
- `col LIKE 'prefix%'`: selectivity = 0.05

### E2. OR Predicate Index Union

For `WHERE col1 = a OR col2 = b` with indexes on both columns:

```go
// OrIndexUnion performs bitmap union of two index lookups.
func OrIndexUnion(db *Database, table string, preds []Expr) *DS.RoaringBitmap {
    var result *DS.RoaringBitmap
    for _, pred := range preds {
        bm := db.indexLookup(table, pred)
        if result == nil {
            result = bm
        } else {
            result.UnionInPlace(bm)
        }
    }
    return result
}
```

### E3. Benchmark

```go
func BenchmarkFair_OrIndexUnion(b *testing.B) {
    // CREATE INDEX idx_a ON t(a)
    // CREATE INDEX idx_b ON t(b)
    // SELECT * FROM t WHERE a = 100 OR b = 200
    // Expected: use both indexes, union results
}
```

---

## Track F: Dispatch Table Expansion

### F1. Add More Opcodes to Dispatch Table

Extend `internal/VM/dispatch.go`:

| Opcode | Handler | Estimated Impact |
|--------|---------|------------------|
| `OpNotNull` | Check if register is not NULL | 5% for WHERE-heavy queries |
| `OpIsNull` | Check if register is NULL | 5% for NULL-heavy queries |
| `OpAnd` | Logical AND | 3% for compound predicates |
| `OpOr` | Logical OR | 3% for compound predicates |
| `OpNot` | Logical NOT | 1% |
| `OpBitAnd` / `OpBitOr` | Bitwise operations | 2% for bit operations |
| `OpRemainder` | Modulo | 1% |

### F2. Inline Hot Paths

Mark hot-path functions for inlining:

```go
//go:noinline  // prevent inlining for cold paths
func (vm *VM) handleComplexOp(inst *Instruction) { ... }

// Inline threshold hint for hot paths
//go:inline
func compareVals(a, b interface{}) int { ... }
```

---

## Track G: Parallel Query Execution (Optional/Stretch)

### G1. Parallel Table Scan

For large tables (>10K rows), split scan across multiple goroutines:

```go
func (db *Database) parallelScan(table string, workers int, fn func(row map[string]interface{}) bool) {
    rows := db.tables[table].Rows()
    chunkSize := (len(rows) + workers - 1) / workers
    
    var wg sync.WaitGroup
    wg.Add(workers)
    
    for w := 0; w < workers; w++ {
        start := w * chunkSize
        end := start + chunkSize
        if end > len(rows) {
            end = len(rows)
        }
        
        go func(start, end int) {
            defer wg.Done()
            for i := start; i < end; i++ {
                if !fn(rows[i]) {
                    break
                }
            }
        }(start, end)
    }
    
    wg.Wait()
}
```

**Note**: This is a stretch goal. Profile first to confirm parallelization overhead is worth it.

---

## Files to Create / Modify

| File | Action | Track |
|------|--------|-------|
| `internal/DS/bloom_filter.go` | **NEW** | B |
| `internal/QP/optimizer.go` | Modify (selectivity, index selection) | A, E |
| `pkg/sqlvibe/exec_columnar.go` | Modify (bloom filter, vectorized WHERE) | B, C |
| `pkg/sqlvibe/database.go` | Modify (index-only scan, row pool) | A, D |
| `pkg/sqlvibe/row_pool.go` | **NEW** | D |
| `pkg/sqlvibe/simd.go` | Modify (add filter functions) | C |
| `internal/VM/dispatch.go` | Modify (more opcodes) | F |
| `internal/TS/Benchmark/benchmark_v0.9.16_test.go` | **NEW** | All |
| `pkg/sqlvibe/version.go` | Bump to `v0.9.16` | Final |
| `docs/HISTORY.md` | Add v0.9.16 entry | Final |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| SELECT WHERE ≤ 80 µs (from 285 µs) | Yes | [ ] |
| JOIN ≤ 200 µs (from 559 µs) | Yes | [ ] |
| Index-only scan implemented | Yes | [ ] |
| Bloom filter for hash join | Yes | [ ] |
| Vectorized WHERE on columnar | Yes | [ ] |
| Allocations reduced 50% | Yes | [ ] |
| SELECT all no regression | ≤ 65 µs | [ ] |
| All existing tests pass | 100% | [ ] |
| Version bumped to v0.9.16 | Yes | [ ] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| BenchmarkFair v0.9.16 | Performance benchmarks | [ ] |
| Benchmark_AllocationOverhead | Allocation profiling | [ ] |
| Full SQL:1999 run | No regressions | [ ] |
| Regression v0.9.16 | New feature tests | [ ] |

---

## Benchmark Commands

```bash
# Full benchmark suite
go test ./internal/TS/Benchmark/... -bench=BenchmarkFair -benchtime=3s

# Allocation profiling
go test ./internal/TS/Benchmark/... -bench=Benchmark_Allocation -benchmem

# Compare before/after
go test ./internal/TS/Benchmark/... -bench=. -benchtime=3s | tee bench-v0.9.16.txt
```

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Bloom filter false positives | Use low FPR (1%), fallback to hash lookup |
| Parallel execution overhead | Profile first, only enable for large tables |
| Memory increase for indexes | Document trade-off, make optional via PRAGMA |
| Regression in existing perf | Run full benchmark suite before each commit |
