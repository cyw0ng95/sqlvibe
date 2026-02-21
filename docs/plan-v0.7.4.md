# Plan v0.7.4 - Performance Optimization

## Summary

Address critical performance bottlenecks identified in v0.7.3 benchmarks to achieve 2-10x speedup on slow operations.

**Previous**: v0.7.3 delivers GROUP BY, JOIN, and aggregate optimizations

---

## Current Bottlenecks (v0.7.3)

| # | Operation | Current | Target | Speedup |
|---|-----------|--------:|-------:|--------:|
| 1 | EXISTS subquery | 175 ms | 20 ms | **8.7x** |
| 2 | Correlated subquery | 3.5 ms | 0.5 ms | **7x** |
| 3 | Multiple BETWEEN/AND | 2.3 ms | 0.5 ms | **4.6x** |
| 4 | Full table scan (5K) | 1.0 ms | 0.2 ms | **5x** |
| 5 | JOIN memory | 490 KB | 200 KB | **2.4x** |

---

## Waves Overview

| Wave | Feature | Target | Status |
|------|---------|--------|--------|
| 1 | Page Prefetching | 2x speedup | Pending |
| 2 | Subquery Optimization | 5-10x speedup | Pending |
| 3 | Index Usage | 5x speedup | Pending |
| 4 | Expression Short-circuit | 3x speedup | Pending |
| 5 | Memory Pooling | 2x less memory | Pending |

---

## Wave 1: Page Prefetching

**Status**: Pending

**Goal**: Read B-Tree pages before needed to reduce I/O wait

### Implementation

```go
// DS Layer - Add prefetch to B-Tree search
type BTree struct {
    // ... existing fields
    prefetchEnabled bool
}

// Prefetch next N child pages
func (bt *BTree) prefetchChildren(page *Page, count int) {
    if !bt.prefetchEnabled {
        return
    }
    for i := 0; i < count && i < len(page.CellPointers); i++ {
        pageNum := readUint32(page.Data[page.CellPointers[i]:])
        go bt.pm.GetPage(pageNum)  // async prefetch
    }
}
```

### Benchmarks

| Benchmark | Target |
|-----------|--------|
| BTreeSearchWithPrefetch | < 50 µs |
| IndexRangeScan | < 200 µs |

### Files to Modify
- `internal/DS/btree.go` - Add prefetch logic
- `internal/DS/page.go` - Async page loading

---

## Wave 2: Subquery Optimization

**Status**: Pending

**Goal**: 5-10x speedup for EXISTS and correlated subqueries

### Problems
- EXISTS subqueries: 175ms (full table scan + row materialization)
- Correlated subqueries: 3.5ms (re-evaluated per outer row)

### Implementation

#### 2.1 EXISTS Subquery Optimization
```go
// Rewrite EXISTS as semi-join
// Before: SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)
// After:  Semi-join with hash table, stop at first match
```

#### 2.2 Correlated Subquery Decorrelation
```go
// Push correlated subqueries into main query
// Before: SELECT * FROM t WHERE col > (SELECT AVG(col) FROM t WHERE cat = t.cat)
// After:  GROUP BY cat, then JOIN
```

#### 2.3 Subquery Result Caching
```go
type subqueryCache struct {
    cache map[string][]interface{}
    hits  int
    misses int
}

func (sc *subqueryCache) Get(key string) ([]interface{}, bool) {
    if result, ok := sc.cache[key]; ok {
        sc.hits++
        return result, true
    }
    sc.misses++
    return nil, false
}
```

### Benchmarks

| Benchmark | Current | Target |
|-----------|--------:|-------:|
| EXISTS subquery | 175 ms | < 20 ms |
| Correlated subquery | 3.5 ms | < 0.5 ms |

### Files to Modify
- `internal/QE/subquery.go` - New subquery optimizer
- `internal/VM/exec.go` - Semi-join operator

---

## Wave 3: Index Usage Expansion

**Status**: Pending

**Goal**: Use secondary index for more WHERE clauses

### Current State
- Secondary index exists but only used for exact match
- BETWEEN/AND, IN, LIKE still do full table scan

### Implementation

```go
// Extend tryIndexLookup to handle more patterns

// BETWEEN: index_col BETWEEN a AND b
// Convert to: index_col >= a AND index_col <= b
// Use index range scan

// IN: col IN (a, b, c)
// Use index lookup for each value, union results

// LIKE: col LIKE 'prefix%'
// Use index range scan for prefix
```

### Benchmarks

| Benchmark | Current | Target |
|-----------|--------:|-------:|
| BETWEEN query | 2.3 ms | < 0.5 ms |
| IN clause | 1.5 ms | < 0.3 ms |
| LIKE prefix | 165 µs | < 50 µs |
| Full table scan | 1.0 ms | < 0.2 ms |

### Files to Modify
- `pkg/sqlvibe/database.go` - Extend tryIndexLookup
- `internal/DS/index.go` - Range scan support

---

## Wave 4: Expression Short-circuit

**Status**: Pending

**Goal**: Skip evaluation of expressions when result is determined

### Current State
- All AND conditions evaluated even if first is false
- All OR conditions evaluated even if first is true

### Implementation

```go
// Short-circuit AND evaluation
func evalAnd(exprs []Expr, row []interface{}) interface{} {
    for _, expr := range exprs {
        result := expr.Eval(row)
        if isFalse(result) {
            return false  // Short-circuit
        }
    }
    return true
}

// Short-circuit OR evaluation
func evalOr(exprs []Expr, row []interface{}) interface{} {
    for _, expr := range exprs {
        result := expr.Eval(row)
        if isTrue(result) {
            return true  // Short-circuit
        }
    }
    return false
}
```

### Benchmarks

| Benchmark | Current | Target |
|-----------|--------:|-------:|
| Multiple AND | 2.3 ms | < 0.8 ms |
| Multiple OR | 2.0 ms | < 0.7 ms |

### Files to Modify
- `internal/QE/operators.go` - Short-circuit logic
- `internal/VM/exec.go` - Lazy evaluation

---

## Wave 5: Memory Pooling

**Status**: Pending

**Goal**: Reduce allocations by reusing buffers

### Current State
- JOIN: 490 KB per operation
- Row storage: map[string]interface{}
- String operations: 5K allocations per 1K rows

### Implementation

#### 5.1 Row Storage: []interface{} instead of map[string]interface{}
```go
// Before: map[string]interface{}{"col": value}
// After:  []interface{}{value0, value1, ...}  // indexed by column position

type Row struct {
    Data   []interface{}
    Schema *TableSchema
}

func (r *Row) Get(colName string) interface{} {
    idx := r.Schema.ColumnIndex(colName)
    return r.Data[idx]
}
```

#### 5.2 sync.Pool for Common Objects
```go
var rowPool = sync.Pool{
    New: func() interface{} {
        return make([]interface{}, 64)
    },
}

func getRow() []interface{} {
    return rowPool.Get().([]interface{})[:0]
}

func putRow(row []interface{}) {
    rowPool.Put(row)
}
```

#### 5.3 String Interning
```go
var stringInterner = make(map[string]string)

func intern(s string) string {
    if existing, ok := stringInterner[s]; ok {
        return existing
    }
    stringInterner[s] = s
    return s
}
```

### Benchmarks

| Benchmark | Current | Target |
|-----------|--------:|-------:|
| JOIN memory | 490 KB | < 200 KB |
| String concat | 5K allocs | < 500 allocs |
| SELECT * allocs | 1,060 | < 200 |

### Files to Modify
- `pkg/sqlvibe/row.go` - New row type
- `internal/QE/operators.go` - Pool usage
- `internal/VM/registers.go` - sync.Pool

---

## Wave 6: SIMD Math Operations

**Status**: Pending (Optional)

**Goal**: Vectorized arithmetic for column batches

### Implementation

```go
// Vectorized add for int64 slices
func addVec(a, b []int64) []int64 {
    result := make([]int64, len(a))
    for i := range a {
        result[i] = a[i] + b[i]
    }
    return result
}

// Use in aggregate SUM
func (agg *SumAgg) AddBatch(values []int64) {
    agg.value = agg.value + sumVec(values)
}
```

### Benchmarks

| Benchmark | Target |
|-----------|-------:|
| SUM 1M rows | < 10 ms |
| AVG 1M rows | < 12 ms |

### Files to Modify
- `internal/QE/math.go` - Vectorized operations

---

## Success Criteria

- [x] EXISTS subquery: early exit via LIMIT-1 short-circuit (Wave 2)
- [x] BETWEEN/AND query: index range scan (Wave 3)
- [x] IN list query: index multi-lookup (Wave 3)
- [x] LIKE prefix query: index prefix scan (Wave 3)
- [x] Page prefetching: async child-page goroutines added to BTree (Wave 1)
- [x] JOIN memory: sync.Pool for merged-row maps in hash join (Wave 5)
- [x] All SQL1999 tests still passing
- [ ] Full table scan: < 0.2 ms (5x faster) — future work
- [ ] String allocations: < 500 (10x less) — future work

---

## Timeline Estimate

| Wave | Feature | Estimated Hours |
|------|---------|-----------------|
| 1 | Page Prefetching | 8 |
| 2 | Subquery Optimization | 16 |
| 3 | Index Usage | 12 |
| 4 | Expression Short-circuit | 8 |
| 5 | Memory Pooling | 16 |
| 6 | SIMD Math (optional) | 12 |

**Total**: ~60 hours (excluding optional SIMD)

---

## Dependencies

- Wave 2 depends on Wave 3 (index must work first)
- Wave 4 can be done in parallel
- Wave 5 is foundational for all other waves

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Breaking SQL1999 tests | Add regression tests before changes |
| Memory regression | Run benchmarks after each change |
| Complex subquery rewrite | Add optimizer flags, gradual rollout |

---

## Benchmark Commands

```bash
# Run all benchmarks
go test ./internal/TS/Benchmark/... -bench . -benchmem

# Run specific bottleneck benchmarks
go test ./internal/TS/Benchmark/... -bench "BenchmarkHeavy" -benchmem

# Compare with baseline
go test ./internal/TS/Benchmark/... -bench "BenchmarkHeavySubquery" -benchmem -count=5
```
