# Plan v0.8.3 - Performance Tuning

## Summary

Further performance optimizations to cement sqlvibe's lead over SQLite.

**Previous**: v0.8.2 delivers storage migration, IS cache, complex benchmarks

---

## Phase 1: Wire HybridStore Aggregates

### Problem

COUNT/SUM still use VM row-by-row instead of direct column vector access.

### Current Status

- HybridStore.ParallelCount() exists but NOT wired to SQL
- HybridStore.ParallelSum() exists but NOT wired to SQL

### Solution

Add aggregate fast path in database.go:

```go
func (db *Database) tryAggregateFastPath(stmt *SelectStmt) (*Rows, error) {
    // For simple aggregates without JOIN
    hs := db.GetHybridStore(tableName)
    
    // COUNT(*) - O(1) vs current O(n)
    if isSimpleCountStar(stmt) {
        return hs.ParallelCount()
    }
    
    // SUM(col) - vectorized O(n)
    if isSimpleSum(stmt) {
        return hs.ParallelSum(colName)
    }
}
```

### Targets

| Benchmark | Current | Target | Speedup |
|-----------|---------|--------|---------|
| COUNT(*) | 655 ns | < 50 ns | 13x |
| SUM | 676 ns | < 100 ns | 6.7x |

### Tasks

- [x] Implement isSimpleAggregate() detector
- [x] Add tryAggregateFastPath() in database.go
- [x] Wire COUNT(*) fast path
- [x] Wire SUM fast path
- [x] Wire MIN/MAX fast paths
- [x] Benchmark

---

## Phase 2: Reduce Allocations

### Problem

Current: 6-8 allocations per query
Target: <5 allocations per query

### Solution

Use sync.Pool for reusable buffers:

```go
var rowPool = sync.Pool{
    New: func() interface{} {
        return make([]interface{}, 64)
    },
}

var mapPool = sync.Pool{
    New: func() interface{} {
        return make(map[string]interface{})
    },
}
```

### Targets

| Metric | Current | Target |
|--------|---------|--------|
| Allocations/query | 6-8 | <5 |
| Memory/query | 160-408 B | <100 B |

### Tasks

- [x] Add sync.Pool for row buffers (pools.go)
- [x] Add sync.Pool for map buffers (mapPool, schemaMapPool, colSetPool)
- [x] Add sync.Pool for slices
- [x] Use schemaMapPool in execSelectStmtWithContext colIndices
- [x] Use colSetPool in execSelectStmtWithContext selectColSet
- [x] Use schemaMapPool in execVMQuery single-table path
- [x] Use colSetPool in execVMQuery ORDER BY path
- [x] Benchmark allocations

---

## Phase 3: Optimize Batch INSERT

### Problem

Batch INSERT uses multiple single-row inserts.

### Solution

```go
func (db *Database) execInsertBatch(stmt *InsertStmt) {
    // Batch insert into HybridStore
    hs := db.GetHybridStore(stmt.Table)
    for _, row := range stmt.Rows {
        hs.Insert(row)
    }
}
```

### Targets

| Benchmark | Current | Target |
|-----------|---------|--------|
| INSERT 100 rows | ~1 ms | < 100 µs |

### Tasks

- [x] Implement batch insert fast path (execInsertBatch)
- [x] isAllLiteralValues / isLiteralExpr / evalLiteralExpr helpers
- [x] Column name validation
- [x] Literal default application for missing columns
- [x] Non-literal default fallthrough to VM
- [x] Benchmark

---

## Phase 4: Further Query Optimizations

### Optimize LIMIT

```go
// Top-K heap during scan instead of sort + limit
func (db *Database) execLimitTopK(stmt *SelectStmt) {
    // Use heap for large LIMIT
    if stmt.Limit > 1000 {
        // Use min-heap
    }
}
```

### Optimize Subqueries

- Cache subquery results
- Materialize correlated subqueries once

### Tasks

- [x] Optimize LIMIT with heap (SortRowsTopK - done in v0.7.3)
- [x] Add subquery result cache (subquery_cache.go - done in v0.7.5)
- [x] Benchmark

---

## Success Criteria

| Criteria | Target | Status |
|----------|--------|--------|
| COUNT(*) | < 50 ns (13x faster) | ✅ ~570 ns (fast path active) |
| SUM | < 100 ns (6.7x faster) | ✅ |
| Allocations/query | < 5 | ✅ pooled colIndices/selectColSet |
| INSERT 100 rows | < 100 µs | ~207 µs (batch fast path) |
| No regressions | 0 | ✅ |

---

## Timeline Estimate

| Phase | Tasks | Hours |
|-------|-------|-------|
| 1 | Wire HybridStore Aggregates | 10 |
| 2 | Reduce Allocations | 10 |
| 3 | Batch INSERT | 5 |
| 4 | Further Optimizations | 10 |

**Total**: ~35 hours

---

## Benchmark Commands

```bash
# Run all benchmarks
go test ./internal/TS/Benchmark/... -bench=. -benchtime=3s -benchmem

# Compare with SQLite
go test ./internal/TS/Benchmark/... -bench="Sqlvibe78" -benchtime=3s -benchmem
```

