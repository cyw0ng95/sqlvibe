# Plan v0.7.2 - Additional Benchmark Tests

## Summary
Expand benchmark coverage to better identify performance bottlenecks and measure optimization impact.

**Previous**: v0.7.1 delivers performance optimizations for subqueries, JOINs, and batch operations

---

## Current Coverage (v0.7.0)

| Category | Benchmarks | Count |
|----------|------------|-------|
| INSERT | Single, Batch100 | 2 |
| SELECT | All, Where, OrderBy, Limit | 4 |
| Aggregates | COUNT, SUM, AVG, MIN, MAX, GroupBy | 6 |
| JOIN | Inner Join | 1 |
| Subquery | IN, Scalar | 2 |
| DML | UPDATE, DELETE | 2 |
| Other | LIKE, UNION, CASE, DDL, Schema | 4 |

**Total**: 21 benchmark tests

---

## Waves Overview

| Wave | Feature | New Tests | Status |
|------|---------|-----------|--------|
| 1 | Transaction & Concurrency | 4 | Pending |
| 2 | Index & Query Optimization | 5 | Pending |
| 3 | Scale & Stress | 4 | Pending |
| 4 | Memory & Cache | 3 | Pending |
| 5 | Specific Operations | 5 | Pending |
| 6 | DDL & Schema | 4 | Pending |
| 7 | Verification | - | Pending |

**Total**: 25 new tests → 46 total

---

## Wave 1: Transaction & Concurrency

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| BenchmarkTransactionCommit | Single transaction commit | <1ms |
| BenchmarkTransactionRollback | Rollback performance | <1ms |
| BenchmarkMultipleTransactions | Multiple sequential transactions | ops/sec |
| BenchmarkConcurrentWrites | Multiple concurrent writers | ops/sec |

### Implementation
```go
func BenchmarkTransactionCommit(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val TEXT)")
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        tx, _ := db.Begin()
        tx.Exec("INSERT INTO t VALUES (?, ?)", i, "value")
        tx.Commit()
    }
}
```

---

## Wave 2: Index & Query Optimization

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| BenchmarkSelectWithIndex | SELECT with index vs without | 10x improvement |
| BenchmarkSelectRangeQuery | Range queries (>, <, BETWEEN) | ops/sec |
| BenchmarkSelectDistinct | DISTINCT operation | ops/sec |
| BenchmarkSelectHaving | HAVING clause | ops/sec |
| BenchmarkCompositeIndex | Multi-column index usage | ops/sec |

### Implementation
```go
func BenchmarkSelectWithIndex(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    mustExec(b, db, "CREATE INDEX idx_val ON t(val)")
    // Insert test data
    for i := 0; i < 1000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, "SELECT * FROM t WHERE val = 500")
    }
}
```

---

## Wave 3: Scale & Stress

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| BenchmarkLargeTable10K | 10K rows SELECT | <100ms |
| BenchmarkLargeTable100K | 100K rows SELECT | <1s |
| BenchmarkBulkInsert10K | INSERT 10K rows | <5s |
| BenchmarkMultiTableJoin | 3+ table JOIN | ops/sec |

### Implementation
```go
func BenchmarkLargeTable10K(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    
    // Insert 10K rows
    for i := 0; i < 10000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, "SELECT * FROM t")
    }
}
```

---

## Wave 4: Memory & Cache

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| BenchmarkPageCacheHitRate | Sequential read cache hit | >90% |
| BenchmarkPageCacheMissRate | Random read cache miss | <10% |
| BenchmarkMemoryUsage | Peak memory per query | KB/query |

### Implementation
```go
func BenchmarkPageCacheHitRate(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    db.Exec("PRAGMA cache_size = 1000")
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    
    // Insert test data
    for i := 0; i < 1000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // Sequential reads should hit cache
        mustQuery(b, db, "SELECT * FROM t ORDER BY id")
    }
}
```

---

## Wave 5: Specific Operations

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| BenchmarkStringOperations | CONCAT, SUBSTR, LENGTH | ops/sec |
| BenchmarkMathOperations | +, -, *, /, % on large dataset | ops/sec |
| BenchmarkTypeConversion | CAST between types | ops/sec |
| BenchmarkNullHandling | IS NULL, COALESCE, IFNULL | ops/sec |
| BenchmarkBooleanLogic | AND, OR, NOT, CASE | ops/sec |

### Implementation
```go
func BenchmarkStringOperations(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, name TEXT)")
    for i := 0; i < 1000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'name_%d')", i, i))
    }
    
    b.Run("CONCAT", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            mustQuery(b, db, "SELECT name || '_suffix' FROM t")
        }
    })
    b.Run("SUBSTR", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            mustQuery(b, db, "SELECT SUBSTR(name, 1, 4) FROM t")
        }
    })
    b.Run("LENGTH", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            mustQuery(b, db, "SELECT LENGTH(name) FROM t")
        }
    })
}
```

---

## Wave 6: DDL & Schema

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| BenchmarkCreateIndex | CREATE INDEX performance | <100ms |
| BenchmarkDropIndex | DROP INDEX performance | <50ms |
| BenchmarkAnalyze | ANALYZE performance | <100ms |

### Implementation
```go
func BenchmarkCreateIndex(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    for i := 0; i < 10000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustExec(b, db, "CREATE INDEX idx_val ON t(val)")
        mustExec(b, db, "DROP INDEX idx_val")
    }
}
```

---

## Wave 7: Verification

**Status**: Pending

### Tasks
- Run all new benchmarks
- Compare with baseline (v0.7.0)
- Document results
- Add to CI pipeline

---

## Target Metrics Summary

| Category | Current | Target |
|----------|---------|--------|
| Transaction Commit | TBD | <1ms |
| Index Lookup vs Scan | N/A | 10x faster |
| Large Table (10K) | N/A | <100ms |
| Large Table (100K) | N/A | <1s |
| Bulk Insert (10K) | N/A | <5s |
| Cache Hit Rate | TBD | >90% |
| Memory per Query | TBD | <10KB |

---

## Files to Create

- `internal/TS/Benchmark/benchmark_txn_test.go` - Transaction benchmarks
- `internal/TS/Benchmark/benchmark_index_test.go` - Index benchmarks
- `internal/TS/Benchmark/benchmark_scale_test.go` - Scale benchmarks
- `internal/TS/Benchmark/benchmark_memory_test.go` - Memory benchmarks
- `internal/TS/Benchmark/benchmark_ops_test.go` - Operation benchmarks
- `internal/TS/Benchmark/benchmark_ddl_test.go` - DDL benchmarks

---

## Success Criteria

- [ ] 25 new benchmark tests added
- [ ] All benchmarks run successfully
- [ ] Baseline metrics captured
- [ ] Targets documented
- [ ] CI integration complete

---

## Timeline Estimate

| Wave | Feature | Estimated Hours |
|------|---------|-----------------|
| 1 | Transaction & Concurrency | 2 |
| 2 | Index & Query Optimization | 3 |
| 3 | Scale & Stress | 2 |
| 4 | Memory & Cache | 2 |
| 5 | Specific Operations | 2 |
| 6 | DDL & Schema | 1 |
| 7 | Verification | 2 |

**Total**: ~14 hours
