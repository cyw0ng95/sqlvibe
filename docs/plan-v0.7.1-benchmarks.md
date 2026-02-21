# Plan v0.7.1 - Additional Benchmark Tests

## Summary
Expand benchmark coverage to better identify performance bottlenecks and measure optimization impact.

---

## Current Coverage

| Category | Benchmarks | Status |
|----------|------------|--------|
| INSERT | Single, Batch100 | ✅ |
| SELECT | All, Where, OrderBy, Limit | ✅ |
| Aggregates | COUNT, SUM, AVG, MIN, MAX, GroupBy | ✅ |
| JOIN | Inner Join | ✅ |
| Subquery | IN, Scalar | ✅ |
| DML | UPDATE, DELETE | ✅ |
| Other | LIKE, UNION, CASE, DDL, Schema | ✅ |

**Total**: 25 benchmark tests

---

## Proposed New Benchmarks

### Wave 1: Transaction & Concurrency

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| TransactionCommit | Single transaction commit | <1ms |
| TransactionRollback | Rollback performance | <1ms |
| MultipleTransactions | Multiple sequential transactions | ops/sec |
| ConcurrentWrites | Multiple concurrent writers (simulated) | ops/sec |

### Wave 2: Index & Query Optimization

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| SelectWithIndex | SELECT with index vs without | 10x improvement |
| SelectRangeQuery | Range queries (>, <, BETWEEN) | ops/sec |
| SelectDistinct | DISTINCT operation | ops/sec |
| SelectHaving | HAVING clause | ops/sec |
| CompositeIndex | Multi-column index usage | ops/sec |

### Wave 3: Scale & Stress

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| LargeTable10K | 10K rows SELECT | ms/row |
| LargeTable100K | 100K rows SELECT | ms/row |
| BulkInsert10K | INSERT 10K rows | <5s |
| MultiTableJoin | 3+ table JOIN | ops/sec |
| DeepNestedQuery | 3+ level nested queries | ops/sec |

### Wave 4: Memory & Cache

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| PageCacheHitRate | Sequential read cache hit | >90% |
| PageCacheMissRate | Random read cache miss | <10% |
| MemoryUsage | Peak memory per query | KB/query |
| LargeResultSet | 10K+ rows result set | ms |

### Wave 5: Specific Operations

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| StringOperations | CONCAT, SUBSTR, LENGTH | ops/sec |
| MathOperations | +, -, *, /, % on large dataset | ops/sec |
| TypeConversion | CAST between types | ops/sec |
| NullHandling | IS NULL, COALESCE, IFNULL | ops/sec |
| BooleanLogic | AND, OR, NOT, CASE | ops/sec |

### Wave 6: DDL & Schema

| Benchmark | Description | Target Metric |
|-----------|-------------|---------------|
| CreateIndex | CREATE INDEX performance | <100ms |
| DropIndex | DROP INDEX performance | <50ms |
| AlterTable | ALTER TABLE (if supported) | <100ms |
| Vacuum | VACUUM (if supported) | <1s |
| Analyze | ANALYZE performance | <100ms |

---

## Implementation

### File: `internal/TS/Benchmark/benchmark_test.go`

```go
// -----------------------------------------------------------------
// Wave 1: Transaction & Concurrency
// -----------------------------------------------------------------

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

func BenchmarkSelectWithIndex(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    mustExec(b, db, "CREATE INDEX idx_val ON t(val)")
    // ... benchmark with index
}

// -----------------------------------------------------------------
// Wave 2: Large Scale
// -----------------------------------------------------------------

func BenchmarkLargeTable10K(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    // Insert 10K rows
    for i := 0; i < 10000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    // Benchmark SELECT
}

// -----------------------------------------------------------------
// Wave 3: Memory & Cache
// -----------------------------------------------------------------

func BenchmarkPageCacheHitRate(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    db.Exec("PRAGMA cache_size = 1000")
    // Sequential reads should hit cache
}

// -----------------------------------------------------------------
// Wave 4: Specific Operations
// -----------------------------------------------------------------

func BenchmarkStringOperations(b *testing.B) {
    // CONCAT, SUBSTR, LENGTH, etc.
}

func BenchmarkMathOperations(b *testing.B) {
    // +, -, *, /, % on large dataset
}
```

---

## Test Categories Summary

| Wave | Category | New Tests | Total After |
|------|----------|-----------|-------------|
| 1 | Transaction & Concurrency | 4 | 29 |
| 2 | Index & Query | 5 | 34 |
| 3 | Scale & Stress | 4 | 38 |
| 4 | Memory & Cache | 3 | 41 |
| 5 | Specific Operations | 5 | 46 |
| 6 | DDL & Schema | 4 | 50 |

---

## Target Metrics

| Benchmark | Current (v0.7.0) | Target (v0.7.1) |
|-----------|------------------|-----------------|
| TransactionCommit | TBD | <1ms |
| SelectWithIndex | N/A | 10x vs table scan |
| LargeTable10K | N/A | <100ms |
| LargeTable100K | N/A | <1s |
| BulkInsert10K | N/A | <5s |
| PageCacheHitRate | TBD | >90% |

---

## Execution Order

1. Add basic transaction benchmarks first
2. Add index-related benchmarks to measure optimization impact
3. Add scale benchmarks for stress testing
4. Add memory/cache benchmarks for performance tuning
5. Add specific operation benchmarks for targeted optimization
6. Add DDL benchmarks for completeness
