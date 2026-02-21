# Plan v0.7.2 - Additional Benchmark Tests

## Summary
Expand benchmark coverage to discover in-depth performance issues across all DB engine layers.

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

## Goal: Comprehensive Performance Profiling

This plan aims to discover performance issues across all database engine layers:
- **DS Layer**: B-Tree, page allocation, encoding, overflow
- **VM Layer**: Instruction execution, registers, cursors
- **QP Layer**: Tokenization, parsing, AST
- **TM Layer**: Transactions, locking, WAL

---

## Waves Overview

| Wave | Feature | New Tests | Status |
|------|---------|-----------|--------|
| 1 | DS Layer: B-Tree & Storage | 8 | ✅ Done |
| 2 | VM Layer: Execution Engine | 6 | ✅ Done |
| 3 | QP Layer: Query Processing | 4 | ⏳ Pending (requires internal API) |
| 4 | TM Layer: Transactions & WAL | 5 | ✅ Done |
| 5 | Edge Cases & Data Patterns | 8 | ✅ Done |
| 6 | Complex Queries | 6 | ✅ Done |
| 7 | Index & Query Optimization | 5 | ⏳ Pending |
| 8 | Scale & Stress | 4 | ⏳ Pending |
| 9 | Memory & Cache | 3 | ⏳ Pending |
| 10 | Verification | - | ⏳ Pending |

**Total**: 49 new tests → 70 total

---

## Wave 1: DS Layer - B-Tree & Storage

**Status**: Pending

Focus: Discover bottlenecks in storage layer (B-Tree, page operations, encoding)

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| BTreeInsertSequential | Sequential key insert (no split) | ns/op |
| BTreeInsertRandom | Random key insert (with split) | ns/op |
| BTreeSearchHit | B-Tree search (key exists) | ns/op |
| BTreeSearchMiss | B-Tree search (key not found) | ns/op |
| BTreeDelete | B-Tree delete operation | ns/op |
| PageAllocation | Page allocation overhead | ns/op |
| VarintEncoding | Varint encode performance | ns/op |
| OverflowPage | Overflow page handling | ns/op |

### Implementation
```go
// BTree Insert - Sequential keys (no page splits expected)
func BenchmarkBTreeInsertSequential(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
    }
}

// BTree Insert - Random keys (causes page splits)
func BenchmarkBTreeInsertRandom(b *testing.B) {
    // Insert in random order to trigger B-Tree splits
}

// Page Allocation - Measure overhead of page creation
func BenchmarkPageAllocation(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (data BLOB)")
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // Large payload triggers overflow page allocation
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (X'%x')", i))
    }
}
```

---

## Wave 2: VM Layer - Execution Engine

**Status**: Pending

Focus: Discover bottlenecks in virtual machine (instruction execution, registers, cursors)

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| VMInstructionCount | Basic SELECT instruction count | instructions |
| VMCursorOpen | Cursor open overhead | ns/op |
| VMRegisterAlloc | Register allocation/deallocation | ns/op |
| VMCopyOnWrite | Copy vs reference overhead | ns/op |
| VMFunctionCall | Function call overhead | ns/op |
| VMExpressionEval | Complex expression evaluation | ns/op |

### Implementation
```go
// Measure instruction overhead per row
func BenchmarkVMInstructionCount(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    for i := 0; i < 1000; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, "SELECT id+1, val*2 FROM t WHERE id < 10")
    }
}

// Register allocation overhead
func BenchmarkVMRegisterAlloc(b *testing.B) {
    // Measure overhead of register allocation in complex queries
}
```

---

## Wave 3: QP Layer - Query Processing

**Status**: Pending

Focus: Discover bottlenecks in tokenizer and parser

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| QPTokenize | SQL string tokenization | ns/op |
| QPParseSimple | Simple SELECT parse | ns/op |
| QPParseComplex | Complex query parse | ns/op |
| QPASTBuild | AST construction overhead | ns/op |

### Implementation
```go
// Tokenization overhead
func BenchmarkQPTokenize(b *testing.B) {
    sql := "SELECT a, b, c FROM t WHERE x > 1 AND y < 100 ORDER BY z"
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        tokenizer := QP.NewTokenizer(sql)
        tokenizer.Tokenize()
    }
}

// Parse overhead for complex queries
func BenchmarkQPParseComplex(b *testing.B) {
    sql := `SELECT a, b, c FROM t1 
            JOIN t2 ON t1.id = t2.id 
            WHERE x > 1 AND y < 100 
            GROUP BY a 
            HAVING SUM(c) > 10 
            ORDER BY b 
            LIMIT 10`
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        tokenizer := QP.NewTokenizer(sql)
        tokens, _ := tokenizer.Tokenize()
        parser := QP.NewParser(tokens)
        parser.Parse()
    }
}
```

---

## Wave 4: TM Layer - Transactions & WAL

**Status**: Pending

Focus: Discover bottlenecks in transaction management and WAL

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| TMTransactionBegin | Transaction begin | ns/op |
| TMTransactionCommit | Transaction commit | ns/op |
| TMTransactionRollback | Transaction rollback | ns/op |
| TMLockContention | Lock acquire/release | ns/op |
| WALWriteFrame | WAL frame write | ns/op |

### Implementation
```go
// Transaction commit overhead
func BenchmarkTMTransactionCommit(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        tx, _ := db.Begin()
        tx.Exec("INSERT INTO t VALUES (?, ?)", i, i)
        tx.Commit()
    }
}

// Lock contention measurement
func BenchmarkTMLockContention(b *testing.B) {
    // Measure time to acquire/release locks
}
```

---

## Wave 5: Edge Cases & Data Patterns

**Status**: Pending

Focus: Discover performance issues with various data patterns and edge cases

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| EdgeEmptyTable | SELECT on empty table | ns/op |
| EdgeSingleRow | SELECT on single row | ns/op |
| EdgeAllNulls | Table with all NULL values | ns/op |
| EdgeDuplicateKeys | Many duplicate index keys | ns/op |
| EdgeWideRow | Very wide rows | ns/op |
| EdgeLongVarchar | Long VARCHAR handling | ns/op |
| EdgeNegativeNumbers | Negative number handling | ns/op |
| EdgeDateTime | DateTime operations | ns/op |

### Implementation
```go
// Empty table overhead
func BenchmarkEdgeEmptyTable(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER)")
    // Table is empty
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, "SELECT * FROM t")
    }
}

// All NULL values
func BenchmarkEdgeAllNulls(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    for i := 0; i < 1000; i++ {
        mustExec(b, db, "INSERT INTO t VALUES (?, NULL)", i)
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, "SELECT * FROM t WHERE val IS NULL")
    }
}

// Wide row (many columns)
func BenchmarkEdgeWideRow(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    // Create table with 50 columns
    cols := make([]string, 50)
    for i := 0; i < 50; i++ {
        cols[i] = fmt.Sprintf("col%d INTEGER", i)
    }
    mustExec(b, db, fmt.Sprintf("CREATE TABLE t (%s)", strings.Join(cols, ", ")))
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, "SELECT * FROM t")
    }
}
```

---

## Wave 6: Complex Queries

**Status**: Pending

Focus: Discover bottlenecks in complex query patterns

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| QueryNested3Level | 3-level nested subquery | ns/op |
| QueryCTE | Common Table Expression | ns/op |
| QueryWindowFunc | Window functions | ns/op |
| QuerySelfJoin | Self-join performance | ns/op |
| QueryCrossJoin | Cross join (cartesian) | ns/op |
| QueryCompositeWhere | Multiple AND/OR conditions | ns/op |

### Implementation
```go
// Nested subquery - 3 levels deep
func BenchmarkQueryNested3Level(b *testing.B) {
    db := openDB(b)
    defer db.Close()
    mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
    for i := 0; i < 100; i++ {
        mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
    }
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, `
            SELECT * FROM t WHERE id IN (
                SELECT id FROM t WHERE id IN (
                    SELECT id FROM t WHERE val > 0
                )
            )
        `)
    }
}

// Common Table Expression
func BenchmarkQueryCTE(b *testing.B) {
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        mustQuery(b, db, `
            WITH cte AS (
                SELECT id, val FROM t WHERE val > 10
            )
            SELECT * FROM cte WHERE id > 5
        `)
    }
}
```

---

## Wave 7: Index & Query Optimization

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| IndexSelectPoint | Point query with index | 10x vs scan |
| IndexSelectRange | Range query with index | ops/sec |
| IndexSelectUnique | Unique index lookup | ops/sec |
| IndexCovered | Covered index query | ops/sec |
| IndexComposite | Multi-column index | ops/sec |

---

## Wave 8: Scale & Stress

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| Scale10KRows | 10K rows SELECT | <100ms |
| Scale100KRows | 100K rows SELECT | <1s |
| ScaleBulkInsert10K | Bulk insert 10K rows | <5s |
| ScaleMultiTableJoin | 3+ table JOIN | ops/sec |

---

## Wave 9: Memory & Cache

**Status**: Pending

### Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| MemoryCacheHit | Sequential read cache hit | >90% |
| MemoryCacheMiss | Random read cache miss | <10% |
| MemoryPerQuery | Peak memory per query | KB/query |

---

## Wave 10: Verification

**Status**: Pending

### Tasks
- Run all new benchmarks
- Generate flame graph / profile
- Identify top 10 bottlenecks
- Document findings
- Add to CI pipeline

---

## Performance Profiling Goals

| Layer | Key Metrics | Target Issues to Find |
|-------|-------------|----------------------|
| DS | B-Tree depth, page splits, encoding time | 3+ issues |
| VM | Instruction count, register pressure | 3+ issues |
| QP | Tokenize time, parse time, AST size | 2+ issues |
| TM | Lock time, commit time, WAL throughput | 2+ issues |

---

## Files to Create

- `internal/TS/Benchmark/benchmark_ds_test.go` - DS layer benchmarks
- `internal/TS/Benchmark/benchmark_vm_test.go` - VM layer benchmarks
- `internal/TS/Benchmark/benchmark_qp_test.go` - QP layer benchmarks
- `internal/TS/Benchmark/benchmark_tm_test.go` - TM layer benchmarks
- `internal/TS/Benchmark/benchmark_edge_test.go` - Edge case benchmarks
- `internal/TS/Benchmark/benchmark_complex_test.go` - Complex query benchmarks

---

## Success Criteria

- [ ] 49 new benchmark tests added
- [ ] All layers (DS, VM, QP, TM) benchmarked
- [ ] Top 10 performance bottlenecks identified
- [ ] Flame graph / profiling data captured
- [ ] Optimization recommendations documented

---

## Timeline Estimate

| Wave | Feature | Estimated Hours |
|------|---------|-----------------|
| 1 | DS Layer | 3 |
| 2 | VM Layer | 2 |
| 3 | QP Layer | 2 |
| 4 | TM Layer | 2 |
| 5 | Edge Cases | 2 |
| 6 | Complex Queries | 2 |
| 7 | Index & Query | 2 |
| 8 | Scale & Stress | 2 |
| 9 | Memory & Cache | 1 |
| 10 | Verification | 3 |

**Total**: ~19 hours
