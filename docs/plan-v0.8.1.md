# Plan v0.8.1 - CG & VM Optimizations

## Summary

Optimize the Code Generator (CG) and Virtual Machine (VM) to leverage the v0.8.0 columnar storage layer. Focus on reducing row-by-row processing and generating more efficient query plans.

**Previous**: v0.8.0 delivers new columnar storage architecture

---

## Phase 1: Columnar VM Opcodes

### 1.1 New Columnar Opcodes

Add VM opcodes that operate on column vectors directly:

| Opcode | Description | Target Query |
|--------|-------------|--------------|
| `OpColumnarScan` | Full scan returning column vectors | `SELECT *` |
| `OpColumnarFilter` | Filter on column vector | `WHERE col = val` |
| `OpColumnarAgg` | Aggregate on vector | `COUNT(*), SUM(col)` |
| `OpColumnarProject` | Project columns from vectors | `SELECT col1, col2` |

### 1.2 Implementation

```go
// New VM opcode for columnar aggregate
type OpColumnarAgg struct {
    ColIdx  int
    AggType int // COUNT=0, SUM=1, MIN=2, MAX=3
    DestReg int
}

func (vm *VM) OpColumnarAgg(inst Instruction) {
    vec := vm.columnVectors[inst.ColIdx]
    var result interface{}
    switch inst.AggType {
    case 0: // COUNT
        result = len(vec.Int64s)
    case 1: // SUM
        var sum int64
        for _, v := range vec.Int64s {
            sum += v
        }
        result = sum
    }
    vm.registers[inst.DestReg] = result
}
```

### 1.3 Benchmark Requirements

| Benchmark | Target |
|-----------|--------|
| ColumnarScan 1K rows | < 1 µs |
| ColumnarFilter 1K rows | < 500 ns |
| ColumnarCOUNT 1K rows | < 200 ns |
| ColumnarSUM 1K rows | < 300 ns |

---

## Phase 2: CG Columnar Plan Generation

### 2.1 Analytical Query Detection

Detect queries that benefit from columnar execution:

```go
func shouldUseColumnar(stmt *SelectStmt) bool {
    // Pure aggregates without complex joins
    if hasAggregate(stmt) && !hasJoin(stmt) {
        return true
    }
    // Full table scan without filter
    if !hasWhere(stmt) && !hasJoin(stmt) {
        return true
    }
    return false
}
```

### 2.2 Generate Columnar Opcodes

When columnar is detected, generate specialized opcodes:

```go
// SELECT COUNT(*) FROM t → OpColumnarAgg
func (cg *Compiler) compileColumnarAgg(stmt *SelectStmt) []Instruction {
    return []Instruction{
        {Op: OpColumnarScan, P1: tableID},
        {Op: OpColumnarAgg, P1: colIdx, P2: COUNT, P3: destReg},
    }
}
```

### 2.3 Benchmark Requirements

| Benchmark | Target | vs v0.8.0 |
|-----------|--------|------------|
| CG Columnar COUNT | < 500 ns | 2x faster |
| CG Columnar SUM | < 800 ns | 2x faster |
| CG Plan generation | < 50 µs | Similar |

---

## Phase 3: Filter Pushdown

### 3.1 Push WHERE to Storage Layer

Move filter evaluation from VM to storage:

```
Current:  VM fetches all rows → iterates → applies filter
Optimized: Storage returns only matching row indices
```

### 3.2 Implementation

```go
// In storage layer
func (hs *HybridStore) ScanWithFilter(col string, pred Predicate) []int64 {
    vec := hs.columns[col]
    // Filter on column vector, return matching indices
    return vectorizedFilter(vec, pred)
}

// In CG - generate filter opcode
func (cg *Compiler) compileWhere(stmt *SelectStmt) []Instruction {
    if simplePredicate(stmt.Where) {
        return []Instruction{
            {Op: OpColumnarScan, P1: tableID},
            {Op: OpColumnarFilter, P1: colIdx, P2: predicateReg},
        }
    }
    // Fall back to current row-by-row
    return cg.compileRowFilter(stmt.Where)
}
```

### 3.3 Benchmark Requirements

| Benchmark | Target | vs v0.8.0 |
|-----------|--------|------------|
| WHERE col = val (1K) | < 500 ns | 2x faster |
| WHERE col > val (1K) | < 1 µs | 1.5x faster |

---

## Phase 4: Early Termination

### 4.1 LIMIT Optimization

Generate optimized opcodes for LIMIT queries:

```go
// SELECT * FROM t LIMIT 10
// Current: Scan all, sort, return top 10
// Optimized: Use heap during scan, stop early

func (cg *Compiler) compileLimit(stmt *SelectStmt) []Instruction {
    if stmt.Limit > 0 && stmt.Limit < 100 {
        return []Instruction{
            {Op: OpColumnarScan, P1: tableID},
            {Op: OpTopK, P1: stmt.Limit}, // Heap-based top-K
            {Op: OpResultRow, P1: 0, P2: stmt.Limit},
        }
    }
    // ... existing logic
}
```

### 4.2 EXISTS Optimization

```go
// EXISTS (SELECT 1 FROM t WHERE ...)
// Current: Evaluate all rows
// Optimized: Short-circuit on first match

func (cg *Compiler) compileExists(subq *Subquery) []Instruction {
    return []Instruction{
        {Op: OpColumnarScan, P1: tableID},
        {Op: OpExistsShortCircuit, P1: subq.Reg}, // Stop at first match
    }
}
```

### 4.3 Benchmark Requirements

| Benchmark | Target | vs v0.8.0 |
|-----------|--------|------------|
| LIMIT 10 (1K rows) | < 300 ns | 2x faster |
| EXISTS subquery | < 1 µs | 3x faster |

---

## Phase 5: Predicate Reordering

### 5.1 CG Predicate Optimization

Reorder WHERE conditions by selectivity:

```go
func (cg *Compiler) reorderPredicates(exprs []Expr) []Expr {
    // Sort by selectivity (estimated)
    // Equality (=) > Range (>, <) > Like > Regex
    
    sort.Slice(exprs, func(i, j int) bool {
        return selectivity(exprs[i]) > selectivity(exprs[j])
    })
    return exprs
}
```

### 5.2 Benchmark Requirements

| Benchmark | Target | vs v0.8.0 |
|-----------|--------|------------|
| Multi-predicate WHERE | < 1 µs | 1.3x faster |

---

## Performance Targets Summary

### v0.8.1 vs v0.8.0 Comparison

| Operation | v0.8.0 | v0.8.1 Target | Speedup |
|-----------|--------|---------------|---------|
| COUNT(*) | 661 ns | < 200 ns | 3x |
| SUM(col) | 679 ns | < 300 ns | 2x |
| SELECT * (1K) | 578 ns | < 300 ns | 2x |
| WHERE = (1K) | 731 ns | < 500 ns | 1.5x |
| LIMIT 10 | 940 ns | < 300 ns | 3x |
| EXISTS | 850 ns | < 1 µs | 1.5x |

### Memory Targets

| Metric | Target |
|--------|--------|
| Allocations/query | < 5 |
| Memory/result | < 100 bytes |

---

## Files to Modify

```
pkg/sqlvibe/
├── storage/
│   ├── column_store.go     # Update: vectorized filter
│   ├── parallel.go          # NEW: parallel scan/aggregate
│   └── worker_pool.go      # NEW: thread pool
├── exec_columnar.go        # Update: new columnar execution
└── vm_exec.go             # Update: integrate columnar path

internal/VM/
├── opcodes.go             # Update: add columnar opcodes
├── engine.go               # Update: implement columnar ops
├── exec.go                 # Update: optimize hot paths
└── compiler.go            # Update: columnar plan generation

internal/CG/
├── compiler.go            # Update: detect analytical queries
├── plan_cache.go         # Update: cache columnar plans
└── optimizer.go          # Update: predicate reordering
```

---

## Tasks

### Phase 1: Columnar VM Opcodes
- [ ] Add OpColumnarScan opcode
- [ ] Add OpColumnarFilter opcode
- [ ] Add OpColumnarAgg opcode
- [ ] Unit tests for each opcode
- [ ] Benchmark: ColumnarScan 1K rows

### Phase 2: CG Columnar Plan Generation
- [ ] Implement shouldUseColumnar() detector
- [ ] Generate OpColumnarAgg for COUNT/SUM
- [ ] Generate OpColumnarScan for SELECT *
- [ ] Benchmark: CG Columnar COUNT/SUM
- [ ] Benchmark: CG plan generation time

### Phase 3: Filter Pushdown
- [ ] Implement ScanWithFilter in storage
- [ ] Generate OpColumnarFilter in CG
- [ ] Benchmark: WHERE col = val
- [ ] Benchmark: WHERE col > val

### Phase 4: Early Termination
- [ ] Implement OpTopK for LIMIT
- [ ] Implement OpExistsShortCircuit
- [ ] Benchmark: LIMIT 10
- [ ] Benchmark: EXISTS subquery

### Phase 5: Predicate Reordering
- [ ] Implement selectivity estimation
- [ ] Reorder predicates in CG
- [ ] Benchmark: Multi-predicate WHERE

### Phase 6: Multi-Core Parallelization

#### 6.1 Core Detection

Detect available CPU cores at startup:

```go
import "runtime"

var numCores int

func init() {
    numCores = runtime.GOMAXPROCS(0)
}

func GetNumCores() int {
    return numCores
}
```

#### 6.2 Adaptive Parallel Threshold

Automatically use parallel processing when dataset > threshold:

```go
const (
    ParallelThreshold = 10000  // Use parallel if > 10K rows
    MinPartitionSize = 1000   // Minimum rows per partition
)

func shouldParallelize(rowCount int) bool {
    return rowCount > ParallelThreshold && numCores > 1
}

func getNumWorkers(rowCount int) int {
    if rowCount < ParallelThreshold {
        return 1
    }
    maxWorkers := rowCount / MinPartitionSize
    if maxWorkers > numCores {
        maxWorkers = numCores
    }
    return maxWorkers
}
```

#### 6.3 Parallel Column Aggregation

```go
func (hs *HybridStore) ParallelSum(col string) int64 {
    numWorkers := getNumWorkers(hs.RowCount())
    if numWorkers == 1 {
        return hs.Sum(col) // Single-core fallback
    }
    
    partitionSize := hs.RowCount() / numWorkers
    partialSums := make(chan int64, numWorkers)
    
    var wg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            start := workerID * partitionSize
            end := start + partitionSize
            if workerID == numWorkers-1 {
                end = hs.RowCount() // Last partition gets remainder
            }
            partialSums <- hs.sumRange(col, start, end)
        }(i)
    }
    
    wg.Wait()
    close(partialSums)
    
    var total int64
    for sum := range partialSums {
        total += sum
    }
    return total
}
```

#### 6.4 Parallel Table Scan

```go
func (hs *HybridStore) ParallelScan() [][]interface{} {
    numWorkers := getNumWorkers(hs.RowCount())
    if numWorkers == 1 {
        return hs.Scan()
    }
    
    partitionSize := hs.RowCount() / numWorkers
    results := make(chan [][]interface{}, numWorkers)
    
    var wg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            start := workerID * partitionSize
            end := start + partitionSize
            if workerID == numWorkers-1 {
                end = hs.RowCount()
            }
            results <- hs.scanRange(start, end)
        }(i)
    }
    
    wg.Wait()
    close(results)
    
    // Merge results
    var merged [][]interface{}
    for partition := range results {
        merged = append(merged, partition...)
    }
    return merged
}
```

#### 6.5 Parallel Hash Join

```go
func ParallelHashJoin(a, b *HybridStore, key string) []Row {
    numWorkers := getNumWorkers(a.RowCount() + b.RowCount())
    if numWorkers == 1 {
        return hashJoinSequential(a, b, key)
    }
    
    // Partition by hash
    aParts := partitionByHash(a, numWorkers)
    bParts := partitionByHash(b, numWorkers)
    
    results := make(chan []Row, numWorkers)
    var wg sync.WaitGroup
    
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            results <- hashJoinPartition(aParts[i], bParts[i], key)
        }(i)
    }
    
    wg.Wait()
    close(results)
    
    // Merge
    var merged []Row
    for r := range results {
        merged = append(merged, r...)
    }
    return merged
}
```

#### 6.6 Work Stealing Thread Pool

For concurrent query execution:

```go
type WorkerPool struct {
    workers  int
    tasks    chan func()
    results  chan interface{}
}

func NewWorkerPool(workers int) *WorkerPool {
    return &WorkerPool{
        workers: workers,
        tasks:   make(chan func(), 1000),
        results: make(chan interface{}, 100),
    }
}

func (wp *WorkerPool) Submit(task func()) {
    wp.tasks <- task
}
```

#### 6.7 Benchmark Requirements

| Benchmark | Target | vs Single-Core |
|-----------|--------|---------------|
| Parallel COUNT 100K | < 5 ms | 4x faster |
| Parallel SUM 100K | < 6 ms | 4x faster |
| Parallel Scan 100K | < 10 ms | 4x faster |
| Parallel JOIN | < 20 ms | 3x faster |

### Phase 7: Validation
- [ ] Run full SQL:1999 test suite
- [ ] Run SQLite comparison tests
- [ ] Benchmark vs v0.8.0
- [ ] Update HISTORY.md

---

## Success Criteria

| Criteria | Target |
|----------|--------|
| SQL:1999 tests pass | 100% |
| COUNT(*) | < 200 ns (3x faster) |
| SUM(col) | < 300 ns (2x faster) |
| SELECT * (1K) | < 300 ns (2x faster) |
| LIMIT 10 | < 300 ns (3x faster) |
| Parallel COUNT 100K | < 5 ms (4x vs single-core) |
| Allocations/query | < 5 |
| No regressions | 0 |

---

## Benchmark Commands

```bash
# Run benchmarks
go test ./internal/TS/Benchmark/... -bench=. -benchtime=3s -benchmem

# Compare with v0.8.0
go test ./internal/TS/Benchmark/... -bench="COUNT|SUM|LIMIT" -benchtime=3s

# Memory profiling
go test ./... -bench=BenchmarkSelectAll -memprofile=mem.prof
```

---

## Timeline Estimate

| Phase | Tasks | Hours |
|-------|-------|-------|
| 1 | Columnar VM Opcodes | 15 |
| 2 | CG Columnar Plans | 20 |
| 3 | Filter Pushdown | 15 |
| 4 | Early Termination | 10 |
| 5 | Predicate Reordering | 10 |
| 6 | Multi-Core Parallelization | 25 |
| 7 | Validation | 10 |

**Total**: ~105 hours

---

## Notes

- Focus on hot-path optimizations first (COUNT, SUM, SELECT *)
- Maintain backward compatibility with row-mode queries
- Profile before and after each change
- Keep CG simple: detect patterns, generate optimized bytecode
