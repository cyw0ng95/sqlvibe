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

internal/QP/
├── tokenizer.go          # Update: add normalization
├── parser.go             # Update: memoization, constant folding
├── normalize.go          # NEW: query normalization
├── type_infer.go         # NEW: type inference
└── dag.go               # NEW: DAG query plan

internal/DS/              # REMOVE: legacy SQLite format
├── btree.go             # DELETE
├── page.go              # DELETE
├── encoding.go           # DELETE
├── overflow.go          # DELETE
├── freelist.go          # DELETE
└── balance.go           # DELETE
```

---

## Tasks

### Phase 1: Columnar VM Opcodes
- [x] Add OpColumnarScan opcode
- [x] Add OpColumnarFilter opcode
- [x] Add OpColumnarAgg opcode
- [x] Add OpTopK opcode
- [x] Unit tests for each opcode
- [x] Benchmark: ColumnarScan 1K rows

### Phase 2: CG Columnar Plan Generation
- [x] Implement shouldUseColumnar() detector
- [x] Generate OpColumnarAgg for COUNT/SUM
- [x] Generate OpColumnarScan for SELECT *
- [x] Benchmark: CG Columnar COUNT/SUM
- [x] Benchmark: CG plan generation time

### Phase 3: Filter Pushdown
- [x] Implement ScanWithFilter in storage
- [x] Generate OpColumnarFilter in CG
- [x] Benchmark: WHERE col = val
- [x] Benchmark: WHERE col > val

### Phase 4: Early Termination
- [x] Implement OpTopK for LIMIT
- [x] Implement OpExistsShortCircuit
- [x] Benchmark: LIMIT 10
- [x] Benchmark: EXISTS subquery

### Phase 5: Predicate Reordering
- [x] Implement selectivity estimation
- [x] Reorder predicates in CG (ReorderPredicates)
- [x] Benchmark: Multi-predicate WHERE

### Phase 6: QP (Query Processing) Optimizations
- [x] Implement NormalizeQuery (normalize.go)
- [x] Implement InferExprType (type_infer.go)
- [x] Implement ParseCached LRU cache (parse_cache.go)

#### 6.1 Query Normalization

Normalize SQL for better cache hit:

```go
var queryNormalizer = regexp.MustCompile(`'[^']*'|[0-9]+`)

func NormalizeQuery(sql string) string {
    sql = strings.ToLower(sql)
    sql = strings.TrimSpace(sql)
    // Replace literals with placeholder
    sql = queryNormalizer.ReplaceAllString(sql, "?")
    return sql
}

// SELECT * FROM users WHERE id = 1
// → select * from users where id = ?
```

#### 6.2 Parser Memoization

Cache parsed ASTs for repeated queries:

```go
type LRUCache struct {
    capacity int
    items    map[string]*list.Element
    list     *list.List
}

var parseCache = NewLRUCache(1000)

func Parse(sql string) (*SelectStmt, error) {
    normalized := NormalizeQuery(sql)
    if cached, ok := parseCache.Get(normalized); ok {
        return cached.(*SelectStmt), nil
    }
    result, err := doParse(sql)
    if err == nil {
        parseCache.Set(normalized, result)
    }
    return result, err
}
```

#### 6.3 Constant Folding

Fold constants at parse time:

```go
func FoldConstants(expr Expr) Expr {
    switch e := expr.(type) {
    case *BinaryExpr:
        left := FoldConstants(e.Left)
        right := FoldConstants(e.Right)
        if isConstant(left) && isConstant(right) {
            return &Literal{Value: evaluate(left, right)}
        }
        return &BinaryExpr{Op: e.Op, Left: left, Right: right}
    case *UnaryExpr:
        child := FoldConstants(e.Child)
        if isConstant(child) {
            return &Literal{Value: evaluateUnary(e.Op, child)}
        }
        return &UnaryExpr{Op: e.Op, Child: child}
    }
    return expr
}

// SELECT * FROM t WHERE id = 5 + 3
// Parsed as: WHERE id = (5 + 3)
// After fold: WHERE id = 8
```

#### 6.4 Type Inference

Infer column types at parse time:

```go
func InferExprType(expr Expr, schema map[string]ColumnType) ColumnType {
    switch e := expr.(type) {
    case *ColumnRef:
        return schema[e.Name]
    case *Literal:
        return inferFromValue(e.Value)
    case *BinaryExpr:
        left := InferExprType(e.Left, schema)
        right := InferExprType(e.Right, schema)
        return promoteTypes(left, right)
    case *FuncCall:
        return getFuncReturnType(e.Name)
    }
    return TypeAny
}
```

#### 6.5 Streaming INSERT

Parse and emit rows incrementally for large datasets:

```go
func (p *Parser) ParseInsertStream(table string, emit func([]interface{})) error {
    for {
        row, err := p.parseInsertRow()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }
        emit(row) // Stream directly to storage
    }
    return nil
}
```

#### 6.6 Benchmark Requirements

| Benchmark | Target | vs v0.8.0 |
|-----------|--------|------------|
| Parse cached query | < 100 ns | 10x faster |
| Parse normalized query | < 500 ns | Similar |
| Constant folding | < 1 µs | Negligible overhead |
| INSERT 1M rows stream | < 100 ms | 2x faster |

### Phase 8: Multi-Core Parallelization
- [x] Implement ParallelCount in HybridStore (parallel.go)
- [x] Implement ParallelSum in HybridStore (parallel.go)
- [x] Implement ParallelScan in HybridStore (parallel.go)
- [x] Implement WorkerPool (worker_pool.go)

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

#### Phase 7: Concurrent DAG Query Plan
- [x] Implement DAGNode and DAGExecutor (dag.go)

Build DAG of operators for parallel execution:

```go
type DAGNode struct {
    ID       int
    Op       Operator
    Inputs   []*DAGNode
    Ready    chan struct{}
    Result   interface{}
}

type DAGExecutor struct {
    nodes []*DAGNode
    cores int
}

// SELECT * FROM a JOIN b ON a.id = b.id WHERE a.x > 5
//
//    [Scan a]    [Scan b]     ← Can run in parallel
//        \         /
//       [Filter]            ← After scans
//          |
//       [HashJoin]         ← After filter
//          |
//       [Project]           ← After join
```

##### 7.1 DAG Compiler

```go
func (cg *Compiler) BuildDAG(stmt *SelectStmt) *DAG {
    dag := &DAG{nodes: make([]*DAGNode, 0)}
    
    // Build nodes from operators
    scanA := dag.AddNode(OpScan{Table: "a"})
    scanB := dag.AddNode(OpScan{Table: "b"})
    filter := dag.AddNode(OpFilter{Predicate: stmt.Where})
    join := dag.AddNode(OpHashJoin{Key: "id"})
    project := dag.AddNode(OpProject{Columns: stmt.Columns})
    
    // Add dependencies
    dag.AddEdge(scanA, filter)
    dag.AddEdge(scanB, filter)
    dag.AddEdge(filter, join)
    dag.AddEdge(join, project)
    
    return dag
}
```

##### 7.2 DAG Executor with Work Stealing

```go
func (e *DAGExecutor) Execute() {
    ready := make(chan *DAGNode, e.cores)
    
    // Phase 1: Schedule nodes with no dependencies
    for _, node := range e.nodes {
        if len(node.Inputs) == 0 {
            ready <- node
        }
    }
    
    // Phase 2: Execute in parallel
    var wg sync.WaitGroup
    for i := 0; i < e.cores; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for node := range ready {
                node.Execute()
                // Check if dependents are now ready
                for _, dep := range node.Outputs {
                    if dep.IsReady() {
                        ready <- dep
                    }
                }
            }
        }()
    }
    wg.Wait()
}
```

##### 7.3 Benchmark Requirements

| Benchmark | Target | vs Sequential |
|-----------|--------|---------------|
| 3-way JOIN | < 15 ms | 2x faster |
| Parallel subqueries | < 5 ms | 2x faster |
| Wide projection | < 2 ms | 2x faster |

### Phase 10: Remove Legacy Format Support
> **DEFERRED**: internal/DS is actively imported by 16+ files. Removal is out of scope for v0.8.1.

#### 10.1 Remove SQLite-compatible Code

- Remove `internal/DS/btree.go` (old B-Tree storage)
- Remove `internal/DS/page.go` (SQLite page format)
- Remove `internal/DS/encoding.go` (SQLite varint encoding)
- Remove `internal/DS/overflow.go` (SQLite overflow pages)
- Remove `internal/DS/freelist.go` (SQLite freelist)
- Remove `internal/DS/balance.go` (B-Tree balancing)

#### 10.2 Keep Only Storage Layer

```go
pkg/sqlvibe/storage/
├── hybrid_store.go      # Main storage (KEEP)
├── column_store.go      # Column vectors (KEEP)
├── row_store.go        # Row storage (KEEP)
├── column_vector.go    # Vector types (KEEP)
├── index_engine.go     # RoaringBitmap indexes (KEEP)
├── roaring_bitmap.go   # Bitmap indexes (KEEP)
├── skip_list.go        # Ordered data (KEEP)
├── arena.go            # Memory allocator (KEEP)
├── persistence.go      # New binary format (KEEP)
└── value.go           # Value types (KEEP)
```

#### 10.3 Clean Up

- Remove SQLite header parsing
- Remove page type constants (0x0d, 0x02, etc.)
- Remove cell encoding/decoding
- Simplify PageManager interface

#### 10.4 Benchmark Requirements

| Benchmark | Target |
|-----------|--------|
| Build time | < 5s |
| Binary size | < 5 MB |
| No regressions | 0 |

### Phase 11: Validation
- [x] Run full SQL:1999 test suite
- [x] Run SQLite comparison benchmarks
- [x] Benchmark vs v0.8.0
- [ ] Update README.md with v0.8.1 vs SQLite comparison
- [x] Update HISTORY.md

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
| Parse cached query | < 100 ns |
| 3-way JOIN (parallel) | < 15 ms (2x vs sequential) |
| Binary size | < 5 MB |
| Allocations/query | < 5 |
| No regressions | 0 |
| SQLite comparison | Added to README.md |

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
| 6 | QP Optimizations | 15 |
| 7 | Multi-Core Partition | 25 |
| 8 | Concurrent DAG Plan | 20 |
| 9 | Remove Legacy Format | 10 |
| 10 | Validation | 10 |

**Total**: ~150 hours

---

## Notes

- Focus on hot-path optimizations first (COUNT, SUM, SELECT *)
- Maintain backward compatibility with row-mode queries
- Profile before and after each change
- Keep CG simple: detect patterns, generate optimized bytecode
