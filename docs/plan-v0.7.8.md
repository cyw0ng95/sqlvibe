# Plan v0.7.8 - VM & CG Performance Optimizations

## Summary

Implement targeted performance optimizations for VM and CG subsystems, including branch prediction, caching strategies, query optimizations, and hash join. Benchmark against go-sqlite for comparison.

**Previous**: v0.7.7 delivers QP & DS performance optimizations

---

## Optimization Targets

### VM Runtime Optimizations

| Optimization | Description | Difficulty | Impact |
|-------------|-------------|------------|--------|
| Branch Prediction | Predict loop branches (OpNext/OpRewind) | Low | High |
| Result Cache | Materialized subquery results | Low | High |
| Async Prefetch | BTree page async loading | Medium | Medium |
| String Interning | Pool repeated strings | Low | Medium |

### CG Query Optimizations

| Optimization | Description | Difficulty | Impact |
|-------------|-------------|------------|--------|
| Plan Cache | Compile once, execute many | Low | High |
| Result Cache | Full query result caching | Low | High |
| Top-N Optimization | Heap-based LIMIT optimization | Low | Medium |
| Predicate Pushdown | Push WHERE to storage layer | Medium | High |
| Hash Join | Alternative to nested loop join | Medium | High |

### Benchmark & Comparison

| Task | Description |
|------|-------------|
| SQLite Benchmark | Compare with go-sqlite driver |
| Performance Report | Document improvements |

---

## Implementation Plan

### Phase 1: VM Runtime Optimizations

#### 1.1 Branch Prediction

```go
// VM/exec.go - Add predictor
type BranchPredictor struct {
    table [1024]uint8  // 2-bit saturating counter
}

// Predict: 1 = strongly taken, 2 = taken, 3 = strongly not taken
func (bp *BranchPredictor) Predict(pc int) bool {
    return bp.table[pc%1024] >= 2
}

func (bp *BranchPredictor) Update(pc int, taken bool) {
    idx := pc % 1024
    c := bp.table[idx]
    if taken {
        if c < 3 {
            c++
        }
    } else {
        if c > 0 {
            c--
        }
    }
    bp.table[idx] = c
}

// Apply to OpNext loop
case OpNext:
    // Predict branch taken (continue loop)
    if bp.Predict(vm.pc) {
        cursor.Index++
        if cursor.Index < len(cursor.Data) {
            vm.pc = target
            bp.Update(pc, true)
            continue
        }
    }
    bp.Update(pc, false)
```

#### 1.2 Result Cache

```go
// VM/result_cache.go
type ResultCache struct {
    mu    sync.RWMutex
    data  map[uint64]*CacheEntry
    limit int
    ttl   time.Duration
}

type CacheEntry struct {
    rows    [][]interface{}
    created time.Time
    query   string
}

func NewResultCache(limit int, ttl time.Duration) *ResultCache {
    return &ResultCache{
        data:  make(map[uint64]*CacheEntry),
        limit: limit,
        ttl:   ttl,
    }
}

func (rc *ResultCache) Get(key uint64) ([][]interface{}, bool) {
    rc.mu.RLock()
    defer rc.mu.RUnlock()
    
    if e, ok := rc.data[key]; ok {
        if time.Since(e.created) < rc.ttl {
            return e.rows, true
        }
    }
    return nil, false
}

func (rc *ResultCache) Set(key uint64, rows [][]interface{}) {
    rc.mu.Lock()
    defer rc.mu.Unlock()
    
    // Evict if full
    if len(rc.data) >= rc.limit {
        rc.evictOldest()
    }
    
    rc.data[key] = &CacheEntry{
        rows:    rows,
        created: time.Now(),
    }
}

func (rc *ResultCache) Invalidate() {
    rc.mu.Lock()
    defer rc.mu.Unlock()
    rc.data = make(map[uint64]*CacheEntry)
}
```

#### 1.3 Async Prefetch

```go
// DS/prefetch.go
type Prefetcher struct {
    degree int
    sem    chan struct{}
    pm     *PageManager
}

func NewPrefetcher(pm *PageManager, degree int) *Prefetcher {
    return &Prefetcher{
        degree: degree,
        sem:    make(chan struct{}, degree),
        pm:     pm,
    }
}

func (p *Prefetcher) Prefetch(pageNum uint32) {
    select {
    case p.sem <- struct{}{}:
        go func() {
            defer func() { <-p.sem }()
            p.pm.ReadPage(pageNum) //nolint:errcheck
        }()
    default:
        // Pool full, skip
    }
}

// Apply in BTree search
func (bt *BTree) searchPage(page *Page, key []byte) ([]byte, error) {
    // Prefetch child pages
    if bt.prefetcher != nil && isInteriorPage(page) {
        childPages := bt.getChildPageNumbers(page, 4)
        for _, cp := range childPages {
            bt.prefetcher.Prefetch(cp)
        }
    }
    // ... rest of search
}
```

#### 1.4 String Interning

```go
// VM/string_pool.go
var stringPool = sync.Map{}

func InternString(s string) string {
    if v, ok := stringPool.Load(s); ok {
        return v.(string)
    }
    stringPool.Store(s, s)
    return s
}

// Apply in string operations
func stringConcat(a, b interface{}) string {
    sa := toString(a)
    sb := toString(b)
    return InternString(sa + sb)
}
```

---

### Phase 2: CG Query Optimizations

#### 2.1 Plan Cache

```go
// CG/plan_cache.go
type PlanCache struct {
    mu    sync.RWMutex
    data  map[string]*CachedPlan
    limit int
}

type CachedPlan struct {
    program   *VM.Program
    createdAt time.Time
    hits      int
}

func NewPlanCache(limit int) *PlanCache {
    return &PlanCache{
        data:  make(map[string]*CachedPlan),
        limit: limit,
    }
}

func (pc *PlanCache) Get(sql string) (*VM.Program, bool) {
    pc.mu.RLock()
    defer pc.mu.RUnlock()
    
    if plan, ok := pc.data[sql]; ok {
        plan.hits++
        return plan.program, true
    }
    return nil, false
}

func (pc *PlanCache) Put(sql string, program *VM.Program) {
    pc.mu.Lock()
    defer pc.mu.Unlock()
    
    if len(pc.data) >= pc.limit {
        pc.evictLRU()
    }
    pc.data[sql] = &CachedPlan{
        program:   program,
        createdAt: time.Now(),
    }
}

func (pc *PlanCache) Invalidate() {
    pc.mu.Lock()
    defer pc.mu.Unlock()
    pc.data = make(map[string]*CachedPlan)
}
```

#### 2.2 Result Cache (Full Query)

```go
// QE/result_cache.go
type QueryResultCache struct {
    mu     sync.RWMutex
    data   map[uint64]*QueryCacheEntry
    limit  int
    ttl    time.Duration
}

type QueryCacheEntry struct {
    result [][]interface{}
    cols   []string
    created time.Time
}

// Cache key: hash of SQL + bindings
func (qc *QueryResultCache) Get(sqlHash uint64) ([][]interface{}, bool) {
    qc.mu.RLock()
    defer qc.mu.RUnlock()
    
    if e, ok := qc.data[sqlHash]; ok {
        if time.Since(e.created) < qc.ttl {
            return e.result, true
        }
    }
    return nil, false
}

// Invalidate on any write
func (qe *QueryEngine) OnWrite() {
    qe.resultCache.Invalidate()
}
```

#### 2.3 Top-N Optimization

```go
// VM/topn.go
type TopN struct {
    n    int
    heap *minHeap
}

type minHeap struct {
    data [][]interface{}
    less func(a, b []interface{}) bool
}

func NewTopN(n int, less func(a, b []interface{}) bool) *TopN {
    return &TopN{
        n:    n,
        heap: &minHeap{data: make([][]interface{}, 0, n), less: less},
    }
}

func (tn *TopN) Push(row []interface{}) {
    if len(tn.heap.data) < tn.n {
        heap.Push(tn.heap, row)
    } else if tn.heap.less(tn.heap.data[0], row) {
        heap.Pop(tn.heap)
        heap.Push(tn.heap, row)
    }
}

func (tn *TopN) Result() [][]interface{} {
    sort.Sort(tn.heap)
    return tn.heap.data
}

// Apply: SELECT * FROM t ORDER BY col LIMIT N
func (vm *VM) optimizeTopN(stmt *QP.SelectStmt) bool {
    if stmt.Limit != nil && stmt.OrderBy != nil {
        // Use TopN instead of full sort
        return true
    }
    return false
}
```

#### 2.4 Predicate Pushdown

```go
// QP/optimizer.go
type PredicatePushdown struct{}

func (ppd *PredicatePushdown) Optimize(stmt *QP.SelectStmt) *QP.SelectStmt {
    if stmt.From == nil {
        return stmt
    }
    
    // Split WHERE into pushdown vs keep
    pushdown, remaining := ppd.splitPredicate(stmt.Where)
    stmt.Where = remaining
    
    // Attach pushdown predicates to table scan
    stmt.From.PushdownConditions = pushdown
    
    return stmt
}

func (ppd *PredicatePushdown) splitPredicate(where QP.Expr) (pushdown, remaining QP.Expr) {
    // Pushdown: column OP constant
    //   e.g., id = 100, age > 25
    // Keep: functions, subqueries
    //   e.g., func(col), col IN (SELECT ...)
    
    if where == nil {
        return nil, nil
    }
    
    switch expr := where.(type) {
    case *QP.BinaryExpr:
        if ppd.isPushable(expr) {
            return expr, nil
        }
        return nil, expr
    case *QP.UnaryExpr:
        push, keep := ppd.splitPredicate(expr.Expr)
        return push, &UnaryExpr{Op: expr.Op, Expr: keep}
    }
    
    return nil, where
}

func (ppd *PredicatePushdown) isPushable(expr *QP.BinaryExpr) bool {
    // Pushable: column OP constant
    // Not pushable: subquery, function, column OP column
    _, leftIsCol := expr.Left.(*QP.ColumnRef)
    _, rightIsConst := expr.Right.(*QP.Literal)
    return leftIsCol && rightIsConst
}
```

#### 2.5 Hash Join

```go
// QE/hash_join.go
type HashJoin struct {
    left   *HashTable
    right  [][]interface{}
    onCol  string
}

type HashTable struct {
    buckets map[interface{}][][]interface{}
}

func NewHashTable() *HashTable {
    return &HashTable{
        buckets: make(map[interface{}][][]interface{}),
    }
}

func (ht *HashTable) Build(rows [][]interface{}, keyCol int) {
    for _, row := range rows {
        key := row[keyCol]
        ht.buckets[key] = append(ht.buckets[key], row)
    }
}

func (ht *HashTable) Probe(key interface{}) [][]interface{} {
    return ht.buckets[key]
}

func HashJoin(left, right [][]interface{}, leftKey, rightKey int) [][]interface{} {
    // Build hash table from right (smaller) relation
    ht := NewHashTable()
    ht.Build(right, rightKey)
    
    // Probe with left relation
    results := [][]interface{}{}
    for _, lrow := range left {
        matches := ht.Probe(lrow[leftKey])
        for _, rrow := range matches {
            results = append(results, append(lrow, rrow...))
        }
    }
    
    return results
}
```

---

### Phase 3: Benchmark & Comparison

#### 3.1 Benchmark Suite

```
test/benchmark/
├── benchmark_test.go      # Main benchmark suite
├── sqlite_compare_test.go # Compare with go-sqlite
└── results/
    └── v0.7.8.txt       # Results
```

#### 3.2 Comparison Tests

```go
// Compare with go-sqlite
func BenchmarkSQLite_WhereFiltering(b *testing.B) {
    // go-sqlite implementation
    db, _ := sql.Open("sqlite3", ":memory:")
    db.Exec("CREATE TABLE t (id INT, x INT)")
    // ... populate data
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        db.Query("SELECT * FROM t WHERE x > 100")
    }
}

func BenchmarkSqlvibe_WhereFiltering(b *testing.B) {
    // sqlvibe implementation
    // ... same setup
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        qe.Query("SELECT * FROM t WHERE x > 100")
    }
}
```

#### 3.3 Key Benchmarks

| Benchmark | Target |
|-----------|--------|
| BenchmarkWhere_Filtering | WHERE clause performance |
| BenchmarkCount_Star | COUNT(*) optimization |
| BenchmarkJoin_NestedLoop | Current join |
| BenchmarkJoin_Hash | Hash join |
| BenchmarkPlanCache_Hit | Plan cache effectiveness |
| BenchmarkResultCache_Hit | Result cache effectiveness |
| BenchmarkTopN_Limit | Top-N optimization |
| BenchmarkPrefetch_TreeSearch | Async prefetch |

---

## Files to Create/Modify

```
internal/VM/
├── exec.go                       # Branch prediction
├── result_cache.go             # NEW - Result cache
├── string_pool.go              # NEW - String interning
└── bench_vm_test.go            # Update benchmarks

internal/DS/
├── prefetch.go                 # NEW - Async prefetch
├── btree.go                    # Integrate prefetch
└── bench_btree_test.go        # Update benchmarks

internal/CG/
├── plan_cache.go              # NEW - Plan cache
└── compiler.go                 # Integrate plan cache

internal/QP/
├── optimizer.go               # Predicate pushdown
└── topn.go                   # NEW - Top-N optimization

internal/QE/
├── hash_join.go               # NEW - Hash join
└── engine.go                  # Integrate optimizations

test/benchmark/
├── sqlite_compare_test.go     # NEW - SQLite comparison
└── benchmark_test.go         # Update benchmarks
```

---

## Tasks

### Phase 1: VM Runtime
- [ ] Implement branch prediction in exec.go
- [ ] Implement result cache in VM
- [ ] Implement async prefetch in DS
- [ ] Implement string interning
- [ ] Add VM benchmarks

### Phase 2: CG Optimizations
- [ ] Implement plan cache in CG
- [ ] Implement result cache in QE
- [ ] Implement Top-N optimization
- [ ] Implement predicate pushdown
- [ ] Implement hash join
- [ ] Add query optimization benchmarks

### Phase 3: Benchmark & Compare
- [ ] Create SQLite comparison tests
- [ ] Run full benchmark suite
- [ ] Compare with go-sqlite driver
- [ ] Document results
- [ ] Update HISTORY.md

---

## Success Criteria

| Criteria | Target |
|----------|--------|
| All existing tests pass | 100% |
| WHERE filtering improvement | >2x vs v0.7.7 |
| COUNT(*) optimization | >5x vs v0.7.7 |
| Plan cache hit rate | >80% for repeated queries |
| Hash join working | Correct results |
| SQLite comparison | Document differences |

---

## SQLite Full Comparison Requirements

### Required Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| **SELECT Performance** | 50+ queries | WHERE, ORDER BY, LIMIT, OFFSET |
| **Aggregate Functions** | 20+ queries | COUNT, SUM, AVG, MIN, MAX |
| **JOIN Operations** | 30+ queries | INNER, LEFT, RIGHT, CROSS |
| **Subqueries** | 20+ queries | Scalar, EXISTS, IN, correlated |
| **Data Types** | 30+ tests | Integer, REAL, TEXT, BLOB, NULL |
| **SQL Features** | 40+ tests | INSERT, UPDATE, DELETE, transactions |
| **Edge Cases** | 20+ tests | Empty tables, NULLs, duplicates |

### Detailed Test Cases (SQLite Compatibility)

#### WHERE Filtering Tests
```
| Test Case | SQL | Expected Rows |
|-----------|-----|----------------|
| where_int_eq | SELECT * FROM t WHERE id = 100 | 1 |
| where_int_gt | SELECT * FROM t WHERE id > 100 | 900 |
| where_int_lt | SELECT * FROM t WHERE id < 100 | 99 |
| where_int_between | SELECT * FROM t WHERE id BETWEEN 50 AND 150 | 101 |
| where_text_eq | SELECT * FROM t WHERE name = 'Alice' | 10 |
| where_null | SELECT * FROM t WHERE col IS NULL | 5 |
| where_and | SELECT * FROM t WHERE id > 50 AND id < 150 | 99 |
| where_or | SELECT * FROM t WHERE id = 1 OR id = 100 | 2 |
| where_not | SELECT * FROM t WHERE NOT id = 1 | 999 |
| where_like | SELECT * FROM t WHERE name LIKE 'A%' | 150 |
| where_in | SELECT * FROM t WHERE id IN (1, 10, 100) | 3 |
```

#### COUNT Aggregate Tests
```
| Test Case | SQL | Expected |
|-----------|-----|----------|
| count_star | SELECT COUNT(*) FROM t | 1000 |
| count_col | SELECT COUNT(col) FROM t | 995 |
| count_distinct | SELECT COUNT(DISTINCT col) FROM t | 100 |
| count_where | SELECT COUNT(*) FROM t WHERE id > 500 | 500 |
| sum_int | SELECT SUM(id) FROM t | 500500 |
| avg_int | SELECT AVG(id) FROM t | 500.5 |
| min_int | SELECT MIN(id) FROM t | 1 |
| max_int | SELECT MAX(id) FROM t | 1000 |
```

#### JOIN Tests
```
| Test Case | SQL | Expected Rows |
|-----------|-----|----------------|
| join_inner | SELECT * FROM t1 JOIN t2 ON t1.id = t2.id | 100 |
| join_left | SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id | 100 |
| join_two | SELECT * FROM t1, t2 WHERE t1.id = t2.id | 100 |
```

#### Subquery Tests
```
| Test Case | SQL | Expected |
|-----------|-----|----------|
| subquery_scalar | SELECT * FROM t WHERE id = (SELECT MAX(id) FROM t) | 1 |
| subquery_exists | SELECT EXISTS(SELECT 1 FROM t WHERE id > 500) | true |
| subquery_in | SELECT * FROM t WHERE id IN (SELECT id FROM t2) | 100 |
| subquery_notin | SELECT * FROM t WHERE id NOT IN (SELECT id FROM t2) | 0 |
```

### Performance Comparison Matrix

```
| Query Type          | go-sqlite (ms) | sqlvibe (ms) | Ratio | Notes |
|---------------------|----------------|---------------|-------|-------|
| SELECT * WHERE      |                |               |       |       |
| COUNT(*)            |                |               |       |       |
| JOIN two tables     |                |               |       |       |
| ORDER BY LIMIT 10  |                |               |       |       |
| Subquery IN        |                |               |       |       |
| ...                 |                |               |       |       |
```

### Accuracy Requirements

| Requirement | Target |
|------------|--------|
| Result match | 100% identical to SQLite |
| Error handling | Same error messages |
| Type affinity | Same as SQLite |
| NULL behavior | SQL standard compliant |

### Test Commands

```bash
# Run full SQLite comparison suite
go test ./test/sqllogictest/... -v

# Run specific comparison tests
go test ./test/benchmark/... -run CompareSQLite -v

# Generate comparison report
go test ./test/benchmark/... -compare -output results/v0.7.8.txt
```

---

## README.md Update Requirements

### Required Sections

After implementation, update README.md with:

```markdown
## Performance (v0.7.8)

### Benchmark Results

| Query Type | go-sqlite | sqlvibe | Speedup |
|-----------|-----------|---------|---------|
| SELECT WHERE (1000 rows) | X ms | Y ms | Zx |
| COUNT(*) | X ms | Y ms | Zx |
| COUNT(*) with WHERE | X ms | Y ms | Zx |
| JOIN two tables | X ms | Y ms | Zx |
| JOIN three tables | X ms | Y ms | Zx |
| ORDER BY LIMIT 10 | X ms | Y ms | Zx |
| Subquery IN | X ms | Y ms | Zx |
| Subquery EXISTS | X ms | Y ms | Zx |

### Optimization Features (v0.7.8)

| Feature | Status | Description |
|---------|--------|-------------|
| Branch Prediction | ✅ | 2-bit saturating counter for loop prediction |
| Result Cache | ✅ | Subquery result caching |
| Plan Cache | ✅ | Compiled query plan caching |
| Result Cache (Full Query) | ✅ | Full query result caching |
| Hash Join | ✅ | Alternative to nested loop join |
| Predicate Pushdown | ✅ | Push WHERE to storage layer |
| Top-N Optimization | ✅ | Heap-based LIMIT optimization |
| Async Prefetch | ✅ | BTree page prefetching |
| String Interning | ✅ | String pool for deduplication |

### SQLite Compatibility

| Test Suite | Status | Notes |
|------------|--------|-------|
| SQL Logic Test | ✅ PASS | All tests pass |
| WHERE Filtering | ✅ PASS | 100% match |
| COUNT Aggregate | ✅ PASS | 7.4x faster than v0.7.7 |
| JOIN Operations | ✅ PASS | Correct results |
| Subqueries | ✅ PASS | Correct results |
| Data Types | ✅ PASS | All types supported |
| NULL Handling | ✅ PASS | SQL standard compliant |

### Comparison with go-sqlite

```bash
# Run comparison benchmarks
$ go test ./test/benchmark/... -bench=Compare -benchmem

# Results
go-sqlite      sqlvibe       Operation
-----------    -----------    ----------------
0.12 ms        0.28 ms       SELECT WHERE (1000 rows)
0.08 ms        0.06 ms       COUNT(*) (7.4x faster)
0.45 ms        0.62 ms       JOIN two tables
0.31 ms        0.28 ms       ORDER BY LIMIT 10
```

### Performance Improvements

- WHERE filtering: 2.3x faster (with predicate pushdown)
- COUNT(*): 7.4x faster (with metadata optimization)
- Hash Join: 5-10x faster for large tables vs nested loop
- Plan Cache: 80%+ hit rate for repeated queries
- Result Cache: Significant speedup for identical queries
```

### Update Commands

```bash
# Generate benchmark report
go test ./test/benchmark/... -bench=. -benchmem -output benchmark.txt

# Run comparison
go test ./test/benchmark/... -compare -output comparison.txt

# Update README
# Manually update with results
```

---

## Benchmark Commands

```bash
# Run VM benchmarks
go test ./internal/VM/... -bench=. -benchmem

# Run DS benchmarks
go test ./internal/DS/... -bench=. -benchmem

# Run comparison benchmarks
go test ./test/benchmark/... -bench=. -benchmem

# Profile
go test ./internal/VM/... -bench=BenchmarkVM_WhereFiltering -cpuprofile=cpu.prof
go tool pprof cpu.prof
```

---

## Timeline Estimate

| Phase | Tasks | Estimated Hours |
|-------|-------|-----------------|
| 1 | VM Runtime Optimizations | 16 |
| 2 | CG Query Optimizations | 20 |
| 3 | Benchmark & Compare | 8 |

**Total**: ~44 hours

---

## Notes

- Focus on correctness first, then performance
- Use pprof to identify actual bottlenecks
- Compare results with SQLite for compatibility
- Document any intentional deviations

