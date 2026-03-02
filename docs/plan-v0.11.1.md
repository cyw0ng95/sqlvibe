# Phase 5: libsvdb Core Consolidation Plan

**Last Updated**: 2026-03-02  
**Target Version**: v0.12.0  
**Status**: 📋 Planned  
**Strategy**: Migrate remaining Go orchestration to C++ for fully self-contained libsvdb

---

## Executive Summary

The sqlvibe C++ migration has achieved **100% completion** of core operations (DS/VM/QP/CG layers). However, significant **orchestration logic remains in Go** — query execution coordination, result materialization, transaction management, and index operations.

**Phase 5 Goal**: Migrate all remaining Go orchestration to C++, making libsvdb a **fully self-contained database engine** with Go as a thin API wrapper only.

### Current State (v0.11.0)

✅ **Complete in C++** (9,000+ LOC):
- DS Layer: B-Tree, Columnar Store, Row Store, Overflow, WAL, Cache
- VM Layer: All 46 bytecode opcodes, expression eval, aggregates
- QP Layer: Full SQL parser, analyzer, binder
- CG Layer: Bytecode compiler, optimizer, plan cache

⚠️ **Remaining in Go** (~3,000 LOC orchestration):
- Query execution orchestration (SELECT/JOIN/AGG/SORT)
- Result row materialization
- Transaction coordination
- Index engine operations
- Information Schema views

### Target State (v0.12.0)

✅ **Fully C++ Engine**:
- libsvdb.so handles complete query execution end-to-end
- Go becomes thin wrapper (<500 LOC API binding)
- Zero Go logic in hot paths
- **5× average speedup** over SQLite (vs 3× currently)

---

## Migration Priorities

### P0: Critical — Query Execution Engine (15-20 days)

#### 5.1.1: SELECT Query Orchestration

**Current**: `internal/VM/engine/select.go` (Go)  
**Target**: `src/core/VM/engine/select.cpp`

**Functions to migrate**:
```go
// Go (current)
func FilterRows(rows []Row, pred func(Row) bool) []Row
func ProjectRows(rows []Row, projections map[string]func(Row) interface{}) []Row
func ApplyDistinct(rows []Row, keyFn func(Row) string) []Row
func ApplyLimitOffset(rows []Row, limit, offset int) []Row
```

```cpp
// C++ (target)
namespace svdb { namespace engine {

std::vector<Row> FilterRows(
    const std::vector<Row>& rows,
    std::function<bool(const Row&)> pred
);

std::vector<Row> ProjectRows(
    const std::vector<Row>& rows,
    const std::map<std::string, std::function<Value(const Row&)>>& projections
);

std::vector<Row> ApplyDistinct(
    const std::vector<Row>& rows,
    std::function<std::string(const Row&)> keyFn
);

std::vector<Row> ApplyLimitOffset(
    const std::vector<Row>& rows,
    int limit, int offset
);

}} // namespace svdb::engine
```

**Impact**: Eliminates Go map/slice operations in hot path  
**Estimated Effort**: 3-4 days

---

#### 5.1.2: JOIN Execution Engine

**Current**: `internal/VM/engine/join.go` (Go)  
**Target**: `src/core/VM/engine/join.cpp`

**Functions to migrate**:
```go
// Go (current)
func CrossJoin(left, right []Row) []Row
func InnerJoin(left, right []Row, pred func(Row) bool) []Row
func LeftOuterJoin(left, right []Row, pred func(Row) bool, rightCols []string) []Row
func MergeRows(a, b Row) Row
func MergeRowsWithAlias(a Row, aliasA string, b Row, aliasB string) Row
```

```cpp
// C++ (target)
namespace svdb { namespace engine {

std::vector<Row> CrossJoin(
    const std::vector<Row>& left,
    const std::vector<Row>& right
);

std::vector<Row> InnerJoin(
    const std::vector<Row>& left,
    const std::vector<Row>& right,
    std::function<bool(const Row&)> pred
);

std::vector<Row> LeftOuterJoin(
    const std::vector<Row>& left,
    const std::vector<Row>& right,
    std::function<bool(const Row&)> pred,
    const std::vector<std::string>& rightCols
);

Row MergeRows(const Row& a, const Row& b);
Row MergeRowsWithAlias(const Row& a, const std::string& aliasA,
                       const Row& b, const std::string& aliasB);

}} // namespace svdb::engine
```

**Impact**: Major performance gain for JOIN queries (currently 1.4-1.8× slower than SQLite)  
**Estimated Effort**: 4-5 days

---

#### 5.1.3: Aggregate Engine (Complete C++ Migration)

**Current**: `internal/VM/engine/aggregate.go` (Go orchestration)  
**Target**: `src/core/VM/engine/aggregate.cpp`

**Already in C++**: `src/core/VM/aggregate_engine.cpp` (batch ops)  
**Missing**: High-level GROUP BY orchestration

**Functions to migrate**:
```go
// Go (current)
func GroupRows(rows []Row, groupCols []string) map[string][]Row
func ApplyAggregates(groups map[string][]Row, aggFuncs []AggregateFunc) map[string]Row
func ApplyHaving(rows []Row, having Expr) []Row
```

```cpp
// C++ (target)
namespace svdb { namespace engine {

std::unordered_map<std::string, std::vector<Row>> GroupRows(
    const std::vector<Row>& rows,
    const std::vector<std::string>& groupCols
);

std::unordered_map<std::string, Row> ApplyAggregates(
    const std::unordered_map<std::string, std::vector<Row>>& groups,
    const std::vector<AggregateFunction>& aggFuncs
);

std::vector<Row> ApplyHaving(
    const std::vector<Row>& rows,
    const Expression& having
);

}} // namespace svdb::engine
```

**Impact**: Complete aggregate execution in C++  
**Estimated Effort**: 3-4 days

---

#### 5.1.4: Sort/ORDER BY Engine

**Current**: `internal/VM/engine/sort.go` (Go)  
**Target**: `src/core/VM/engine/sort.cpp`

**Already in C++**: `src/core/VM/sort.cpp` (sort ops)  
**Missing**: ORDER BY orchestration with LIMIT

**Functions to migrate**:
```go
// Go (current)
func SortRows(rows []Row, orderBy []OrderByClause) []Row
func ApplyTopN(rows []Row, orderBy []OrderByClause, limit int) []Row
func CompareRows(a, b Row, orderBy []OrderByClause) int
```

```cpp
// C++ (target)
namespace svdb { namespace engine {

std::vector<Row> SortRows(
    const std::vector<Row>& rows,
    const std::vector<OrderByClause>& orderBy
);

std::vector<Row> ApplyTopN(
    const std::vector<Row>& rows,
    const std::vector<OrderByClause>& orderBy,
    int limit
);

int CompareRows(
    const Row& a, const Row& b,
    const std::vector<OrderByClause>& orderBy
);

}} // namespace svdb::engine
```

**Impact**: Improve ORDER BY performance (currently 1.2-1.5× slower)  
**Estimated Effort**: 2-3 days

---

#### 5.1.5: Subquery Engine

**Current**: `internal/VM/engine/subquery.go` (Go)  
**Target**: `src/core/VM/engine/subquery.cpp`

**Functions to migrate**:
```go
// Go (current)
func EvalScalarSubquery(ctx Context, subquery *Query) (Value, error)
func EvalExistsSubquery(ctx Context, subquery *Query) (bool, error)
func EvalInSubquery(ctx Context, value Value, subquery *Query) (bool, error)
```

```cpp
// C++ (target)
namespace svdb { namespace engine {

Value EvalScalarSubquery(
    QueryContext* ctx,
    const QueryPlan& subquery
);

bool EvalExistsSubquery(
    QueryContext* ctx,
    const QueryPlan& subquery
);

bool EvalInSubquery(
    QueryContext* ctx,
    const Value& value,
    const QueryPlan& subquery
);

}} // namespace svdb::engine
```

**Impact**: Complete subquery execution in C++  
**Estimated Effort**: 3-4 days

---

#### 5.1.6: Window Function Engine

**Current**: `internal/VM/engine/window.go` (Go)  
**Target**: `src/core/VM/engine/window.cpp`

**Already in C++**: Window opcodes exist  
**Missing**: Window frame orchestration

**Functions to migrate**:
```go
// Go (current)
func ApplyWindowFunctions(rows []Row, windowFuncs []WindowFunc) []Row
func ComputeWindowFrame(rows []Row, frame FrameSpec) []Row
func PartitionRows(rows []Row, partitionBy []string) map[string][]Row
```

```cpp
// C++ (target)
namespace svdb { namespace engine {

std::vector<Row> ApplyWindowFunctions(
    const std::vector<Row>& rows,
    const std::vector<WindowFunction>& windowFuncs
);

std::vector<Row> ComputeWindowFrame(
    const std::vector<Row>& rows,
    const FrameSpec& frame
);

std::unordered_map<std::string, std::vector<Row>> PartitionRows(
    const std::vector<Row>& rows,
    const std::vector<std::string>& partitionBy
);

}} // namespace svdb::engine
```

**Impact**: Complete window function execution in C++  
**Estimated Effort**: 3-4 days

---

### P1: High — Storage Layer Orchestration (10-15 days)

#### 5.2.1: Hybrid Store Query Interface

**Current**: `internal/DS/hybrid_store.go` (Go, ~400 LOC)  
**Target**: `src/core/DS/hybrid_store.cpp`

**Functions to migrate**:
```go
// Go (current)
func (hs *HybridStore) Scan() [][]Value
func (hs *HybridStore) ScanWhere(colName string, val Value) [][]Value
func (hs *HybridStore) ScanRange(colName string, lo, hi Value) [][]Value
func (hs *HybridStore) ScanWithFilter(colName string, op string, val Value) [][]Value
func (hs *HybridStore) ScanProjected(requiredCols []string) [][]Value
```

```cpp
// C++ (target)
namespace svdb { namespace ds {

class HybridStore {
public:
    std::vector<std::vector<Value>> Scan();
    std::vector<std::vector<Value>> ScanWhere(
        const std::string& colName,
        const Value& val
    );
    std::vector<std::vector<Value>> ScanRange(
        const std::string& colName,
        const Value& lo,
        const Value& hi
    );
    std::vector<std::vector<Value>> ScanWithFilter(
        const std::string& colName,
        const std::string& op,
        const Value& val
    );
    std::vector<std::vector<Value>> ScanProjected(
        const std::vector<std::string>& requiredCols
    );
};

}} // namespace svdb::ds
```

**Impact**: Complete scan execution in C++  
**Estimated Effort**: 4-5 days

---

#### 5.2.2: Index Engine

**Current**: `internal/DS/index_engine.go` (Go, ~300 LOC)  
**Target**: `src/core/DS/index_engine.cpp`

**Functions to migrate**:
```go
// Go (current)
func (ie *IndexEngine) CreateIndex(name string, cols []string) error
func (ie *IndexEngine) DropIndex(name string) error
func (ie *IndexEngine) Lookup(key Value) ([]uint32, error)
func (ie *IndexEngine) RangeScan(lo, hi Value) ([]uint32, error)
func (ie *IndexEngine) SkipScan(leadingCol, filterCol string, filterVal Value) *RoaringBitmap
```

```cpp
// C++ (target)
namespace svdb { namespace ds {

class IndexEngine {
public:
    int CreateIndex(const std::string& name, const std::vector<std::string>& cols);
    int DropIndex(const std::string& name);
    std::vector<uint32_t> Lookup(const Value& key);
    std::vector<uint32_t> RangeScan(const Value& lo, const Value& hi);
    RoaringBitmap SkipScan(
        const std::string& leadingCol,
        const std::string& filterCol,
        const Value& filterVal
    );
};

}} // namespace svdb::ds
```

**Impact**: Complete index operations in C++  
**Estimated Effort**: 4-5 days

---

#### 5.2.3: Backup & Persistence

**Current**: `internal/DS/backup.go`, `internal/DS/persistence.go` (Go)  
**Target**: `src/core/DS/backup.cpp`, `src/core/DS/persistence.cpp`

**Functions to migrate**:
```go
// Go (current)
func (db *Database) BackupTo(path string) error
func (db *Database) BackupIncremental(path string) error
func (db *Database) Persist() error
func (db *Database) Compact() error
```

```cpp
// C++ (target)
namespace svdb { namespace ds {

class Database {
public:
    int BackupTo(const std::string& path);
    int BackupIncremental(const std::string& path);
    int Persist();
    int Compact();
};

}} // namespace svdb::ds
```

**Impact**: Complete backup/persistence in C++  
**Estimated Effort**: 3-4 days

---

### P2: Medium — Transaction & Concurrency (10-15 days)

#### 5.3.1: Transaction Coordinator

**Current**: `internal/TM/` (Go orchestration)  
**Target**: `src/core/TM/coordinator.cpp`

**Already in C++**: `src/core/TM/transaction.cpp`  
**Missing**: Transaction coordination, MVCC

**Functions to migrate**:
```go
// Go (current)
func (tm *TransactionManager) BeginTransaction(isolation IsolationLevel) *Transaction
func (tm *TransactionManager) CommitTransaction(tx *Transaction) error
func (tm *TransactionManager) RollbackTransaction(tx *Transaction) error
func (tm *TransactionManager) CreateSavepoint(tx *Transaction, name string) error
```

```cpp
// C++ (target)
namespace svdb { namespace tm {

class TransactionCoordinator {
public:
    Transaction* BeginTransaction(IsolationLevel isolation);
    int CommitTransaction(Transaction* tx);
    int RollbackTransaction(Transaction* tx);
    int CreateSavepoint(Transaction* tx, const std::string& name);
};

}} // namespace svdb::tm
```

**Impact**: Complete transaction management in C++  
**Estimated Effort**: 5-7 days

---

#### 5.3.2: Concurrency Control

**Current**: Go concurrency primitives  
**Target**: C++ threading/locking

**Functions to migrate**:
- Lock manager (SHARED/RESERVED/EXCLUSIVE)
- Deadlock detection
- Wait-for graph management

**Impact**: Better concurrency with C++ threading  
**Estimated Effort**: 5-8 days

---

### P3: Low — Information Schema & Utilities (5-7 days)

#### 5.4.1: Information Schema Views

**Current**: `internal/IS/*.go` (10 files)  
**Target**: `src/core/IS/views.cpp`

**Views to migrate**:
- `TABLES` view
- `COLUMNS` view
- `CONSTRAINTS` view
- `VIEWS` view
- `REFERENTIAL_CONSTRAINTS` view

**Impact**: Complete metadata queries in C++  
**Estimated Effort**: 3-4 days

---

#### 5.4.2: Virtual Tables

**Current**: `internal/DS/vtab*.go` (Go)  
**Target**: `src/core/DS/vtab.cpp`

**Modules to migrate**:
- `series()` — generate_series virtual table
- Extension virtual tables

**Impact**: Complete virtual table support in C++  
**Estimated Effort**: 2-3 days

---

## Performance Targets

| Workload | Current (v0.11.0) | Target (v0.12.0) | Improvement |
|----------|-------------------|------------------|-------------|
| **SELECT all** | 3.0× faster | 5.0× faster | +67% |
| **WHERE filter** | 2.9× slower | 1.2× slower | +142% |
| **GROUP BY** | 5.2× faster | 8.0× faster | +54% |
| **JOIN** | 1.8× slower | 1.0× (equal) | +80% |
| **ORDER BY** | 1.5× slower | 1.0× (equal) | +50% |
| **INSERT** | 2.2× faster | 3.0× faster | +36% |
| **AVG Speedup** | **3.0×** | **5.0×** | **+67%** |

---

## Implementation Strategy

### Strategy 1: New C++ Engine Module

Create new `src/core/VM/engine/` directory:

```
src/core/VM/engine/
├── select.cpp/h       — SELECT orchestration
├── join.cpp/h         — JOIN orchestration
├── aggregate.cpp/h    — GROUP BY orchestration
├── sort.cpp/h         — ORDER BY orchestration
├── subquery.cpp/h     — Subquery execution
├── window.cpp/h       — Window functions
└── CMakeLists.txt
```

### Strategy 2: Unified Query Execution API

```cpp
// C++ API for complete query execution
typedef struct svdb_query_engine svdb_query_engine_t;
typedef struct svdb_result svdb_result_t;

// Execute complete query and return results
svdb_result_t* svdb_query_engine_execute(
    svdb_query_engine_t* engine,
    svdb_program_t* program,
    svdb_context_t* context
);

// Execute SELECT with all clauses
svdb_result_t* svdb_select_execute(
    svdb_select_plan_t* plan,
    svdb_table_t* table,
    svdb_filter_t* where,
    svdb_group_by_t* group_by,
    svdb_order_by_t* order_by,
    svdb_limit_t* limit
);

// Execute JOIN
svdb_result_t* svdb_join_execute(
    svdb_result_t* left,
    svdb_result_t* right,
    svdb_join_type type,
    svdb_predicate_t* predicate
);

// Free result
void svdb_result_free(svdb_result_t* result);
```

### Strategy 3: Go Wrapper Becomes Thin

```go
// Before (current): Go orchestrates, C++ executes
type QueryEngine struct {
    vm *VM  // Go VM with C++ helpers
}

func (qe *QueryEngine) Query(sql string) ([][]interface{}, error) {
    // Go: parse, compile, orchestrate, materialize results
    // C++: execute individual ops only
}

// After (target): C++ orchestrates and executes
type QueryEngine struct {
    ptr unsafe.Pointer  // *C.svdb_query_engine_t
}

func (qe *QueryEngine) Query(sql string) ([][]interface{}, error) {
    // Go: API call only
    // C++: everything (parse, compile, execute, materialize, return)
    
    result := C.svdb_query_engine_execute(
        (*C.svdb_query_engine_t)(qe.ptr),
        sql,
    )
    return convertResult(result), nil
}
```

---

## File Migration Map

### Go Files to Replace

| Go File | C++ Replacement | Priority | Effort |
|---------|-----------------|----------|--------|
| `internal/VM/engine/select.go` | `src/core/VM/engine/select.cpp` | P0 | 3-4 days |
| `internal/VM/engine/join.go` | `src/core/VM/engine/join.cpp` | P0 | 4-5 days |
| `internal/VM/engine/aggregate.go` | `src/core/VM/engine/aggregate.cpp` | P0 | 3-4 days |
| `internal/VM/engine/sort.go` | `src/core/VM/engine/sort.cpp` | P0 | 2-3 days |
| `internal/VM/engine/subquery.go` | `src/core/VM/engine/subquery.cpp` | P0 | 3-4 days |
| `internal/VM/engine/window.go` | `src/core/VM/engine/window.cpp` | P0 | 3-4 days |
| `internal/DS/hybrid_store.go` | `src/core/DS/hybrid_store.cpp` | P1 | 4-5 days |
| `internal/DS/index_engine.go` | `src/core/DS/index_engine.cpp` | P1 | 4-5 days |
| `internal/DS/backup.go` | `src/core/DS/backup.cpp` | P1 | 2-3 days |
| `internal/DS/persistence.go` | `src/core/DS/persistence.cpp` | P1 | 2-3 days |
| `internal/TM/coordinator.go` | `src/core/TM/coordinator.cpp` | P2 | 5-7 days |
| `internal/IS/*.go` (10 files) | `src/core/IS/views.cpp` | P3 | 3-4 days |

**Total**: ~25 Go files → C++  
**Total Effort**: 40-57 days

---

## Timeline Estimate

| Phase | Tasks | Effort | Priority |
|-------|-------|--------|----------|
| **5.1: Query Engine** | SELECT/JOIN/AGG/SORT/SUBQ/WINDOW | 15-20 days | P0 |
| **5.2: Storage Layer** | Hybrid store, indexes, backup | 10-15 days | P1 |
| **5.3: Transactions** | Coordinator, MVCC, concurrency | 10-15 days | P2 |
| **5.4: Utilities** | IS views, vtabs | 5-7 days | P3 |
| **TOTAL** | | **40-57 days** | |

---

## Success Criteria

- [ ] **Query Engine**: SELECT/JOIN/AGG/SORT fully in C++
- [ ] **Performance**: 5× average speedup over SQLite (vs 3× currently)
- [ ] **Go Wrapper**: <500 LOC (thin API binding only)
- [ ] **libsvdb.so**: Complete query execution end-to-end
- [ ] **Tests**: 100% SQL:1999 pass, no regressions
- [ ] **Documentation**: Updated architecture docs

---

## Risk Mitigation

### Risk 1: C++ Map/Hash Performance
**Risk**: `std::unordered_map` slower than Go maps  
**Mitigation**: Use custom hash functions, Abseil library, or flat_hash_map

### Risk 2: Go Concurrency Features
**Risk**: C++ threading more complex than Go goroutines  
**Mitigation**: Use C++20 coroutines, std::async, or retain Go for high-level orchestration only

### Risk 3: Memory Management Complexity
**Risk**: Manual memory management in C++  
**Mitigation**: Use RAII, smart pointers (`std::unique_ptr`, `std::shared_ptr`), arena allocators

### Risk 4: Breaking Changes
**Risk**: API changes break existing Go code  
**Mitigation**: Maintain backwards-compatible Go API during migration, deprecate gradually

### Risk 5: Row Representation Overhead
**Risk**: `map[string]interface{}` equivalent in C++ is slow  
**Mitigation**: Use struct-of-arrays layout, columnar representation for intermediate results

---

## Post-Migration Architecture (v0.12.0)

```
┌─────────────────────────────────────────────────────────┐
│                  Go Application Layer                    │
│              (Thin Wrapper — <500 LOC)                   │
├─────────────────────────────────────────────────────────┤
│  Database.Open()  →  CGO API call only                  │
│  db.Query()       →  C.svdb_query_execute()             │
│  db.Exec()        →  C.svdb_query_execute()             │
└─────────────────────────────────────────────────────────┘
                            │
                            │ (Single CGO boundary)
                            ▼
┌─────────────────────────────────────────────────────────┐
│              C++ libsvdb.so (Complete Engine)            │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │  Query Engine (NEW — Phase 5)                   │   │
│  │  ├─ SELECT orchestration                        │   │
│  │  ├─ JOIN execution (hash/merge/nested-loop)     │   │
│  │  ├─ Aggregates (GROUP BY, HAVING)               │   │
│  │  ├─ Sort (ORDER BY with LIMIT)                  │   │
│  │  ├─ Subqueries (scalar, EXISTS, IN)             │   │
│  │  ├─ Window functions (OVER, PARTITION, FRAME)   │   │
│  │  └─ Result materialization                      │   │
│  └─────────────────────────────────────────────────┘   │
│                            │                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │  VM Layer (existing — Phase 2)                  │   │
│  │  ├─ Bytecode VM (46 opcodes)                    │   │
│  │  ├─ Expression engine (batch SIMD)              │   │
│  │  └─ Aggregate engine                            │   │
│  └─────────────────────────────────────────────────┘   │
│                            │                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Storage Layer (existing + NEW — Phase 1, 5)    │   │
│  │  ├─ B-Tree (embedded PageManager)               │   │
│  │  ├─ Columnar Store (persistence)                │   │
│  │  ├─ Row Store (persistence)                     │   │
│  │  ├─ Hybrid Store queries (NEW)                  │   │
│  │  ├─ Index Engine (NEW)                          │   │
│  │  └─ Overflow, WAL, Cache, Freelist              │   │
│  └─────────────────────────────────────────────────┘   │
│                            │                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │  QP/CG Layer (existing — Phase 3)               │   │
│  │  ├─ Parser (full SQL)                           │   │
│  │  ├─ Analyzer, Binder                            │   │
│  │  ├─ Compiler (AST → bytecode)                   │   │
│  │  └─ Optimizer, Plan Cache                       │   │
│  └─────────────────────────────────────────────────┘   │
│                            │                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Transaction Layer (NEW — Phase 5)              │   │
│  │  ├─ Transaction Coordinator                     │   │
│  │  ├─ MVCC, Concurrency Control                   │   │
│  │  └─ Lock Manager                                │   │
│  └─────────────────────────────────────────────────┘   │
│                            │                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Information Schema (NEW — Phase 5)             │   │
│  │  ├─ TABLES, COLUMNS, CONSTRAINTS views          │   │
│  │  └─ VIEWS, REFERENTIAL views                    │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                            │
                            │ (All C++ — zero Go overhead)
                            ▼
                    Result: [][]interface{}
                    (returned to Go)
```

---

## Key Benefits

1. **5× Performance**: Eliminate remaining Go orchestration overhead
2. **Cleaner Architecture**: C++ core, Go wrapper (clear separation of concerns)
3. **Better Maintainability**: Single language for core logic
4. **Foundation for Advanced Features**: Distributed, replication easier in C++
5. **Complete libsvdb**: Self-contained database engine
6. **Reduced CGO Boundaries**: Single call per query vs multiple calls currently

---

## Conclusion

Phase 5 will complete the C++ migration vision: a **fully self-contained libsvdb database engine** in C++ with Go as a thin, idiomatic API wrapper. The performance target is **5× average speedup** over SQLite (vs 3× currently) by eliminating all remaining Go orchestration overhead in hot paths.

**Total Investment**: 40-57 days  
**Expected Return**: 5× performance, cleaner architecture, foundation for advanced features

---

## Appendix: Related Documents

- `docs/MIGRATION_100_PERCENT.md` — Phase 1-4 completion report
- `docs/MIGRATION_STATUS.md` — Migration status tracking
- `docs/plan-cgo.md` — CGO architecture and progress
- `docs/ARCHITECTURE.md` — System architecture overview
