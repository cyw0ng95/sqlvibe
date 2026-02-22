# Plan v0.8.0 - New Architecture: In-Memory Columnar Storage

## Summary

**Major release** that introduces a completely new data storage architecture optimized for in-memory analytical workloads. This version breaks whitebox storage compatibility with SQLite in exchange for 10-100x performance improvements.

**Previous**: v0.7.7 delivers QP & DS performance optimizations

**Breaking Changes**:
- SQLite file format compatibility: **REMOVED**
- Only SQL interface remains compatible
- Database files are not readable by SQLite tools

---

## Motivation

### Current Bottlenecks (v0.7.x)

| Bottleneck | Current | SQLite | Impact |
|------------|---------|--------|--------|
| Full table scan | 55 µs | 1019 µs | ✅ 18x faster |
| SELECT WHERE | 279 µs | 122 µs | ❌ 2.3x slower |
| COUNT(*) | 47 µs | 6 µs | ❌ 7.4x slower |
| Memory allocation | High GC | Low | ❌ 10x more |

### Root Causes

1. **Row storage**: `map[string]interface{}` - hash lookup per column access
2. **No columnar storage**: Cannot leverage SIMD/vectorization
3. **Index inefficiency**: Hash index but no bitmap filtering
4. **GC pressure**: Too many small heap allocations

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        sqlvibe v0.8.0                               │
├─────────────────────────────────────────────────────────────────────┤
│  SQL Interface (Fully Compatible)                                    │
│  ├── Parser (QP) - No changes needed                                │
│  ├── Compiler (CG) - Adapts to new storage                          │
│  └── VM - Uses new storage API                                      │
├─────────────────────────────────────────────────────────────────────┤
│  New Storage Layer                                                   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Adaptive Hybrid Store                                         │   │
│  │  ├── Row Store: Contiguous memory, struct-based             │   │
│  │  └── Column Store: Typed vectors (int64/float64/string)     │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Index Layer                                                   │   │
│  │  ├── Roaring Bitmaps: O(1) filtering                        │   │
│  │  └── Skip Lists: Ordered data                               │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Memory Layer                                                  │   │
│  │  └── Arena Allocator: Zero-GC query execution               │   │
│  └─────────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────────┤
│  Persistence (NEW FORMAT)                                           │
│  └── Custom binary format (not SQLite compatible)                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Core Data Structures (Foundation)

#### 1.1 New Row Format

```go
// New row representation - fixed layout, no map
type Row struct {
    Cols    []Value  // Column values
    Bitmap  uint64   // Null bitmap (64 columns max)
}

type Value struct {
    Type  ValueType
    Int   int64
    Float float64
    Str   string
    Bytes []byte
}

// Compact column storage
type ColumnVector struct {
    Name     string
    Type     ValueType
    Values   interface{}  // []int64, []float64, []string, [][]byte
    Nulls    []bool
}
```

#### 1.2 Roaring Bitmap Index

```go
// Roaring Bitmap for fast filtering
type RoaringBitmap struct {
   -containers []container
}

type container struct {
    key    uint16
    bitmap []uint64  // 16-bit values → 4096 bits
    array  []uint16  // runs of zeros/ones optimization
}

// O(1) set operations for WHERE clause
func (rb *RoaringBitmap) And(other *RoaringBitmap) *RoaringBitmap
func (rb *RoaringBitmap) Or(other *RoaringBitmap) *RoaringBitmap
func (rb *RoaringBitmap) Filter(values []int64) []int64
```

#### 1.3 Skip List for Ordered Data

```go
// Skip list for ORDER BY optimization
type SkipList struct {
    head     *skipNode
    levels   int
    rand     *rand.Rand
}

type skipNode struct {
    value    Value
    forward  []*skipNode  // Forward pointers at each level
}
```

#### 1.4 Arena Allocator

```go
// Zero-GC memory management
type Arena struct {
    buf      []byte
    offset   int
    chunks   [][]byte
}

func (a *Arena) Alloc(size int) []byte
func (a *Arena) Reset()  // Batch free for query
```

---

### Phase 2: Storage Engine Implementation

#### 2.1 Hybrid Storage Engine

```go
type StorageEngine struct {
    rowStore     *RowStore
    columnStore  *ColumnStore
    indexEngine  *IndexEngine
    arena        *Arena
    
    // Adaptive switching
    queryStats   QueryStats
}

type RowStore struct {
    rows      []Row
    columns   []string
    schema    map[string]ColumnType
}

type ColumnStore struct {
    vectors   map[string]*ColumnVector
    order     []string  // Column order for projection
}

type IndexEngine struct {
    bitmaps   map[string]*RoaringBitmap  // col_name → bitmap
    skipLists map[string]*SkipList       // col_name → sorted
}
```

#### 2.2 Query Path Changes

```
SELECT * FROM t WHERE col = 5
         │
         ▼
┌─────────────────────────┐
│ Parse & Compile        │ (unchanged)
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Index Lookup            │
│ - RoaringBitmap[col=5] │ ◄── O(1) bitmap fetch
│ - Returns row indices   │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Columnar Fetch          │
│ - Read columns as vector│ ◄── Batch memory access
│ - Apply bitmap filter   │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Result Construction     │
│ - Arena allocate        │ ◄── No GC during query
└─────────────────────────┘
```

---

### Phase 3: Execution Engine Updates

#### 3.1 Vectorized Filter

```go
// Batch filter instead of row-by-row
func VectorizedFilter(col *ColumnVector, pred Predicate) []int64 {
    switch col.Type {
    case TypeInt:
        vals := col.Values.([]int64)
        result := make([]int64, 0, len(vals))
        for i := 0; i < len(vals); i++ {
            if pred.EvalInt(vals[i]) {
                result = append(result, int64(i))
            }
        }
        return result
    }
    // ...
}
```

#### 3.2 Columnar Aggregation

```go
// Aggregate directly on column vectors
func ColumnarSum(col *ColumnVector) float64 {
    switch col.Type {
    case TypeInt:
        vals := col.Values.([]int64)
        var sum int64
        for _, v := range vals {
            sum += v
        }
        return float64(sum)
    }
    // ...
}
```

---

### Phase 4: Persistence (New Format)

#### 4.1 Custom Binary Format

```
┌────────────────────────────────────────┐
│ Magic Number (8 bytes): "SQLVIBE01"   │
├────────────────────────────────────────┤
│ Header                                │
│  - Version: uint32                     │
│  - Schema Length: uint32              │
│  - Schema: JSON encoded                │
├────────────────────────────────────────┤
│ Column Data                            │
│  - For each column:                    │
│    - Name length + Name               │
│    - Type: uint8                       │
│    - Null count: uint32               │
│    - Value count: uint32              │
│    - Compressed values (gzip/lz4)    │
├────────────────────────────────────────┤
│ Index Data                             │
│  - Bitmap segments (optional)          │
│  - Skip list pointers (optional)      │
├────────────────────────────────────────┤
│ Footer                                 │
│  - Checksum: CRC32                     │
│  - Data offset: uint64                │
└────────────────────────────────────────┘
```

---

## Performance Targets

### Benchmark Comparison (v0.8.0 vs v0.7.x)

| Operation | v0.7.x | v0.8.0 Target | Method |
|-----------|--------|---------------|--------|
| SELECT all 1K | 55 µs | < 5 µs | Columnar + vectorized |
| SELECT WHERE | 279 µs | < 20 µs | RoaringBitmap index |
| Full scan 5K | 1000 µs | < 30 µs | SIMD batch |
| COUNT(*) | 47 µs | < 3 µs | Column metadata |
| SUM | 65 µs | < 5 µs | Columnar aggregate |
| GROUP BY | 126 µs | < 15 µs | Hash on vectors |
| Memory/row | ~500 B | < 100 B | Contiguous + arena |
| GC pause | High | None | Arena allocator |

### Memory Efficiency

| Metric | v0.7.x | v0.8.0 |
|--------|--------|--------|
| 1M rows (int only) | ~500 MB | < 50 MB |
| Allocations/query | 1000+ | < 10 |
| GC pressure | High | Zero |

---

## Files Structure

```
pkg/sqlvibe/
├── database.go           # Updated: new storage engine
├── storage/              # NEW: Storage subsystem
│   ├── row_store.go
│   ├── column_store.go
│   ├── index_engine.go  # RoaringBitmap + SkipList
│   ├── arena.go        # Zero-GC allocator
│   └── persistence.go  # New binary format
├── exec_columnar.go     # NEW: Vectorized execution
├── exec_row.go          # Updated: Row execution
└── vm_context.go       # Updated: New storage API

internal/
├── DS/                  # KEPT: Legacy disk format (optional)
│   ├── btree.go        # Deprecated for in-memory
│   └── page.go         # Optional: disk persistence
├── VM/
│   ├── query_engine.go # Updated: Hybrid execution
│   └── cursor.go       # Updated: New cursor API
└── QP/                  # UNCHANGED
```

---

## Migration Guide

### For Users Upgrading from v0.7.x

1. **Database files**: Old `.db` files are incompatible. Re-export data.
2. **Memory databases**: `:memory:` works identically.
3. **Performance**: Expect 10-100x improvement on analytical queries.
4. **API**: No changes to SQL interface.

### Backward Compatibility

- SQL syntax: 100% compatible
- In-memory mode: Behavior unchanged (just faster)
- Disk mode: Requires re-creation

---

## Tasks

### Phase 1: Core Data Structures
- [ ] Design and implement Value type system
- [ ] Implement Row struct with null bitmap
- [ ] Implement ColumnVector for typed storage
- [ ] Implement RoaringBitmap with And/Or/Not operations
- [ ] Implement SkipList for ordered data
- [ ] Implement Arena allocator with batch free
- [ ] Unit tests for all new structures

### Phase 2: Storage Engine
- [ ] Implement HybridStorageEngine
- [ ] Implement RowStore (insert/update/delete)
- [ ] Implement ColumnStore (vector read/write)
- [ ] Implement IndexEngine (bitmap + skiplist)
- [ ] Add adaptive switching logic
- [ ] Integrate with QueryEngine

### Phase 3: Execution Engine
- [ ] Implement vectorized filter
- [ ] Implement columnar aggregation
- [ ] Update hash join for columnar
- [ ] Update GROUP BY for vectors
- [ ] Benchmark and tune

### Phase 4: Persistence
- [ ] Design new binary format
- [ ] Implement serialization
- [ ] Implement deserialization
- [ ] Add compression (optional)
- [ ] Migration tools

### Phase 5: Testing & Validation
- [ ] Run full SQL:1999 test suite
- [ ] Run comparison benchmarks vs v0.7.x
- [ ] Memory profiling
- [ ] GC profiling
- [ ] Update documentation

---

## Success Criteria

| Criteria | Target |
|----------|--------|
| SQL:1999 tests pass | 100% |
| SELECT WHERE | < 20 µs (14x faster) |
| COUNT(*) | < 3 µs (15x faster) |
| GROUP BY | < 15 µs (8x faster) |
| Memory/row | < 100 bytes (5x less) |
| Allocations/query | < 10 (100x fewer) |
| GC pause | Zero during query |

---

## Benchmark Commands

```bash
# Run all benchmarks
go test ./internal/TS/Benchmark/... -bench=. -benchtime=3s -benchmem

# Compare with SQLite
go test ./internal/TS/Benchmark/... -bench="SQLite" -benchtime=3s

# Memory profiling
go test ./internal/TS/Benchmark/... -bench=BenchmarkSelectAll -memprofile=mem.prof
go tool pprof mem.prof

# GC profiling
go test ./internal/TS/Benchmark/... -bench=BenchmarkSelectAll -trace=trace.out
go tool trace trace.out
```

---

## Timeline Estimate

| Phase | Tasks | Estimated Hours |
|-------|-------|-----------------|
| 1 | Core Data Structures | 20 |
| 2 | Storage Engine | 25 |
| 3 | Execution Engine | 20 |
| 4 | Persistence | 15 |
| 5 | Testing & Validation | 10 |

**Total**: ~90 hours

---

## Notes

- This is a breaking change - document migration path clearly
- Focus on analytical (OLAP) workloads first
- Consider WAL for disk mode in future
- Monitor memory usage carefully
- Test with real-world SQL:1999 queries

