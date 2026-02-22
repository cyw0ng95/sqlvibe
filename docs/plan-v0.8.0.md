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

#### 4.2 New On-Disk File Format (v0.8.0)

**Design Principles:**
- Columnar-first: Store data by columns, not rows
- Embedded compression: LZ4 for fast compression/decompression
- Optional indexes: Bitmap indexes for fast filtering
- Memory-mapped: Support mmap for large databases

**File Structure:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SQLVIBE Database File                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ HEADER (256 bytes fixed)                                        │   │
│  ├──────────────────────────────────────────────────────────────────┤   │
│  │ Offset │ Size │ Field                                           │   │
│  │--------|------|-------------------------------------------------│   │
│  │ 0      │ 8    │ Magic: "SQLVIBE\x01" (7 chars + \x01)          │   │
│  │ 8      │ 4    │ Format Version Major: uint32 (e.g., 1)          │   │
│  │ 12     │ 4    │ Format Version Minor: uint32 (e.g., 0)          │   │
│  │ 16     │ 4    │ Format Version Patch: uint32 (e.g., 0)          │   │
│  │ 20     │ 4    │ Flags: uint32                                  │   │
│  │        │      │   - Bit 0: Compression enabled                  │   │
│  │        │      │   - Bit 1: Indexes embedded                    │   │
│  │        │      │   - Bit 2: Encryption enabled                  │   │
│  │        │      │   - Bit 3: Columnar storage                    │   │
│  │ 24     │ 4    │ Schema offset: uint32                         │   │
│  │ 28     │ 4    │ Schema length: uint32                         │   │
│  │ 32     │ 4    │ Column count: uint32                           │   │
│  │ 36     │ 4    │ Row count: uint32 (total rows)                │   │
│  │ 40     │ 4    │ Index count: uint32                            │   │
│  │ 44     │ 4    │ Created timestamp: uint32 (Unix)              │   │
│  │ 48     │ 4    │ Modified timestamp: uint32 (Unix)              │   │
│  │ 52     │ 4    │ Compression type: uint32                       │   │
│  │        │      │   0 = none, 1 = lz4, 2 = zstd                  │   │
│  │ 56     │ 4    │ Page size: uint32 (default 4096)              │   │
│  │ 60     │ 4    │ Reserved for future use                       │   │
│  │ ...    │ ...  │ ...                                           │   │
│  │ 248    │ 8    │ Header CRC64: uint64                          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ SCHEMA SECTION (JSON)                                            │   │
│  ├──────────────────────────────────────────────────────────────────┤   │
│  │ {                                                               │   │
│  │   "tables": [                                                   │   │
│  │     {                                                           │   │
│  │       "name": "users",                                          │   │
│  │       "columns": [                                              │   │
│  │         {"name": "id", "type": "INT64"},                        │   │
│  │         {"name": "name", "type": "STRING"},                    │   │
│  │         {"name": "email", "type": "STRING"}                    │   │
│  │       ],                                                        │   │
│  │       "primary_key": ["id"],                                    │   │
│  │       "indexes": ["idx_email"]                                  │   │
│  │     }                                                           │   │
│  │   ],                                                            │   │
│  │   "indexes": [                                                  │   │
│  │     {"name": "idx_email", "table": "users", "column": "email"} │   │
│  │   ]                                                            │   │
│  │ }                                                               │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ COLUMN DATA SECTION                                              │   │
│  ├──────────────────────────────────────────────────────────────────┤   │
│  │                                                                  │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Column Header (16 bytes fixed)                               │ │   │
│  │ ├─────────────────────────────────────────────────────────────┤ │   │
│  │ │ Offset │ Size │ Field                                       │ │   │
│  │ │--------|------|----------------------------------------------│ │   │
│  │ │ 0      │ 2    │ Column name length: uint16                  │ │   │
│  │ │ 2      │ 64   │ Column name (null-padded)                  │ │   │
│  │ │ 66     │ 1    │ Column type: uint8                         │ │   │
│  │ │        |      │   0 = NULL, 1 = INT8, 2 = INT16            │ │   │
│  │ │        |      │   3 = INT32, 4 = INT64, 5 = UINT64         │ │   │
│  │ │        |      │   6 = FLOAT32, 7 = FLOAT64                 │ │   │
│  │ │        |      │   8 = STRING, 9 = BYTES                   │ │   │
│  │ │        |      │  10 = BOOL, 11 = DATE, 12 = DATETIME      │ │   │
│  │ │ 67     │ 1    │ Compression: uint8 (0=none, 1=lz4)        │ │   │
│  │ │ 68     │ 4    │ Value count: uint32                        │ │   │
│  │ │ 72     │ 4    │ Null count: uint32                         │ │   │
│  │ │ 76     │ 4    │ Data offset: uint32                        │ │   │
│  │ │ 80     │ 4    │ Compressed size: uint32 (0 if not compr)  │ │   │
│  │ │ 84     │ 4    │ RLE run count: uint32 (if RLE enabled)     │ │   │
│  │ │ 88     │ 4    │ Reserved                                    │ │   │
│  │ │ 92     │ 4    │ Column CRC32: uint32                        │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                  │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Column Data (compressed or raw)                             │ │   │
│  │ ├─────────────────────────────────────────────────────────────┤ │   │
│  │ │                                                                  │   │
│  │ │ For INT8/INT16/INT32/INT64:                                   │   │
│  │ │   - Stored as plain little-endian bytes                       │   │
│  │ │   - Example: [0x01, 0x00, 0x00, 0x00] = 1 (int32)           │   │
│  │ │                                                                  │   │
│  │ │ For FLOAT32/FLOAT64:                                           │   │
│  │ │   - IEEE 754 little-endian                                     │   │
│  │ │                                                                  │   │
│  │ │ For STRING:                                                     │   │
│  │ │   - [4 bytes length] [UTF-8 bytes] [null terminator]          │   │
│  │ │   - Strings are length-prefixed                                │   │
│  │ │                                                                  │   │
│  │ │ For BYTES:                                                     │   │
│  │ │   - [4 bytes length] [raw bytes]                              │   │
│  │ │                                                                  │   │
│  │ │ Optional RLE Compression:                                        │   │
│  │ │   - For columns with high repetition                           │   │
│  │ │   - Format: [value] [run_count-1]                              │   │
│  │ │   - Example: [0x41, 0x09] = 'A' repeated 10 times             │   │
│  │ │                                                                  │   │
│  │ │ Optional LZ4 Compression:                                        │   │
│  │ │   - Applied after RLE                                          │   │
│  │ │   - Better for random data                                     │   │
│  │ │                                                                  │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                  │   │
│  │ [Repeat for each column...]                                     │   │
│  │                                                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ INDEX SECTION (Optional - only if indexes defined)               │   │
│  ├──────────────────────────────────────────────────────────────────┤   │
│  │                                                                  │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Index Header (16 bytes)                                     │ │   │
│  │ ├─────────────────────────────────────────────────────────────┤ │   │
│  │ │ Offset │ Size │ Field                                       │ │   │
│  │ │--------|------|----------------------------------------------│ │   │
│  │ │ 0      │ 2    │ Index name length: uint16                   │ │   │
│  │ │ 2      │ 64   │ Index name (null-padded)                   │ │   │
│  │ │ 66     │ 1    │ Index type: uint8                          │ │   │
│  │ │        |      │   0 = Bitmap, 1 = BTree, 2 = SkipList     │ │   │
│  │ │ 67     │ 1    │ Key type: uint8 (same as column types)    │ │   │
│  │ │ 68     │ 4    │ Unique values count: uint32               │ │   │
│  │ │ 72     │ 4    │ Data offset: uint32                        │ │   │
│  │ │ 76     │ 4    │ Data size: uint32                          │ │   │
│  │ │ 80     │ 4    │ Reserved                                    │ │   │
│  │ │ 84     │ 4    │ Index CRC32: uint32                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                  │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Bitmap Index Data                                            │ │   │
│  │ │ (For equality filters: WHERE col = value)                  │ │   │
│  │ ├─────────────────────────────────────────────────────────────┤ │   │
│  │ │                                                                  │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ Bitmap Container (4096 bits = 512 bytes per container)  │ │ │   │
│  │ │ ├─────────────────────────────────────────────────────────┤ │ │   │
│  │ │ │ For each unique value v in indexed column:             │ │ │   │
│  │ │ │   - Key: uint16 (value mod 65536)                     │ │ │   │
│  │ │ │   - Container: Roaring Bitmap (set of row indices)    │ │ │   │
│  │ │ │                                                                  │   │
│  │ │ │ Example: column "status" with values [0,1,2]           │ │ │   │
│  │ │ │   - status=0: bitmap {0, 5, 10, 15, ...}               │ │ │   │
│  │ │ │   - status=1: bitmap {1, 6, 11, 16, ...}               │ │ │   │
│  │ │ │   - status=2: bitmap {2, 7, 12, 17, ...}               │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │                                                                  │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                  │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ BTree/SkipList Index Data                                   │ │   │
│  │ │ (For range queries: WHERE col > value)                    │ │   │
│  │ ├─────────────────────────────────────────────────────────────┤ │   │
│  │ │   - Ordered key-value pairs                                │ │   │
│  │ │   - Value: list of row indices                             │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                  │   │
│  │ [Repeat for each index...]                                     │   │
│  │                                                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ FOOTER (32 bytes)                                               │   │
│  ├──────────────────────────────────────────────────────────────────┤   │
│  │ Offset │ Size │ Field                                           │   │
│  │--------|------|-------------------------------------------------│   │
│  │ 0      │ 4    │ Data section magic: 0x53414D50 ("SAMP")       │   │
│  │ 4      │ 4    │ Index section magic: 0x494E4458 ("INDX")      │   │
│  │ 8      │ 8    │ Data section length: uint64                   │   │
│  │ 16     │ 8    │ Index section length: uint64                  │   │
│  │ 24     │ 8    │ File CRC64: uint64 (entire file)             │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Column Type Encoding:**

| Type ID | Type Name  | Size (bytes) | Description                    |
|---------|------------|--------------|--------------------------------|
| 0       | NULL       | 0            | No data, tracked in null bitmap |
| 1       | INT8       | 1            | 8-bit signed integer           |
| 2       | INT16      | 2            | 16-bit signed integer          |
| 3       | INT32      | 4            | 32-bit signed integer          |
| 4       | INT64      | 8            | 64-bit signed integer          |
| 5       | UINT64     | 8            | 64-bit unsigned integer        |
| 6       | FLOAT32    | 4            | IEEE 754 float32               |
| 7       | FLOAT64    | 8            | IEEE 754 float64              |
| 8       | STRING     | variable     | Length-prefixed UTF-8          |
| 9       | BYTES      | variable     | Length-prefixed binary         |
| 10      | BOOL       | 1            | 0 or 1                         |
| 11      | DATE       | 4            | Unix timestamp (date only)     |
| 12      | DATETIME   | 8            | Unix timestamp (full)         |

**Compression Strategy:**

1. **No compression** (default for small tables)
   - Fastest read/write
   - Best for frequently updated data

2. **RLE (Run-Length Encoding)**
   - Good for: boolean, low-cardinality columns
   - Example: `AAAAABBBBBCCCCC` → `A*5 B*5 C*5`

3. **LZ4**
   - Good for: random string data
   - Fast decompression (important for reads)
   - Compression ratio: 2-5x typical

4. **Zstd** (optional)
   - Better compression ratio than LZ4
   - Slower but still fast

**Advantages over SQLite Format:**

| Feature         | SQLite v3          | SQLVIBE v0.8.0           |
|-----------------|-------------------|--------------------------|
| Storage model   | Row-based B-Tree  | Columnar + optional index|
| Compression     | None              | RLE + LZ4               |
| Index type      | B-Tree only       | Bitmap + B-Tree + SkipList |
| Random access   | Page-based        | Column offset direct    |
| Schema          | SQLite header     | JSON                    |
| Encryption      | External          | Optional embedded       |
| Read performance| Good              | 10-50x faster (columnar)|
| Write performance| Good              | Similar or slightly less|

**File Size Estimate:**

For 1 million rows with 10 columns:
- SQLite: ~50-100 MB (depends on data)
- SQLVIBE (uncompressed): ~40-80 MB
- SQLVIBE (LZ4): ~20-50 MB (2-3x compression)
- SQLVIBE (RLE + LZ4): ~15-40 MB (3-5x compression)

---

## Format Versioning

### Version Scheme

The SQLVIBE database format uses **Semantic Versioning** for the file format:

```
Format Version: MAJOR.MINOR.PATCH

- MAJOR: Breaking changes (incompatible structure)
- MINOR: New features (backward compatible)
- PATCH: Bug fixes (backward compatible)
```

### Current Format Version

| Version | Status | Description |
|---------|--------|-------------|
| **1.0.0** | **Current** | Initial columnar format |

### Version Encoding in File

The format version is stored in the header:

```
Offset 8-11 (4 bytes): Format Version Major (uint32)
Offset 12-15 (4 bytes): Format Version Minor (uint32)
Offset 16-19 (4 bytes): Format Version Patch (uint32)
```

### Compatibility Rules

1. **Reader compatibility**: A reader can read files with version ≤ reader's max supported version
2. **Writer compatibility**: A writer should write in the highest version supported by target readers
3. **Migration**: When opening an older format, automatically migrate to current version

### Future Version Planning

| Version | Planned Features |
|---------|-----------------|
| 1.1.0 | Vector indexes, spatial data types |
| 1.2.0 | Encryption support |
| 2.0.0 | Sharding/distributed support (breaking) |

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
- [x] Design and implement Value type system (`pkg/sqlvibe/storage/value.go`)
- [x] Implement Row struct with null bitmap (`pkg/sqlvibe/storage/row.go`)
- [x] Implement ColumnVector for typed storage (`pkg/sqlvibe/storage/column_vector.go`)
- [x] Implement RoaringBitmap with And/Or/Not operations (`pkg/sqlvibe/storage/roaring_bitmap.go`)
- [x] Implement SkipList for ordered data (`pkg/sqlvibe/storage/skip_list.go`)
- [x] Implement Arena allocator with batch free (`pkg/sqlvibe/storage/arena.go`)
- [x] Unit tests for all new structures (`pkg/sqlvibe/storage/storage_test.go`)

### Phase 2: Storage Engine
- [x] Implement HybridStorageEngine (`pkg/sqlvibe/storage/hybrid_store.go`)
- [x] Implement RowStore (insert/update/delete) (`pkg/sqlvibe/storage/row_store.go`)
- [x] Implement ColumnStore (vector read/write) (`pkg/sqlvibe/storage/column_store.go`)
- [x] Implement IndexEngine (bitmap + skiplist) (`pkg/sqlvibe/storage/index_engine.go`)
- [x] Add adaptive switching logic
- [x] Integrate with QueryEngine (`pkg/sqlvibe/database.go` — per-table HybridStore, lazy rebuild, `GetHybridStore()`)

### Phase 3: Execution Engine
- [x] Implement vectorized filter (`pkg/sqlvibe/exec_columnar.go`)
- [x] Implement columnar aggregation (`pkg/sqlvibe/exec_columnar.go`)
- [x] Update hash join for columnar (`ColumnarHashJoin` in `pkg/sqlvibe/exec_columnar.go`)
- [x] Update GROUP BY for vectors (`VectorizedGroupBy` in `pkg/sqlvibe/exec_columnar.go`)
- [x] Benchmark and tune (memory/GC profiling benchmarks in `internal/TS/Benchmark/`)

### Phase 4: Persistence (New v0.8.0 Format)
- [x] **Create formal spec: `docs/DB-FORMAT.md`**
  - Version: **1.0.0** (see Format Versioning below)
  - Document complete binary format specification
  - Include wire format diagrams, field descriptions
  - Document compression algorithms
  - Document index structures
  - Add change log for format versions
- [x] Design new binary format (columnar-first, see spec above)
- [x] Implement file header read/write (256 bytes)
- [x] Implement schema JSON serialization/deserialization
- [x] Implement column data serialization (typed vectors)
- [x] Implement RLE compression for low-cardinality columns (`encodeRLE`/`decodeRLE` in `persistence.go`)
- [x] Implement gzip/deflate compression for general columns (`compressGzip`/`decompressGzip` in `persistence.go`; replaces LZ4 — no external deps required)
- [x] Implement RoaringBitmap index serialization (`SerializeIndexes`/`DeserializeIndexes`)
- [x] Implement BTree/SkipList index serialization (`SerializeIndexes`/`DeserializeIndexes`)
- [x] Implement file footer with CRC64 checksums (`pkg/sqlvibe/storage/persistence.go`)
- [ ] Implement mmap-based random access
- [ ] Implement WAL (Write-Ahead Logging) for durability
- [ ] Implement checkpoint/compact operation
- [ ] Migration tools: SQLite import, SQLVIBE export

### Phase 5: Testing & Validation
- [x] Run full SQL:1999 test suite (100% pass rate — no regressions)
- [x] Run comparison benchmarks vs v0.7.x
- [x] Memory profiling benchmarks (`BenchmarkStorage_MemoryProfile_*` in `internal/TS/Benchmark/`)
- [x] GC profiling benchmarks (`BenchmarkStorage_GCProfile_*` in `internal/TS/Benchmark/`)
- [x] Update documentation (plan-v0.8.0.md, docs/DB-FORMAT.md)

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

