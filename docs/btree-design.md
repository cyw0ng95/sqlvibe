# SQLite BTree Design Documentation

## Overview

This document describes the SQLite BTree implementation that will be used in the DS (Data Storage) subsystem. The implementation follows SQLite's design for maximum compatibility.

## References

- **SQLite File Format**: https://www.sqlite.org/fileformat2.html
- **SQLite BTree Module**: https://www.sqlite.org/btreemodule.html
- **SQLite Source Code**: btree.c, btreeInt.h

## Page Types

SQLite uses four types of BTree pages:

| Type | Code | Name | Description |
|------|------|------|-------------|
| Table Interior | 0x05 | Interior index b-tree page | Contains pointers to child pages |
| Table Leaf | 0x0d | Leaf table b-tree page | Contains actual data records |
| Index Interior | 0x02 | Interior index b-tree page | Contains index entries with pointers |
| Index Leaf | 0x0a | Leaf index b-tree page | Contains index entries only |

## Page Structure

### Page Header

Each BTree page starts with a header:

**Leaf Page Header (8 bytes)**:
```
Offset  Size  Description
------  ----  -----------
0       1     Page type (0x0d for table leaf, 0x0a for index leaf)
1       2     First freeblock offset (0 if none)
3       2     Number of cells on page
5       2     Start of cell content area (0 means 65536)
7       1     Fragmented free bytes
```

**Interior Page Header (12 bytes)**:
```
Offset  Size  Description
------  ----  -----------
0       1     Page type (0x05 for table interior, 0x02 for index interior)
1       2     First freeblock offset
3       2     Number of cells
5       2     Start of cell content area
7       1     Fragmented free bytes
8       4     Right-most pointer (page number)
```

### Cell Pointer Array

Following the page header is an array of 2-byte cell pointers, one per cell. Each pointer is an offset from the beginning of the page to the start of the cell.

### Unallocated Space

Between the cell pointer array and the cell content area is unallocated space.

### Cell Content Area

Cells grow from the end of the page backwards towards the beginning.

## Cell Format

### Table Leaf Cell

```
Payload size (varint)
Rowid (varint)
Payload bytes
[Overflow page number if payload too large] (4 bytes)
```

### Table Interior Cell

```
Left child page number (4 bytes)
Rowid (varint)
```

### Index Leaf Cell

```
Payload size (varint)
Payload bytes
[Overflow page number if needed] (4 bytes)
```

### Index Interior Cell

```
Left child page number (4 bytes)
Payload size (varint)
Payload bytes
[Overflow page number if needed] (4 bytes)
```

## Varint Encoding

SQLite uses variable-length integers (varint) to save space:
- 1-9 bytes for 64-bit values
- First 7 bits of each byte are data
- MSB indicates if more bytes follow
- Maximum 9 bytes (8 bytes with 7 bits + 1 byte with 8 bits)

## Record Format

Table records use a header followed by data:

```
Header size (varint)
Serial type codes (varint for each column)
Data for each column
```

**Serial Type Codes**:
- 0: NULL
- 1: 8-bit signed integer
- 2: 16-bit signed integer
- 3: 24-bit signed integer
- 4: 32-bit signed integer
- 5: 48-bit signed integer
- 6: 64-bit signed integer
- 7: IEEE 754 floating point
- 8: Integer 0 (schema format 4 only)
- 9: Integer 1 (schema format 4 only)
- 10,11: Reserved
- N≥12 and even: BLOB (N-12)/2 bytes
- N≥13 and odd: TEXT (N-13)/2 bytes

## Overflow Pages

When a payload is too large to fit entirely on a BTree page, overflow pages are used:

1. **Local Payload**: Part of payload stored on BTree page
2. **Overflow Chain**: Remaining payload stored in linked overflow pages

**Overflow Page Format**:
```
Next overflow page number (4 bytes, 0 if last)
Payload continuation bytes
```

**Local Payload Calculation**:
- U = usable page size (page_size - reserved_space)
- P = payload size
- M = ((U-12)*32/255)-23  (min local)
- X = U-35                  (max local)

For table leaf:
- If P ≤ X: entire payload on page
- If P > X: first M+(P-M)%(U-4) bytes on page, rest overflow

## Page Balancing

### When to Balance

- **After Insert**: If page becomes too full (> threshold)
- **After Delete**: If page becomes too empty (< threshold)

### Balance Operations

1. **Split**: Divide an overfull page into two pages
2. **Merge**: Combine two underfull sibling pages
3. **Redistribute**: Move cells between siblings

### Balancing Algorithm

1. Check if page needs balancing
2. Load parent and sibling pages
3. Determine operation: split, merge, or redistribute
4. Update divider keys in parent
5. Recursively balance parent if needed

## BTree Cursor

A cursor maintains position for traversing the tree:

**Cursor State**:
- Path from root to current page (stack of page numbers)
- Current page
- Current cell index within page
- Key and value at current position

**Cursor Operations**:
- `First()`: Position at leftmost leaf
- `Last()`: Position at rightmost leaf
- `Seek(key)`: Binary search to find key
- `Next()`: Move to next cell (may traverse pages)
- `Previous()`: Move to previous cell

## Freelist Management

Free pages are managed via a freelist:

**Freelist Trunk Page**:
```
Next trunk page number (4 bytes)
Number of leaf page numbers on this trunk (4 bytes)
Array of leaf page numbers
```

**Freelist Operations**:
- **Allocate**: Pop page from freelist
- **Deallocate**: Push page onto freelist
- **Compact**: Consolidate trunk pages when fragmented

## Implementation Phases

### Phase 1: Foundation (Task 8.1-8.3)
- [x] Design documentation
- [ ] Page structure implementation
- [ ] Cell format implementation
- [ ] Varint encoding/decoding

### Phase 2: Core Operations (Task 8.4-8.6)
- [ ] Basic BTree operations (Insert, Delete, Search)
- [ ] Cursor implementation
- [ ] Tree traversal

### Phase 3: Advanced Features (Task 8.7-8.9)
- [ ] Page balancing
- [ ] Overflow page handling
- [ ] Freelist management

### Phase 4: Indexes (Task 8.10)
- [ ] Index BTree implementation
- [ ] Composite indexes
- [ ] Unique index constraints

### Phase 5: Integration (Task 8.11-8.12)
- [ ] Migrate DS subsystem to use new BTree
- [ ] Testing and validation
- [ ] Performance optimization

## Testing Strategy

1. **Unit Tests**: Test each component in isolation
   - Varint encoding/decoding
   - Cell format encoding/decoding
   - Page operations

2. **Integration Tests**: Test BTree operations
   - Insert sequences (ordered, random)
   - Delete operations
   - Search and seek
   - Cursor traversal

3. **Compatibility Tests**: Compare with SQLite
   - File format compatibility
   - Query results match SQLite
   - Performance benchmarks

4. **Stress Tests**:
   - Large datasets
   - High page fill factors
   - Many overflow pages
   - Deep tree structures

## Notes

- Current DS implementation is a placeholder
- New implementation must maintain backward compatibility where possible
- Focus on correctness first, then optimize
- Use SQLite test vectors where available
- Page size should be configurable (512-65536 bytes, power of 2)

## Next Steps

1. Implement page structure (page.go, page_header.go)
2. Implement cell encoding/decoding (cell.go)
3. Implement varint encoding (encoding.go)
4. Begin basic BTree operations (btree_ops.go)
