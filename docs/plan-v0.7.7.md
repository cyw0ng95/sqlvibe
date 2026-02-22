# Plan v0.7.7 - QP & DS Performance Optimizations

## Summary

Implement targeted performance optimizations for the Query Processing (QP) and Data Storage (DS) subsystems, with focus on hot-path operations and benchmark-driven improvements.

**Previous**: v0.7.6 delivers CG & VM performance optimizations

---

## Optimization Targets

### QP (Query Processing) Optimizations

| Optimization | Description | Complexity | Impact |
|-------------|-------------|------------|--------|
| Keyword Lookup | Use switch instead of map for common keywords | Low | Medium |
| Hex String Parsing | Replace fmt.Sscanf with lookup table | Low | High |
| Token Pre-allocation | Pre-allocate token slice capacity | Low | Low-Medium |
| Parser Expression Cache | Cache parsed expressions | Medium | Medium |

### DS (Data Storage) Optimizations

| Optimization | Description | Complexity | Impact |
|-------------|-------------|------------|--------|
| Varint Encoding | Bit manipulation in VarintLen | Low | Medium |
| LRU Cache | Optimize with ring or custom impl | Medium | High |
| Record Encoding | Use sync.Pool for buffers | Medium | Medium |
| Page Prefetch | Worker pool instead of goroutines | Medium | Medium |
| BTree Cell Search | Cache decoded keys during binary search | Medium | Medium |

---

## Benchmark Suite

### QP Benchmarks

| Benchmark | Target |
|-----------|--------|
| BenchmarkTokenizer_Identifiers | Tokenizing identifiers |
| BenchmarkTokenizer_Numbers | Tokenizing numbers |
| BenchmarkTokenizer_Strings | Tokenizing strings |
| BenchmarkTokenizer_HexStrings | Tokenizing hex strings |
| BenchmarkTokenizer_FullQuery | Tokenizing full SQL query |
| BenchmarkParser_Select | Parsing SELECT statements |
| BenchmarkParser_ComplexExpr | Complex expression parsing |

### DS Benchmarks

| Benchmark | Target |
|-----------|--------|
| BenchmarkVarint_Put | Varint encoding |
| BenchmarkVarint_Get | Varint decoding |
| BenchmarkVarint_Len | Varint length calculation |
| BenchmarkCache_Get | LRU cache hit |
| BenchmarkCache_Set | LRU cache miss+set |
| BenchmarkBTree_Search | BTree key search |
| BenchmarkBTree_Insert | BTree insert |
| BenchmarkBTree_Cursor | BTree cursor traversal |
| BenchmarkRecord_Encode | Record encoding |
| BenchmarkRecord_Decode | Record decoding |

---

## Implementation Plan

### Phase 1: QP Optimizations

#### 1.1 Keyword Lookup - Switch-based

```go
// In tokenizer.go - use switch for common keywords
func (t *Tokenizer) lookupKeyword(s string) TokenType {
    switch len(s) {
    case 2:
        switch s {
        case "OR": return TokenOr
        case "IN": return TokenIn
        }
    case 3:
        switch s {
        case "NOT": return TokenNot
        case "AND": return TokenAnd
        case "SET": return TokenKeyword
        }
    case 4:
        switch s {
        case "LIKE": return TokenLike
        case "GLOB": return TokenGlob
        case "NULL": return TokenKeyword
        case "TRUE": return TokenKeyword
        case "FALSE": return TokenKeyword
        }
    // ... more lengths
    }
    // Fallback to map
    if tokenType, ok := keywords[s]; ok {
        return tokenType
    }
    return TokenIdentifier
}
```

#### 1.2 Hex String Parsing - Lookup Table

```go
// In tokenizer.go
var hexValTable [256]byte

func init() {
    for i := range hexValTable {
        switch {
        case i >= '0' && i <= '9':
            hexValTable[i] = byte(i - '0')
        case i >= 'A' && i <= 'F':
            hexValTable[i] = byte(i - 'A' + 10)
        case i >= 'a' && i <= 'f':
            hexValTable[i] = byte(i - 'a' + 10)
        }
    }
}

func parseHexStringFast(s string) ([]byte, error) {
    if len(s)%2 != 0 {
        return nil, fmt.Errorf("invalid hex string: odd length")
    }
    result := make([]byte, len(s)/2)
    for i := 0; i < len(s); i += 2 {
        hi := hexValTable[s[i]]
        lo := hexValTable[s[i+1]]
        if hi > 15 || lo > 15 {
            return nil, fmt.Errorf("invalid hex: %s", s[i:i+2])
        }
        result[i/2] = (hi << 4) | lo
    }
    return result, nil
}
```

#### 1.3 Token Pre-allocation

```go
// In tokenizer.go Tokenize()
func (t *Tokenizer) Tokenize() ([]Token, error) {
    estimated := len(t.input) / 8
    if estimated < 16 {
        estimated = 16
    }
    t.tokens = make([]Token, 0, estimated)
    // ...
}
```

---

### Phase 2: DS Optimizations

#### 2.1 VarintLen - Bit Manipulation

```go
// In encoding.go
import "math/bits"

func VarintLen(v int64) int {
    uv := uint64(v)
    if uv < 0x80 {
        return 1
    }
    // Count bits needed, convert to varint bytes
    bitsNeeded := bits.Len64(uv | 1)
    return (bitsNeeded + 6) / 7
}
```

#### 2.2 Record Encoding - sync.Pool

```go
// In encoding.go
var recordBufferPool = sync.Pool{
    New: func() interface{} {
        return new(bytes.Buffer)
    },
}

func EncodeRecordPooled(values []interface{}) []byte {
    buf := recordBufferPool.Get().(*bytes.Buffer)
    buf.Reset()
    defer recordBufferPool.Put(buf)
    // ... encoding logic
    return buf.Bytes()
}
```

#### 2.3 Page Prefetch - Worker Pool

```go
// In btree.go
type Prefetcher struct {
    workers int
    tasks   chan func()
    wg      sync.WaitGroup
}

func (bt *BTree) prefetchChildren(page *Page, count int) {
    // ... validation
    for i := 0; i < count; i++ {
        childNum := getChildPageNum(page, i)
        cn := childNum
        select {
        case globalPrefetcher.tasks <- func() {
            bt.pm.ReadPage(cn) //nolint:errcheck
        }:
        default:
            bt.pm.ReadPage(cn) //nolint:errcheck
        }
    }
}
```

#### 2.4 BTree Cell Search - Caching

```go
// In btree.go - pre-decode keys for binary search
func (bt *BTree) findCellOptimized(page *Page, key []byte) int {
    numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))
    
    // Pre-decode all cell keys once
    cellKeys := make([][]byte, numCells)
    for i := 0; i < numCells; i++ {
        cellKeys[i] = decodeCellKey(page, i)
    }
    
    // Binary search on pre-decoded keys
    left, right := 0, numCells
    for left < right {
        mid := (left + right) / 2
        cmp := CompareKeys(key, cellKeys[mid])
        if cmp < 0 {
            right = mid
        } else if cmp > 0 {
            left = mid + 1
        } else {
            return mid
        }
    }
    return left
}
```

---

## Files to Modify

```
internal/QP/
├── tokenizer.go                  # Keyword lookup, hex parsing, pre-allocation
└── bench_tokenizer_test.go     # NEW - Tokenizer benchmarks

internal/DS/
├── encoding.go                  # Varint optimization, sync.Pool
├── btree.go                    # Cell search caching, prefetch worker pool
├── bench_encoding_test.go      # NEW - Encoding benchmarks
├── bench_btree_test.go        # NEW - BTree benchmarks
└── bench_record_test.go       # NEW - Record benchmarks
```

---

## Tasks

### Phase 1: QP Optimizations
- [x] Implement switch-based keyword lookup in tokenizer.go
- [x] Implement hex string lookup table in tokenizer.go
- [x] Add token slice pre-allocation in tokenizer.go
- [x] Create tokenizer benchmarks and verify improvement

### Phase 2: DS Optimizations
- [x] Optimize VarintLen with bit manipulation in encoding.go
- [x] Implement sync.Pool for record encoding in encoding.go
- [x] Implement worker pool prefetch in btree.go
- [x] Implement cell key caching in btree.go (findCell pre-decodes all keys)
- [x] Create DS benchmarks and verify improvements

### Phase 3: Validation
- [x] Run full test suite
- [x] Run benchmarks and compare
- [x] Update HISTORY.md

---

## Success Criteria

| Criteria | Target |
|----------|--------|
| All existing tests pass | 100% |
| QP benchmarks improve | >15% faster |
| DS benchmarks improve | >20% faster |
| Memory allocations reduced | >25% fewer |
| No new test failures | 0 |

---

## Benchmark Commands

```bash
# Run QP benchmarks
go test ./internal/QP/... -bench=. -benchmem

# Run DS benchmarks
go test ./internal/DS/... -bench=. -benchmem

# Profile specific benchmark
go test ./internal/QP/... -bench=BenchmarkTokenizer_FullQuery -cpuprofile=cpu.prof
go tool pprof cpu.prof
```

---

## Timeline Estimate

| Phase | Tasks | Estimated Hours |
|-------|-------|-----------------|
| 1 | QP Optimizations | 10 |
| 2 | DS Optimizations | 14 |
| 3 | Validation | 6 |

**Total**: ~30 hours

---

## Notes

- Focus on hot-path optimizations first
- Keep code readable with clear comments
- Run benchmarks multiple times for stability
- Use pprof to verify improvements

