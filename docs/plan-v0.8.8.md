# Plan v0.8.8 - Production-Ready: Error System + CPU Pipeline Optimization

## Summary

Design and implement:
1. A unified error code system for sqlvibe based on SQLite error codes
2. CPU pipeline optimizations (prefetch, branch prediction, memory alignment)
3. Lock optimizations for multicore performance

**Previous**: v0.8.7 delivers Views, VACUUM, ANALYZE, PRAGMAs, builtin functions

**v0.8.8 Scope**: ~44 hours total
- Error Code Definitions: 8h
- Error Struct & API: 6h
- Error Mapping: 8h
- Integration: 5h
- Testing: 3h
- Lock Optimization: 8h
- **CPU Pipeline Optimization: 6h**

---

## Problem Statement

Current sqlvibe:
- Uses `fmt.Errorf` and `errors.New` inconsistently
- No unified error code system
- No way to programmatically check error types
- Poor SQLite compatibility in error handling

Goals:
- Consistent error handling across all packages
- SQLite-compatible error codes (SVDB_* prefix)
- Programmatic error code checking
- Better error messages for debugging

---

## Phase 1: Error Code Definitions (8h)

### Overview

Define all error codes following SQLite specification with SVDB_ prefix.

### Error Code Categories

| Category | Code Range | Description |
|----------|------------|-------------|
| Success | 0 | Operation successful |
| Primary | 1-28 | Main error codes |
| Row | 100 | Row available |
| Done | 101 | Operation complete |
| Extended | 256+ | Detailed errors |

### Implementation

```go
// pkg/sqlvibe/error_code.go

type ErrorCode int32

// Primary error codes (0-28)
const (
    SVDB_OK         ErrorCode = 0
    SVDB_ERROR      ErrorCode = 1
    SVDB_INTERNAL   ErrorCode = 2
    SVDB_PERM       ErrorCode = 3
    SVDB_ABORT      ErrorCode = 4
    SVDB_BUSY       ErrorCode = 5
    SVDB_LOCKED     ErrorCode = 6
    SVDB_NOMEM      ErrorCode = 7
    SVDB_READONLY   ErrorCode = 8
    SVDB_INTERRUPT  ErrorCode = 9
    SVDB_IOERR      ErrorCode = 10
    SVDB_CORRUPT    ErrorCode = 11
    SVDB_NOTFOUND   ErrorCode = 12
    SVDB_FULL       ErrorCode = 13
    SVDB_CANTOPEN   ErrorCode = 14
    SVDB_PROTOCOL   ErrorCode = 15
    SVDB_EMPTY      ErrorCode = 16
    SVDB_SCHEMA     ErrorCode = 17
    SVDB_TOOBIG     ErrorCode = 18
    SVDB_CONSTRAINT ErrorCode = 19
    SVDB_MISMATCH   ErrorCode = 20
    SVDB_MISUSE     ErrorCode = 21
    SVDB_NOLFS      ErrorCode = 22
    SVDB_AUTH       ErrorCode = 23
    SVDB_FORMAT     ErrorCode = 24
    SVDB_RANGE      ErrorCode = 25
    SVDB_NOTADB     ErrorCode = 26
    SVDB_NOTICE     ErrorCode = 27
    SVDB_WARNING    ErrorCode = 28
    
    SVDB_ROW        ErrorCode = 100
    SVDB_DONE       ErrorCode = 101
)

// Extended error codes (256+)
const (
    SVDB_OK_LOAD_PERMANENTLY ErrorCode = 256
    
    // CONSTRAINT extended
    SVDB_CONSTRAINT_CHECK       ErrorCode = 275
    SVDB_CONSTRAINT_NOTNULL    ErrorCode = 1299
    SVDB_CONSTRAINT_PRIMARYKEY ErrorCode = 1555
    SVDB_CONSTRAINT_UNIQUE     ErrorCode = 2067
    SVDB_CONSTRAINT_FOREIGNKEY ErrorCode = 787
    
    // BUSY extended
    SVDB_BUSY_RECOVERY ErrorCode = 261
    SVDB_BUSY_TIMEOUT  ErrorCode = 773
    
    // IOERR extended
    SVDB_IOERR_READ       ErrorCode = 266
    SVDB_IOERR_WRITE      ErrorCode = 778
    SVDB_IOERR_FSYNC     ErrorCode = 1034
    // ... more IOERR variants
    
    // And so on for all extended codes
)
```

### Tasks

- [ ] Define primary error codes (0-28)
- [ ] Define extended error codes (256+)
- [ ] Add error code to string mapping
- [ ] Add Primary() method to extract base code
- [ ] Add error code constants documentation

**Workload:** ~8 hours

---

## Phase 2: Error Struct & API (6h)

### Overview

Create Error struct and public API for error handling.

### Implementation

```go
// pkg/sqlvibe/error.go

type Error struct {
    Code    ErrorCode
    Message string
    Err     error  // wrapped error
}

func (e *Error) Error() string {
    return fmt.Sprintf("[%s] %s", e.Code.String(), e.Message)
}

func (e *Error) Unwrap() error {
    return e.Err
}

// Error constructors
func NewError(code ErrorCode, msg string) *Error {
    return &Error{Code: code, Message: msg}
}

func Errorf(code ErrorCode, format string, args ...interface{}) *Error {
    return &Error{
        Code:    code,
        Message: fmt.Sprintf(format, args...),
    }
}

func (e *Error) Is(target error) bool {
    if t, ok := target.(*Error); ok {
        return e.Code == t.Code
    }
    return false
}

// Error code extraction
func ErrorCodeOf(err error) ErrorCode {
    if err == nil {
        return SVDB_OK
    }
    var e *Error
    if errors.As(err, &e) {
        return e.Code
    }
    return SVDB_ERROR
}

// Check error code
func IsErrorCode(err error, code ErrorCode) bool {
    return ErrorCodeOf(err) == code
}
```

### Tasks

- [ ] Add Error struct definition
- [ ] Implement Error() and Unwrap() methods
- [ ] Add NewError, Errorf constructors
- [ ] Add Is() method for errors.Is support
- [ ] Add ErrorCodeOf function
- [ ] Add IsErrorCode helper

**Workload:** ~6 hours

---

## Phase 3: Error Mapping (8h)

### Overview

Map existing Go errors to sqlvibe error codes.

### Error Mapping Strategy

```go
// pkg/sqlvibe/error_map.go

func ToError(err error) *Error {
    if err == nil {
        return nil
    }
    
    // Already a sqlvibe error
    var se *Error
    if errors.As(err, &se) {
        return se
    }
    
    // Map based on error type/message
    switch {
    case errors.Is(err, io.EOF):
        return &Error{Code: SVDB_DONE, Message: err.Error(), Err: err}
    case errors.Is(err, io.ErrUnexpectedEOF):
        return &Error{Code: SVDB_CORRUPT, Message: err.Error(), Err: err}
    case errors.Is(err, io.ErrShortWrite):
        return &Error{Code: SVDB_IOERR_WRITE, Message: err.Error(), Err: err}
    case errors.Is(err, os.ErrNotExist):
        return &Error{Code: SVDB_NOTFOUND, Message: err.Error(), Err: err}
    case errors.Is(err, os.ErrPermission):
        return &Error{Code: SVDB_PERM, Message: err.Error(), Err: err}
    case errors.Is(err, context.DeadlineExceeded):
        return &Error{Code: SVDB_BUSY, Message: err.Error(), Err: err}
    case errors.Is(err, context.Canceled):
        return &Error{Code: SVDB_INTERRUPT, Message: err.Error(), Err: err}
    default:
        return &Error{Code: SVDB_ERROR, Message: err.Error(), Err: err}
    }
}
```

### Package-Specific Mappings

| Package | Current Errors | Target Code |
|---------|----------------|-------------|
| QP | parse errors | SVDB_ERROR |
| VM | execution errors | SVDB_ERROR / SVDB_IOERR |
| DS | storage errors | SVDB_IOERR / SVDB_CORRUPT |
| PB | file errors | SVDB_IOERR / SVDB_CANTOPEN |

### Tasks

- [ ] Create error mapping function
- [ ] Map IO errors (io.*, os.*)
- [ ] Map context errors
- [ ] Map parser errors
- [ ] Map VM errors
- [ ] Map DS errors

**Workload:** ~8 hours

---

## Phase 4: Integration (5h)

### Overview

Integrate error system across all packages.

### Integration Points

```go
// Replace existing error returns

// Before
return nil, fmt.Errorf("table %s already exists", name)

// After
return nil, NewError(SVDB_ERROR, "table %s already exists", name)
```

### Packages to Update

| Package | Files | Priority |
|---------|-------|----------|
| pkg/sqlvibe | database.go | High |
| internal/QP | parser.go, tokenizer.go | High |
| internal/VM | exec.go, engine.go | High |
| internal/DS | persistence.go, page.go | High |
| internal/PB | file.go, vfs.go | Medium |
| internal/CG | compiler.go | Medium |

### Tasks

- [ ] Update database.go errors
- [ ] Update QP parser errors
- [ ] Update VM execution errors
- [ ] Update DS storage errors
- [ ] Update PB VFS errors

**Workload:** ~5 hours

---

## Phase 5: Testing (3h)

### Overview

Add tests for error system.

### Test Cases

```go
// pkg/sqlvibe/error_test.go

func TestErrorCodeString(t *testing.T) {
    tests := []struct {
        code    ErrorCode
        want    string
    }{
        {SVDB_OK, "SVDB_OK"},
        {SVDB_ERROR, "SVDB_ERROR"},
        {SVDB_NOMEM, "SVDB_NOMEM"},
        // ...
    }
    for _, tt := range tests {
        t.Run(tt.want, func(t *testing.T) {
            if got := tt.code.String(); got != tt.want {
                t.Errorf("String() = %v, want %v", got, tt.want)
            }
        })
    }
}

func TestErrorCodePrimary(t *testing.T) {
    tests := []struct {
        code   ErrorCode
        want   ErrorCode
    }{
        {SVDB_CONSTRAINT_CHECK, SVDB_CONSTRAINT},
        {SVDB_IOERR_READ, SVDB_IOERR},
        {SVDB_BUSY_RECOVERY, SVDB_BUSY},
    }
    for _, tt := range tests {
        t.Run(tt.code.String(), func(t *testing.T) {
            if got := tt.code.Primary(); got != tt.want {
                t.Errorf("Primary() = %v, want %v", got, tt.want)
            }
        })
    }
}

func TestErrorCodeOf(t *testing.T) {
    err := NewError(SVDB_NOTFOUND, "table not found")
    if Code := ErrorCodeOf(err); Code != SVDB_NOTFOUND {
        t.Errorf("ErrorCodeOf() = %v, want %v", Code, SVDB_NOTFOUND)
    }
}
```

### Tasks

- [ ] Add error code string tests
- [ ] Add Primary() extraction tests
- [ ] Add ErrorCodeOf tests
- [ ] Add error mapping tests

**Workload:** ~3 hours

---

## Success Criteria

### Phase 1: Error Code Definitions

| Criteria | Target | Status |
|----------|--------|--------|
| Primary codes (0-28) | 29 codes | [ ] |
| Extended codes (256+) | 70+ codes | [ ] |
| Code to string mapping | Works | [ ] |
| Primary() extraction | Works | [ ] |

### Phase 2: Error Struct & API

| Criteria | Target | Status |
|----------|--------|--------|
| Error struct | Works | [ ] |
| NewError/Errorf | Works | [ ] |
| errors.Is support | Works | [ ] |
| ErrorCodeOf function | Works | [ ] |

### Phase 3: Error Mapping

| Criteria | Target | Status |
|----------|--------|--------|
| IO error mapping | Works | [ ] |
| Context error mapping | Works | [ ] |
| Parser error mapping | Works | [ ] |
| VM error mapping | Works | [ ] |

### Phase 4: Integration

| Criteria | Target | Status |
|----------|--------|--------|
| database.go updated | All errors | [ ] |
| QP errors updated | All errors | [ ] |
| VM errors updated | All errors | [ ] |
| DS errors updated | All errors | [ ] |

### Phase 5: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| Error code tests | > 50 tests | [ ] |
| All tests pass | 100% | [ ] |

### Phase 6: Lock Optimization

| Criteria | Target | Status |
|----------|--------|--------|
| RWMutex for Database | Works | [ ] |
| Sharded map | Works | [ ] |
| Atomic counters | Works | [ ] |
| Read throughput | 3x improvement | [ ] |
| Lock contention | Reduced | [ ] |

### Phase 7: CPU Pipeline Optimization

| Criteria | Target | Status |
|----------|--------|--------|
| Prefetch in scans | Works | [ ] |
| Branch predictor | Works | [ ] |
| Memory alignment | Works | [ ] |
| Scan performance | 1.5x improvement | [ ] |

---

## Phase 6: Lock Optimization (8h)

### Overview

Optimize locking for better multicore concurrency.

### Current Issues

```go
// Current: coarse-grained mutex
type Table struct {
    mu sync.Mutex
    data map[string]Row
}
```

### Optimization Strategies

#### 1. RWMutex for Read-Heavy Workloads

```go
type Table struct {
    rwmu sync.RWMutex  // Multiple readers, single writer
    data map[string]Row
}
```

#### 2. Sharded Locks

```go
type ShardedTable struct {
    shards []*TableShard
    numShards int
}

type TableShard struct {
    mu   sync.RWMutex
    data map[string]interface{}
}

func (st *ShardedTable) Get(key string) (interface{}, bool) {
    shard := st.shard(key)
    shard.mu.RLock()
    defer shard.mu.RUnlock()
    return shard.data[key]
}
```

#### 3. Atomic Counters

```go
type AtomicCounter struct {
    val int64
}

func (ac *AtomicCounter) Add(n int64) int64 {
    return atomic.AddInt64(&ac.val, n)
}

func (ac *AtomicCounter) Get() int64 {
    return atomic.LoadInt64(&ac.val)
}
```

### Tasks

- [ ] Add RWMutex to Database for concurrent reads
- [ ] Implement sharded map for HybridStore
- [ ] Replace mutex with atomic for counters
- [ ] Add lock-free read path for snapshots
- [ ] Benchmark lock contention
- [ ] Add lock metrics

**Workload:** ~8 hours

---

## Phase 7: CPU Pipeline Optimization (6h)

### Overview

Optimize VM to take advantage of modern CPU pipeline techniques.

### 1. Prefetching

```go
// Prefetch data before it's needed
type Prefetcher struct {
    depth int // Prefetch distance
}

func (p *Prefetcher) Prefetch(rows []Row, idx int) {
    // Prefetch N rows ahead
    target := idx + p.depth
    if target < len(rows) {
        cpu.PrefetchT0(rows[target].Data)
    }
}

func (hs *HybridStore) ScanWithPrefetch() {
    for i := 0; i < len(rows); i++ {
        prefetcher.Prefetch(rows, i)
        process(rows[i])
    }
}
```

### 2. Branch Prediction

```go
// 2-bit saturating counter for branch prediction
type BranchPredictor struct {
    counters map[uint64]*SatCounter
}

type SatCounter struct {
    value int // 0-3: 0=strong not taken, 3=strong taken
}

func (sc *SatCounter) Predict() bool {
    return sc.value >= 2
}

func (sc *SatCounter) Update(taken bool) {
    if taken && sc.value < 3 {
        sc.value++
    } else if !taken && sc.value > 0 {
        sc.value--
    }
}

// Use in VM for conditional jumps
func (vm *VM) execBranch(inst Instruction) {
    if vm.branchPred.Predict(inst.Addr) {
        vm.jump(inst.Target)
    }
}
```

### 3. Memory Alignment

```go
// Cache line alignment to reduce false sharing
const CacheLineSize = 64

type align64 [CacheLineSize]byte

type AlignedCounter struct {
    _    align64
    val  int64
    _    align64
}

// Use in hot paths
type HybridStoreAligned struct {
    mu           align64
    counters     align64
    data         map[string]*ColumnVector
}
```

### Tasks

- [ ] Add Prefetcher for sequential scans
- [ ] Implement 2-bit branch predictor
- [ ] Add prefetch to HybridStore scan
- [ ] Align hot data structures to cache lines
- [ ] Benchmark CPU pipeline improvements

**Workload:** ~6 hours

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | Error Code Definitions | 8 |
| 2 | Error Struct & API | 6 |
| 3 | Error Mapping | 8 |
| 4 | Integration | 5 |
| 5 | Testing | 3 |
| 6 | Lock Optimization | 8 |
| 7 | CPU Pipeline Optimization | 6 |

**Total**: ~44 hours

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | Error Code Definitions | 8 |
| 2 | Error Struct & API | 6 |
| 3 | Error Mapping | 8 |
| 4 | Integration | 5 |
| 5 | Testing | 3 |
| 6 | Lock Optimization | 8 |

**Total**: ~38 hours

---

## Benchmark Commands

```bash
# Run error tests
go test ./pkg/sqlvibe/... -v -run Error

# Run all tests
go test ./... -v -run Error

# Check error code coverage
go test ./pkg/sqlvibe/... -v -cover
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Error code conflicts | High | Careful value assignment |
| Missing mappings | Medium | Comprehensive testing |
| Performance overhead | Low | Minimal allocation |

---

## Notes

- Error codes follow SQLite specification exactly
- SVDB_ prefix distinguishes from SQLite SQLITE_ prefix
- Extended codes include base code in upper bits for easy extraction
- Consider adding error message localization in future
