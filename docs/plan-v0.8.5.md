# Plan v0.8.5 - Concurrency & Storage Enhancements

## Summary

Enhance sqlvibe with concurrent transaction support and advanced storage capabilities. This includes WAL (Write-Ahead Logging), MVCC, better compression, and incremental backup.

**Previous**: v0.8.4 delivers Window Functions, GROUP_CONCAT, Recursive CTE, ON CONFLICT, ANY/ALL

**v0.8.5 Scope**: ~80 hours total
- Part 1: Concurrency & Transactions - 45h
- Part 2: Storage Enhancements - 35h

---

## Problem Statement

Current state:
- Single-writer architecture - no concurrent writes
- No WAL - writes are synchronous
- Basic compression - room for improvement
- No incremental backup capability

Goals:
- Enable concurrent reads with MVCC
- Add WAL for async writes and crash recovery
- Improve compression ratio
- Add incremental backup

---

## Part 1: Concurrency & Transactions (45h)

### Phase 1.1: WAL (Write-Ahead Logging)

Implement WAL mode for async writes and crash recovery.

#### Architecture

```
┌─────────────────────────────────────────┐
│              Writer                      │
│  ┌─────────┐    ┌──────────┐           │
│  │ WAL Log │───▶│ Page Cache│           │
│  └─────────┘    └──────────┘           │
│       │               │                 │
│       ▼               ▼                 │
│  ┌─────────────────────────────────┐    │
│  │         HybridStore              │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘

Reader: Directly reads from HybridStore (no lock)
Writer: Appends to WAL, checkpoints periodically
```

#### Implementation

```go
// internal/TM/wal.go
type WAL struct {
    logFile *os.File
    frame  []byte
    // Frame header: pageNum(4) + pageSize(4) + commitID(8) + crc(4)
}

type WALWriter struct {
    wal    *WAL
    buf    *bufio.Writer
    // WAL frame format:
    // - Magic (4 bytes): 0x377F0682
    // - Frame size (4 bytes)
    // - Page number (4 bytes)
    // - Commit ID (8 bytes)
    // - Page data (variable)
    // - CRC (4 bytes)
}

func (wal *WAL) Append(pageNum uint32, data []byte) error
func (wal *WAL) Checkpoint() error
func (wal *WAL) Recover() error
```

#### Features

- **WAL mode toggle**: PRAGMA wal_mode = ON/OFF
- **Checkpoint**: Auto-checkpoint every N pages or timeout
- **WAL recovery**: Replay committed transactions on startup
- **WAL archive**: Optional archive mode for incremental backup

#### Tasks

- [x] Add WAL struct and frame format
- [x] Implement WAL.Append() for log writes
- [x] Implement WAL.Checkpoint() to flush to main store
- [x] Implement WAL.Recover() for crash recovery
- [x] Add PRAGMA wal_mode
- [x] Integrate with Transaction Manager
- [x] Tests: WAL append, checkpoint, recovery

**Workload:** ~15 hours

---

### Phase 1.2: MVCC (Multi-Version Concurrency Control)

Enable concurrent reads without locks using snapshot isolation.

#### Architecture

```go
// Each read gets a consistent snapshot
type Transaction struct {
    ID        uint64
    StartTime uint64  // Commit ID watermark
    Mode      TransactionMode // READ_ONLY, READ_WRITE
    Snapshot  *Snapshot
}

type Snapshot struct {
    CommitID    uint64           // Visible up to this commit
    ActiveTxns map[uint64]bool  // Active transactions
}

// Read: See all commits with CommitID <= snapshot.CommitID
// Write: Create new version, visible after commit
```

#### Visibility Rules

```
┌──────────────────────────────────────────┐
│  Transaction T1 writes: x=5             │
│  Transaction T1 commits (CommitID=100)   │
│                                          │
│  Transaction T2 reads: x=?              │
│  T2 sees x=5 if:                        │
│    - T2.StartTime > 100 (snapshot after)│
│                                          │
│  T2 sees old x if:                      │
│    - T2.StartTime <= 100 (snapshot before)│
└──────────────────────────────────────────┘
```

#### Implementation

```go
// internal/TM/mvcc.go
type MVCCStore struct {
    store    *DS.HybridStore
    versions map[uint64]map[string]interface{} // CommitID -> {key -> value}
    commitID uint64
    mu       sync.RWMutex
}

func (mvcc *MVCCStore) Get(key string, snapshot *Snapshot) (interface{}, error)
func (mvcc *MVCCStore) Put(key string, value interface{}) uint64 // Returns commitID
func (mvcc *MVCCStore) Delete(key string) uint64
func (mvcc *MVCCStore) Commit() error
func (mvcc *MVCCStore) Rollback()

// Version cleanup (lazy cleanup of old versions)
func (mvcc *MVCCStore) GC(keepVersions int)
```

#### Tasks

- [x] Add MVCCStore struct
- [x] Implement Get with snapshot visibility
- [x] Implement Put/Delete with versioning
- [x] Implement Commit/Rollback
- [x] Add GC for old version cleanup
- [x] Integrate with Database
- [x] Tests: concurrent reads, write visibility

**Workload:** ~15 hours

---

### Phase 1.3: Transaction Isolation Levels

Implement proper transaction isolation.

```sql
-- Supported isolation levels
PRAGMA isolation_level = READ UNCOMMITTED;
PRAGMA isolation_level = READ COMMITTED;
PRAGMA isolation_level = SERIALIZABLE;
```

| Level | Behavior |
|-------|----------|
| READ UNCOMMITTED | See uncommitted changes (rarely used) |
| READ COMMITTED | See committed changes only (default) |
| SERIALIZABLE | All reads are consistent, no phantoms |

#### Implementation

```go
type IsolationLevel int
const (
    ReadUncommitted IsolationLevel = iota
    ReadCommitted
    Serializable
)

type TransactionManager struct {
    mvcc        *MVCCStore
    wal         *WAL
    isolation   IsolationLevel
    activeTxns  map[uint64]*Transaction
}
```

#### Tasks

- [x] Add IsolationLevel type
- [x] Add PRAGMA isolation_level
- [x] Implement READ COMMITTED (default)
- [x] Implement SERIALIZABLE (pessimistic locking)
- [x] Tests: isolation level behavior

**Workload:** ~8 hours

---

### Phase 1.4: Deadlock Detection & Timeout

Handle transaction deadlocks gracefully.

```sql
-- Transaction timeout
PRAGMA busy_timeout = 5000; -- 5 seconds

-- Check for locks
SELECT * FROM sqlite_master WHERE type='table' AND name='t1';
-- Returns error if table is locked
```

#### Implementation

```go
type LockManager struct {
    locks   map[string]*TableLock
    timeout time.Duration
}

type TableLock struct {
    Mode   LockMode // READ, WRITE
    holder uint64   // Transaction ID
    queue  []uint64 // Waiting transactions
}

func (lm *LockManager) Acquire(txnID uint64, table string, mode LockMode) error
func (lm *LockManager) Release(txnID uint64, table string)
func (lm *LockManager) DetectDeadlock() (deadlock bool, victim uint64)
```

#### Tasks

- [x] Add LockManager
- [x] Implement table-level locking
- [x] Implement deadlock detection
- [x] Add busy_timeout PRAGMA
- [x] Tests: deadlock detection, timeout

**Workload:** ~7 hours

---

## Part 2: Storage Enhancements (35h)

### Phase 2.1: Advanced Compression

Improve compression ratio with better algorithms.

#### Compression Options

| Algorithm | Ratio | Speed | Use Case |
|-----------|-------|-------|----------|
| None | 1x | ∞ | In-memory |
| RLE | 1.5-3x | Fast | Sequential data |
| LZ4 | 2-4x | Very Fast | Fast compression |
| ZSTD | 3-10x | Medium | Best ratio |
| GZIP | 2-5x | Medium | Compatibility |

#### Implementation

```go
// internal/DS/compression.go
type Compressor interface {
    Compress(src []byte) ([]byte, error)
    Decompress(src []byte) ([]byte, error)
    Name() string
}

type ZSTDCompressor struct {
    level int // 1-19
}

type LZ4Compressor struct {
    blockSize int
}

func NewCompressor(name string, level int) (Compressor, error)

// PRAGMA to select
PRAGMA compression = 'ZSTD'; -- 'NONE', 'RLE', 'LZ4', 'ZSTD', 'GZIP'
```

#### Tasks

- [x] Add Compressor interface
- [x] Implement ZSTD compressor (best ratio)
- [x] Implement LZ4 compressor (fast)
- [x] Add PRAGMA compression
- [x] Integrate with persistence layer
- [x] Tests: compression ratio benchmarks

**Workload:** ~12 hours

---

### Phase 2.2: Incremental Backup

Support backup of changed pages only.

#### Architecture

```
┌──────────────┐     ┌──────────────┐
│   Source DB  │     │  Backup DB   │
│              │     │              │
│ Page 1 ──────│────▶│ Page 1       │ (changed)
│ Page 2 ──────│     │ (unchanged)  │
│ Page 3 ──────│────▶│ Page 3       │ (changed)
└──────────────┘     └──────────────┘

Backup tracks: lastBackupCommitID
Incremental:  copy pages with CommitID > lastBackupCommitID
```

#### Implementation

```go
// internal/DS/backup.go
type IncrementalBackup struct {
    source    *HybridStore
    backup    *HybridStore
    lastCommitID uint64
}

func (ib *IncrementalBackup) Start() error
func (ib *IncrementalBackup) Next() (pagesCopied int, err error)
func (ib *IncrementalBackup) Close() error

// SQL interface
-- Full backup
BACKUP DATABASE TO 'backup.db';

-- Incremental backup (requires prior backup)
BACKUP INCREMENTAL TO 'backup.db';
```

#### Tasks

- [x] Add IncrementalBackup struct
- [x] Implement page-level change tracking
- [x] Implement incremental copy
- [x] Add BACKUP SQL command
- [x] Tests: full backup, incremental backup

**Workload:** ~10 hours

---

### Phase 2.3: Data Page Compression

Compress individual pages for better memory efficiency.

#### Page-Level Compression

```go
// internal/DS/page.go
type Page struct {
    Num         uint32
    Data        []byte        // Compressed data
    IsCompressed bool
    UncompressedSize int
}

func (p *Page) Compress(compressor Compressor) error
func (p *Page) Decompress(compressor Compressor) error

// When loading page:
// 1. Read compressed bytes from disk
// 2. Decompress to get raw data
// 3. Parse as B-tree leaf/interior page
```

#### Tasks

- [x] Add page-level compression in Page struct
- [x] Compress on write, decompress on read
- [x] Track compression metadata in header
- [x] Tests: page compression ratio

**Workload:** ~8 hours

---

### Phase 2.4: Storage Metrics & Monitoring

Add storage statistics.

```sql
-- Storage info
PRAGMA storage_info;

-- Returns:
-- - Page count
-- - Free pages
-- - Compression ratio
-- - WAL size
-- - Last checkpoint
```

#### Implementation

```go
// internal/DS/metrics.go
type StorageMetrics struct {
    PageCount       int
    UsedPages       int
    FreePages       int
    CompressionRatio float
    WALSize         int64
    LastCheckpoint  time.Time
}

func (db *Database) StorageInfo() (*StorageMetrics, error)
```

#### Tasks

- [x] Add StorageMetrics struct
- [x] Implement storage_info PRAGMA
- [x] Add compression stats
- [x] Add WAL stats

**Workload:** ~5 hours

---

## Success Criteria

### Part 1: Concurrency

| Criteria | Target | Status |
|----------|--------|--------|
| WAL append | Works | [x] |
| WAL checkpoint | Works | [x] |
| WAL recovery | Works | [x] |
| MVCC reads | Works | [x] |
| MVCC writes | Works | [x] |
| READ COMMITTED | Works | [x] |
| SERIALIZABLE | Works | [x] |
| Deadlock detection | Works | [x] |
| busy_timeout | Works | [x] |
| Concurrent read/write | No corruption | [x] |

### Part 2: Storage

| Criteria | Target | Status |
|----------|--------|--------|
| ZSTD compression | 3-10x ratio | [x] |
| LZ4 compression | 2-4x ratio | [x] |
| Page-level compression | Works | [x] |
| Full backup | Works | [x] |
| Incremental backup | Works | [x] |
| storage_info PRAGMA | Works | [x] |
| Compression ratio > 2x | Yes | [x] |

---

## Timeline Estimate

| Phase | Tasks | Hours |
|-------|-------|-------|
| 1.1 | WAL | 15 |
| 1.2 | MVCC | 15 |
| 1.3 | Isolation Levels | 8 |
| 1.4 | Deadlock/Timeout | 7 |
| **Subtotal** | **Concurrency** | **45** |
| 2.1 | Advanced Compression | 12 |
| 2.2 | Incremental Backup | 10 |
| 2.3 | Page Compression | 8 |
| 2.4 | Storage Metrics | 5 |
| **Subtotal** | **Storage** | **35** |

**Total**: ~80 hours

---

## Benchmark Commands

```bash
# WAL benchmarks
go test ./internal/TM/... -bench=WAL -benchtime=3s

# MVCC benchmarks
go test ./internal/TM/... -bench=MVCC -benchtime=3s

# Compression benchmarks
go test ./internal/DS/... -bench=Compress -benchtime=3s

# Concurrent benchmarks
go test ./internal/TS/Benchmark/... -bench=Concurrent -benchtime=3s
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| WAL complexity | High | Implement sync mode first, then WAL |
| MVCC memory overhead | Medium | Add GC for old versions |
| Compression speed | Medium | Add fast path for uncompressible data |
| Incremental backup complexity | Medium | Start with full backup, then incremental |

---

## Notes

- WAL requires careful sync handling - implement basic version first
- MVCC adds memory overhead - consider lazy cleanup
- ZSTD requires CGO or pure-Go port - use pure-Go if available
- Incremental backup requires consistent page tracking
