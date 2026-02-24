# Plan v0.9.10 - WAL Enhancement, Storage PRAGMAs & FuzzDBFile

## Summary

This version focuses on three main areas:
1. **WAL Enhancement** - Auto-checkpoint, WAL replay on startup, checkpoint modes
2. **Storage PRAGMAs** - New diagnostic and optimization PRAGMAs
3. **FuzzDBFile** - A new fuzzer that mutates database files directly

---

## Track A: WAL Enhancement

### A1. Auto-Checkpoint Background

**Goal**: Automatically checkpoint WAL after N frames or timeout

| Feature | Description |
|---------|-------------|
| `PRAGMA wal_autocheckpoint = N` | Checkpoint after N pages (default: 1000) |
| Background goroutine | Non-blocking checkpoint in background |
| Configurable interval | Time-based checkpoint option |

**Implementation**:
```
pkg/sqlvibe/database.go:
- Add autoCheckpointInterval int to Database struct
- Add startAutoCheckpoint() method
- Add stopAutoCheckpoint() method
- Modify execCommit() to track WAL size

internal/DS/wal.go:
- Add FrameCount() method
- Add ShouldCheckpoint(frameThreshold int) bool
```

### A2. WAL Startup Replay

**Goal**: Automatically replay WAL on database open

| Feature | Description |
|---------|-------------|
| Detect WAL on open | Check for {dbname}-wal file |
| Auto replay | Call WAL.Replay() before queries |
| Recovery mode | Open in read-only if replay fails |

**Implementation**:
```
pkg/sqlvibe/database.go - Open():
- Check for WAL file existence
- If WAL exists, open and call Replay()
- Set journalMode = "wal" automatically

internal/DS/wal.go:
- Add Path() string getter
- Add Exists(path string) bool helper
```

### A3. Checkpoint Modes

**Goal**: Support SQLite-compatible checkpoint modes

| Mode | Behavior |
|------|----------|
| PASSIVE | Checkpoint if no active readers (default) |
| FULL | Exclusive lock, full checkpoint |
| TRUNCATE | Full + truncate WAL to 0 |

**Implementation**:
```
PRAGMA wal_checkpoint(passive|full|truncate)

pkg/sqlvibe/pragma.go:
- Add pragmaWALCheckpointFull() handling different modes
- Return (busy, logRemoved, checkpointed) like SQLite
```

### A4. WAL Corruption Recovery

**Goal**: Detect and recover from corrupted WAL entries

| Feature | Description |
|---------|-------------|
| Entry validation | Skip malformed JSON entries |
| Partial read | Handle truncated entries at EOF |
| CRC check | Add optional entry checksums |

**Implementation**:
```
internal/DS/wal.go - Replay():
- Add tryRecoverFromCorruption() wrapper
- Skip entries that fail JSON unmarshal
- Log skipped entries for diagnostics
```

---

## Track B: Storage PRAGMAs

### B1. `PRAGMA shrink_memory`

**Goal**: Release unused memory from caches

| Behavior |
|----------|
| Clear page cache |
| Clear query result cache |
| Force GC (optional) |

```go
func (db *Database) pragmaShrinkMemory() (*Rows, error) {
    db.pm.ClearCache()
    db.queryCache.Clear()
    db.planCache.Clear()
    return &Rows{
        Columns: []string{"size_released"},
        Data:    [][]interface{}{{releasedBytes}},
    }, nil
}
```

### B2. `PRAGMA optimize`

**Goal**: Run ANALYZE to update query planner statistics

| Behavior |
|----------|
| Analyze all tables |
| Update column histograms |
| Similar to SQLite's ANALYZE |

### B3. `PRAGMA integrity_check`

**Goal**: Full database integrity verification

| Check | Description |
|-------|-------------|
| Header magic | Verify file header |
| Footer CRC | Verify file footer |
| Schema consistency | Tables match indexes |
| Row count consistency | Verify row counts |

**Returns**: `ok` or list of errors

### B4. `PRAGMA quick_check`

**Goal**: Fast integrity check (header + footer only)

| Check | Description |
|-------|-------------|
| Magic bytes | Quick header check |
| Footer exists | Basic footer check |
| File size | Sanity check |

### B5. `PRAGMA journal_size_limit`

**Goal**: Limit WAL file size

| Behavior |
|----------|
| `journal_size_limit = N` | Max WAL size in bytes |
| Truncate on checkpoint | If WAL > limit, truncate |

### B6. `PRAGMA cache_grind` / `PRAGMA cache_size` (detailed)

**Goal**: Detailed cache statistics

| Column | Description |
|--------|-------------|
| pagesCached | Current pages in cache |
| pagesFree | Free pages available |
| hits | Cache hit count |
| misses | Cache miss count |

---

## Track C: FuzzDBFile (Database File Fuzzer - Parallel to FuzzSQL)

### C1. Overview

**Goal**: Create a parallel fuzz test inside PlainFuzzer that mutates SQLVIBE binary database files to find bugs in persistence/loading code.

**Location**: `internal/TS/PlainFuzzer/fuzz_file_test.go`

**Design**: Runs in parallel with FuzzSQL as a separate `FuzzDBFile` test function

### C2. Architecture

```
internal/TS/PlainFuzzer/
├── fuzz_test.go           # Existing FuzzSQL
├── fuzz_file_test.go       # NEW: FuzzDBFile
├── filemutator.go         # NEW: File mutation strategies
└── filecorpus.go          # NEW: Seed database files
```

### C3. Mutation Strategies

| Strategy | Description | Example |
|----------|-------------|---------|
| Header corruption | Modify magic/version/flags | Change version to 255 |
| Truncate | Cut file at random offset | Remove last 50% |
| Byte flip | Random byte changes | Flip bit in CRC |
| Structure damage | Corrupt column data | Corrupt string length |
| Footer removal | Remove footer | Set footer size to 0 |
| Padding injection | Insert null bytes | Add 100 nulls at offset 512 |
| WAL corruption | Corrupt associated WAL | Malformed JSON entries |

### C4. Seed Corpus Generation

**Design**: Generate seed database files dynamically at runtime (not pre-prepared files)

```go
// Generate seed databases at runtime
func generateSeedDatabases(tmpDir string) []string {
    var paths []string
    
    // empty.db - minimal header only
    paths = append(paths, generateEmptyDB(tmpDir))
    
    // single_table.db - one table with few rows
    paths = append(paths, generateSingleTableDB(tmpDir))
    
    // multi_table.db - multiple tables
    paths = append(paths, generateMultiTableDB(tmpDir))
    
    // with_index.db - tables with indexes
    paths = append(paths, generateIndexDB(tmpDir))
    
    // with_wal.db - database with WAL
    paths = append(paths, generateWALDB(tmpDir))
    
    return paths
}

func generateEmptyDB(tmpDir string) string {
    path := tmpDir + "/empty.db"
    // Write minimal valid header
    data := make([]byte, 256)
    copy(data[0:7], []byte("SQLVIBE\x01"))
    // ... rest of minimal header
    os.WriteFile(path, data, 0644)
    return path
}

func generateSingleTableDB(tmpDir string) string {
    path := tmpDir + "/single_table.db"
    db, _ := sqlvibe.Open(path)
    db.Exec("CREATE TABLE t1 (id INTEGER, name TEXT)")
    db.Exec("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
    db.Close()
    return path
}

// ... etc for other generators
```

### C5. Fuzz Test Interface

```go
// FuzzDBFile runs in parallel with FuzzSQL
func FuzzDBFile(f *testing.F) {
    // Generate seed databases at startup
    seedDir := f.TempDir()
    seedPaths := generateSeedDatabases(seedDir)
    for _, path := range seedPaths {
        f.Add(path)
    }
    
    f.Fuzz(func(t *testing.T, seedPath string) {
        // 1. Copy original file to temp location
        tmpPath := t.TempDir() + "/test.db"
        copyFile(seedPath, tmpPath)
        
        // 2. Apply random mutations
        mutator := NewFileMutator(rand.Int63())
        mutatedData, err := os.ReadFile(tmpPath)
        if err != nil {
            t.Skip()
        }
        mutatedData = mutator.Mutate(mutatedData)
        os.WriteFile(tmpPath, mutatedData, 0644)
        
        // 3. Try to open the database (with panic recovery)
        defer func() {
            if r := recover(); r != nil {
                // Found a bug - database panicked on corrupted file
                t.Errorf("panic on corrupted db: %v", r)
            }
        }()
        
        db, err := sqlvibe.Open(tmpPath)
        if err != nil {
            // Expected to fail - corruption is okay
            os.RemoveAll(tmpPath)
            return
        }
        defer func() {
            db.Close()
            os.RemoveAll(tmpPath)
        }()
        
        // 4. Try basic operations
        _, _ = db.Query("SELECT * FROM sqlite_master")
        
        // 5. Try to read data if tables exist
        db.Query("SELECT * FROM t1")
    })
}
```

### C6. Mutator Interface

```go
type Mutator interface {
    Mutate(data []byte) []byte
    Name() string
}

type FileMutator struct {
    seed int64
}

func NewRandomMutator(seed int64) *FileMutator {
    return &FileMutator{seed: seed}
}

func (m *FileMutator) Mutate(data []byte) []byte {
    // Apply random mutation strategy
    strategy := rand.Intn(6)
    switch strategy {
    case 0:
        return mutateHeader(data)
    case 1:
        return mutateTruncate(data)
    case 2:
        return mutateByteFlip(data)
    case 3:
        return mutateStructure(data)
    case 4:
        return mutateFooter(data)
    case 5:
        return mutatePadding(data)
    }
    return data
}

// Mutation implementations
func mutateHeader(data []byte) []byte { ... }
func mutateTruncate(data []byte) []byte { ... }
func mutateByteFlip(data []byte) []byte { ... }
func mutateStructure(data []byte) []byte { ... }
func mutateFooter(data []byte) []byte { ... }
func mutatePadding(data []byte) []byte { ... }
```

### C7. Running FuzzDBFile

```bash
# Run both FuzzSQL and FuzzDBFile in parallel
go test -fuzz=FuzzSQL -fuzz=FuzzDBFile -fuzztime=60s ./internal/TS/PlainFuzzer/...

# Run only FuzzDBFile
go test -fuzz=FuzzDBFile -fuzztime=60s ./internal/TS/PlainFuzzer/...
```

---

## Files to Modify

### WAL Enhancement
- `internal/DS/wal.go` - Add auto-checkpoint, frame counting
- `pkg/sqlvibe/database.go` - WAL replay on Open()
- `pkg/sqlvibe/pragma.go` - Add checkpoint modes

### Storage PRAGMAs
- `pkg/sqlvibe/pragma.go` - Add new PRAGMA handlers
- `pkg/sqlvibe/optimize.go` - ANALYZE implementation (new)
- `pkg/sqlvibe/integrity.go` - integrity_check implementation (new)

### FuzzDBFile (New - inside PlainFuzzer)
- `internal/TS/PlainFuzzer/fuzz_file_test.go` - NEW: FuzzDBFile test
- `internal/TS/PlainFuzzer/filemutator.go` - NEW: File mutation strategies
- `internal/TS/PlainFuzzer/filecorpus.go` - NEW: Seed corpus generation (dynamic)

---

## Success Criteria

| Feature | Target |
|---------|--------|
| Auto-checkpoint working | WAL auto-checkpoints at threshold |
| WAL replay on open | Database recovers from WAL |
| Checkpoint modes | PASSIVE/FULL/TRUNCATE work |
| WAL recovery | Corrupted WAL entries skipped |
| shrink_memory | Returns bytes released |
| optimize | ANALYZE updates statistics |
| integrity_check | Detects corruption |
| quick_check | Fast validation |
| journal_size_limit | WAL size limited |
| FuzzDBFile | Finds persistence bugs |

---

## Testing

| Test Suite | Description |
|------------|-------------|
| F880 test suite | WAL enhancement tests |
| F881 test suite | Storage PRAGMA tests |
| FuzzDBFile corpus | 5+ seed files |
| Regression tests | Bug fixes from fuzzer |
