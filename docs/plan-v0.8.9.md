# Plan v0.8.9 - CLI Tools

## Summary

1. Rename `cmd/sqlvibe` to `cmd/sv-cli` (CLI application)
2. Add core library APIs in `pkg/sqlvibe` for information schema and integrity
3. Implement CLI dot commands in `sv-cli`
4. Add new `sv-check` tool
5. Add comprehensive test cases (L2 temp files only)

**Architecture**: CLI tools are thin wrappers. Core logic resides in `pkg/sqlvibe` and `internal/`.

```
┌─────────────────────────────────────────────┐
│               cmd/sv-cli                     │
│         (thin wrapper - UI only)            │
│  - dot commands parsing                     │
│  - output formatting                       │
│  - user input handling                      │
└──────────────────┬──────────────────────────┘
                   │ calls
                   ▼
┌─────────────────────────────────────────────┐
│              pkg/sqlvibe                    │
│         (core library API)                  │
│  - Database.Open/Query/Exec                │
│  - GetTables() -> []string                  │
│  - GetSchema(table) -> string              │
│  - GetIndexes(table) -> []IndexInfo        │
│  - CheckIntegrity() -> IntegrityReport     │
│  - BackupTo(path)                          │
└─────────────────────────────────────────────┘
```

**Previous**: v0.8.5 delivers WAL enhancements, MVCC, compression, incremental backup

**v0.8.9 Scope**:
- CLI Rename: 1h
- Core APIs in pkg/sqlvibe: 6h
- CLI Dot Commands: 4h
- sv-check Tool: 6h
- Testing: 4h

---

## Phase 1: Rename cmd/sqlvibe to cmd/sv-cli (1h)

### Overview

Rename CLI application from `cmd/sqlvibe` to `cmd/sv-cli`.

### Steps

```bash
mv cmd/sqlvibe cmd/sv-cli
```

### Tasks

- [ ] Rename `cmd/sqlvibe` directory to `cmd/sv-cli`
- [ ] Verify build passes

**Workload:** ~1 hour

---

## Phase 2: Core APIs in pkg/sqlvibe (6h)

### Overview

Add library functions in `pkg/sqlvibe` for information schema and integrity checks. CLI tools will call these APIs.

### 2.1 Information Schema APIs

```go
// pkg/sqlvibe/info.go

type TableInfo struct {
    Name    string
    Type    string // "table", "view"
    SQL     string // CREATE statement
}

type IndexInfo struct {
    Name        string
    Table       string
    Unique      bool
    Columns     []string
    SQL         string
}

type ColumnInfo struct {
    Name     string
    Type     string
    NotNull  bool
    Default  interface{}
    PrimaryKey bool
}

// GetTables returns all user tables (excluding sqlite_*)
func (db *Database) GetTables() ([]TableInfo, error)

// GetSchema returns CREATE statement for a table
func (db *Database) GetSchema(table string) (string, error)

// GetIndexes returns all indexes for a table
func (db *Database) GetIndexes(table string) ([]IndexInfo, error)

// GetColumns returns column info for a table
func (db *Database) GetColumns(table string) ([]ColumnInfo, error)
```

### 2.2 Integrity APIs

```go
// pkg/sqlvibe/integrity.go

type IntegrityReport struct {
    Valid           bool
    Errors          []string
    PageCount       int
    FreePages       int
    SchemaErrors    []string
    RowCountErrors  []string
}

// CheckIntegrity runs integrity checks
func (db *Database) CheckIntegrity() (*IntegrityReport, error)

// GetDatabaseInfo returns database metadata
type DatabaseInfo struct {
    FilePath    string
    FileSize    int64
    PageSize    int
    PageCount   int
    FreePages   int
    WALMode     bool
    Encoding    string
}

func (db *Database) GetDatabaseInfo() (*DatabaseInfo, error)

// GetPageInfo returns page statistics
type PageStats struct {
    LeafPages      int
    InteriorPages  int
    OverflowPages  int
    TotalPages    int
}

func (db *Database) GetPageStats() (*PageStats, error)
```

### 2.3 Backup APIs

```go
// pkg/sqlvibe/backup.go

// BackupTo creates a backup of the database
func (db *Database) BackupTo(path string) error

// BackupConfig for backup options
type BackupConfig struct {
    Progress     bool
    PagesPerStep int
}

func (db *Database) BackupToWithConfig(path string, cfg BackupConfig) error
```

### Tasks

- [ ] Add `GetTables()` function
- [ ] Add `GetSchema(table)` function
- [ ] Add `GetIndexes(table)` function
- [ ] Add `GetColumns(table)` function
- [ ] Add `CheckIntegrity()` function
- [ ] Add `GetDatabaseInfo()` function
- [ ] Add `GetPageStats()` function
- [ ] Add `BackupTo(path)` function

### API Summary

| Function | Returns | Description |
|----------|--------|-------------|
| `GetTables()` | `[]TableInfo` | List all tables |
| `GetSchema(table)` | `string` | Get CREATE statement |
| `GetIndexes(table)` | `[]IndexInfo` | List table indexes |
| `GetColumns(table)` | `[]ColumnInfo` | List column info |
| `CheckIntegrity()` | `IntegrityReport` | Run integrity check |
| `GetDatabaseInfo()` | `DatabaseInfo` | Get DB metadata |
| `GetPageStats()` | `PageStats` | Get page statistics |
| `BackupTo(path)` | `error` | Backup database |

**Workload:** ~6 hours

---

## Phase 3: CLI Dot Commands (4h)

### Overview

Implement dot commands in `sv-cli` using pkg APIs. Thin wrappers only.

### Implementation

```go
// cmd/sv-cli/main.go

func handleMetaCommand(line string) bool {
    switch strings.ToLower(line) {
    case ".tables":
        listTables(db)
        return false
    }
    // ...
}

func listTables(db *sqlvibe.Database) {
    tables, err := db.GetTables()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        return
    }
    for _, t := range tables {
        fmt.Println(t.Name)
    }
}

func showSchema(db *sqlvibe.Database, tableName string) {
    sql, err := db.GetSchema(tableName)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        return
    }
    fmt.Println(sql)
}
```

### Dot Commands

| Command | Description | Uses API |
|---------|-------------|----------|
| `.tables` | List all tables | `GetTables()` |
| `.schema [table]` | Show CREATE statements | `GetSchema()` |
| `.headers on\|off` | Toggle column headers | (local state) |
| `.indexes [table]` | List indexes | `GetIndexes()` |

### Tasks

- [ ] Implement `.tables` command using `db.GetTables()`
- [ ] Implement `.schema` command using `db.GetSchema()`
- [ ] Implement `.indexes` command using `db.GetIndexes()`
- [ ] Implement `.headers` command (local state)
- [ ] Add help text

**Workload:** ~4 hours

---

## Phase 4: sv-check Tool (6h)

### Overview

Create `sv-check` as a thin wrapper calling pkg APIs.

### Tool Location

```
cmd/sv-check/main.go
```

### Implementation

```go
// cmd/sv-check/main.go

func main() {
    flag.Parse()
    dbPath := flag.Arg(0)
    
    db, err := sqlvibe.Open(dbPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }
    defer db.Close()
    
    if *checkFlag {
        report, err := db.CheckIntegrity()
        if err != nil || !report.Valid {
            fmt.Fprintf(os.Stderr, "Check failed\n")
            os.Exit(1)
        }
        fmt.Println("Database is valid")
    }
    
    if *infoFlag {
        info, _ := db.GetDatabaseInfo()
        fmt.Printf("File: %s\n", info.FilePath)
        fmt.Printf("Size: %d bytes\n", info.FileSize)
        fmt.Printf("Pages: %d\n", info.PageCount)
    }
}
```

### Flags

| Flag | Short | Uses API | Description |
|------|-------|----------|-------------|
| `--check` | `-c` | `CheckIntegrity()` | Validate database |
| `--info` | `-i` | `GetDatabaseInfo()` | Show metadata |
| `--tables` | `-t` | `GetTables()` | List tables |
| `--schema` | `-s` | `GetSchema()` | Show schema |
| `--indexes` | | `GetIndexes()` | Show indexes |
| `--pages` | `-p` | `GetPageStats()` | Page stats |
| `--verbose` | `-v` | | Verbose output |

### Tasks

- [ ] Create `cmd/sv-check/main.go`
- [ ] Implement `--check` using `db.CheckIntegrity()`
- [ ] Implement `--info` using `db.GetDatabaseInfo()`
- [ ] Implement `--tables` using `db.GetTables()`
- [ ] Implement `--schema` using `db.GetSchema()`
- [ ] Implement `--indexes` using `db.GetIndexes()`
- [ ] Implement `--pages` using `db.GetPageStats()`

**Workload:** ~6 hours

---

## Phase 5: Testing (4h)

### Overview

All tests use **temp files only** (L2) - no files in source tree.

### Test Requirements

- Use `t.TempDir()` for all file creation
- Always defer `os.Remove()` cleanup
- Test pkg APIs directly, not CLI wrappers

### pkg/sqlvibe Tests

```go
// pkg/sqlvibe/info_test.go

func TestGetTables(t *testing.T) {
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")
    db, _ := sqlvibe.Open(dbPath)
    defer db.Close()
    defer os.Remove(dbPath)
    
    db.Exec("CREATE TABLE users (id INT)")
    db.Exec("CREATE TABLE posts (id INT)")
    
    tables, err := db.GetTables()
    if err != nil {
        t.Fatal(err)
    }
    if len(tables) != 2 {
        t.Errorf("Expected 2 tables, got %d", len(tables))
    }
}

func TestGetSchema(t *testing.T) {
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")
    db, _ := sqlvibe.Open(dbPath)
    defer db.Close()
    defer os.Remove(dbPath)
    
    db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    
    schema, err := db.GetSchema("users")
    if err != nil {
        t.Fatal(err)
    }
    if !strings.Contains(schema, "CREATE TABLE users") {
        t.Error("Missing CREATE TABLE in schema")
    }
}

func TestCheckIntegrity(t *testing.T) {
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")
    db, _ := sqlvibe.Open(dbPath)
    defer db.Close()
    defer os.Remove(dbPath)
    
    db.Exec("CREATE TABLE users (id INT)")
    db.Exec("INSERT INTO users VALUES (1)")
    
    report, err := db.CheckIntegrity()
    if err != nil {
        t.Fatal(err)
    }
    if !report.Valid {
        t.Error("Expected valid integrity")
    }
}

func TestGetDatabaseInfo(t *testing.T) {
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")
    db, _ := sqlvibe.Open(dbPath)
    defer db.Close()
    defer os.Remove(dbPath)
    
    info, err := db.GetDatabaseInfo()
    if err != nil {
        t.Fatal(err)
    }
    if info.FilePath != dbPath {
        t.Error("File path mismatch")
    }
}
```

### Test Coverage

| API | Tests |
|-----|-------|
| `GetTables()` | empty, single, multiple, exclude system |
| `GetSchema()` | empty, single table, non-existent |
| `GetIndexes()` | empty, with indexes |
| `GetColumns()` | basic, with constraints |
| `CheckIntegrity()` | valid DB, corrupted |
| `GetDatabaseInfo()` | basic info |
| `GetPageStats()` | page counts |

### Tasks

- [ ] Test `GetTables()` - 4 test cases
- [ ] Test `GetSchema()` - 3 test cases
- [ ] Test `GetIndexes()` - 2 test cases
- [ ] Test `CheckIntegrity()` - 2 test cases
- [ ] Test `GetDatabaseInfo()` - 2 test cases
- [ ] Test `GetPageStats()` - 1 test case

**Workload:** ~4 hours

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | CLI Rename | 1 |
| 2 | Core APIs in pkg | 6 |
| 3 | CLI Dot Commands | 4 |
| 4 | sv-check Tool | 6 |
| 5 | Testing | 4 |

**Total:** ~21 hours

---

## Success Criteria

### Phase 1: CLI Rename

| Criteria | Target | Status |
|----------|--------|--------|
| Directory renamed | cmd/sv-cli | [ ] |
| Build passes | Yes | [ ] |

### Phase 2: Core APIs

| Criteria | Target | Status |
|----------|--------|--------|
| `GetTables()` works | Returns tables | [ ] |
| `GetSchema()` works | Returns CREATE | [ ] |
| `GetIndexes()` works | Returns indexes | [ ] |
| `GetColumns()` works | Returns columns | [ ] |
| `CheckIntegrity()` works | Returns report | [ ] |
| `GetDatabaseInfo()` works | Returns info | [ ] |
| `GetPageStats()` works | Returns stats | [ ] |
| `BackupTo()` works | Creates backup | [ ] |

### Phase 3: CLI Dot Commands

| Criteria | Target | Status |
|----------|--------|--------|
| `.tables` works | Lists tables | [ ] |
| `.schema` works | Shows CREATE | [ ] |
| `.indexes` works | Shows indexes | [ ] |
| `.headers` works | Toggles headers | [ ] |

### Phase 4: sv-check

| Criteria | Target | Status |
|----------|--------|--------|
| `--check` works | Validates DB | [ ] |
| `--info` works | Shows metadata | [ ] |
| `--tables` works | Lists tables | [ ] |
| `--schema` works | Shows schema | [ ] |
| `--indexes` works | Shows indexes | [ ] |
| `--pages` works | Shows stats | [ ] |

### Phase 5: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| API tests | 14 tests | [ ] |
| All tests pass | 100% | [ ] |
| Temp files only | L2 only | [ ] |

---

## Architecture Notes

- **pkg/sqlvibe**: Core library with all business logic
- **cmd/sv-cli**: Thin UI wrapper, parses input → calls pkg → formats output
- **cmd/sv-check**: Thin UI wrapper, parses flags → calls pkg → formats output
- Easy to test: test pkg APIs directly, CLI tests are optional
- Reusable: other tools can import pkg

---

## Future CLI Tools (v0.9.0+)

| Tool | Description |
|------|-------------|
| `sv-dump` | Database export (backup) |
| `sv-import` | Data import |
| `sv-exec` | Execute SQL file |
| `sv-recover` | Corrupted DB recovery |

These would also be thin wrappers around pkg APIs.
