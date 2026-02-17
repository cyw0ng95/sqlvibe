# SQLite-Compatible Database Engine Architecture

## Project Overview

**Project Name**: sqlvibe - SQLite-Compatible Database Engine in Go  
**Language**: Golang  
**Goal**: Achieve SQLite features and compatibility with blackbox-level correctness verification

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Interface Layer                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  Library Binding (pkg/sqlvibe)  │  CLI Tool (cmd/sqlvibe)                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Core Subsystems                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────┐ │
│  │   Query Processing   │───▶│  Code Generator     │───▶│  Virtual     │ │
│  │       (QP)           │    │       (CG)          │    │  Machine     │ │
│  │                      │    │                     │    │     (VM)     │ │
│  │  - Tokenizer         │    │  - Expression       │    │              │ │
│  │  - Parser            │    │    Compiler         │    │  - Bytecode  │ │
│  │  - Planner           │    │  - DML Compiler     │    │    Executor  │ │
│  │  - AST Generator     │    │  - Aggregate        │    │  - Register  │ │
│  │                      │    │    Compiler         │    │    Manager   │ │
│  │                      │    │  - Optimizer        │    │  - Cursor    │ │
│  │                      │    │    (future)         │    │    Manager   │ │
│  └──────────────────────┘    └──────────────────────┘    └──────────────┘ │
│                                                                             │
│              ▼                                                              │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                     Transaction Monitor (TM)                    │       │
│  │                                                                  │       │
│  │  - ACID Transaction Management                                   │       │
│  │  - Concurrency Control (Lock Manager)                            │       │
│  │  - Write-Ahead Log (WAL)                                        │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│              │                                                          │
│              ▼                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                      Data Storage (DS)                          │       │
│  │                                                                  │       │
│  │  - B-Tree Storage Engine                                         │       │
│  │  - Page Cache / Buffer Pool                                     │       │
│  │  - Free List Manager                                             │       │
│  │  - Database Header                                               │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│              │                                                          │
│              ▼                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                   Platform Bridges (PB)                         │       │
│  │                                                                  │       │
│  │  - VFS Implementations (Unix, Windows, Memory)                  │       │
│  │  - File Locking                                                 │       │
│  │  - Memory Management (mmap)                                     │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                      │                                     │
│                                      ▼                                     │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                  System Framework (SF)                           │       │
│  │                                                                  │       │
│  │  - VFS Interface                                                 │       │
│  │  - Logging Infrastructure                                        │       │
│  │  - Error Handling                                               │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Core Subsystems                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────┐    ┌──────────────────────┐                     │
│  │   Query Processing   │───▶│  Query Execution     │                     │
│  │       (QP)           │    │       (QE)           │                     │
│  │                      │    │                      │                     │
│  │  - Tokenizer         │    │  - VM Executor       │                     │
│  │  - Parser            │    │  - Operator Engine   │                     │
│  │  - Planner           │    │  - Result Set       │                     │
│  │  - Optimizer         │    │                      │                     │
│  └──────────────────────┘    └──────────────────────┘                     │
│              │                                  │                           │
│              ▼                                  ▼                           │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                     Transaction Monitor (TM)                    │       │
│  │                                                                  │       │
│  │  - ACID Transaction Management                                   │       │
│  │  - Concurrency Control (Lock Manager)                            │       │
│  │  - Write-Ahead Log (WAL)                                        │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│              │                                                          │
│              ▼                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                      Data Storage (DS)                           │       │
│  │                                                                  │       │
│  │  - B-Tree Storage Engine                                         │       │
│  │  - Page Cache / Buffer Pool                                      │       │
│  │  - File Manager                                                   │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                      │                                     │
│                                      ▼                                     │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                   Platform Bridges (PB)                         │       │
│  │                                                                  │       │
│  │  - OS File Operations Abstraction                               │       │
│  │  - File Locking                                                 │       │
│  │  - Memory Management (mmap)                                     │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                      │                                     │
│                                      ▼                                     │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                  System Framework (SF)                           │       │
│  │                                                                  │       │
│  │  - Logging Infrastructure                                        │       │
│  │  - Error Handling                                               │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2. Subsystem Detailed Design

### 2.1 Platform Bridges (PB)

**Purpose**: Abstract OS-level operations for portability and testability

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `file.go` | File open, read, write, sync, lock operations |
| `mmap.go` | Memory-mapped file I/O (optional, for performance) |
| `memory.go` | Memory allocation and management |
| `os_unix.go` | Unix-specific implementations |
| `os_windows.go` | Windows-specific implementations |

**Interface Design**:
```go
type File interface {
    Open(path string, flag int) (File, error)
    ReadAt(p []byte, off int64) (n int, err error)
    WriteAt(p []byte, off int64) (n int, err error)
    Sync() error
    Close() error
    Lock(lockType LockType) error
    Unlock() error
    Size() (int64, error)
}
```

**Key Design Decisions**:
- Use OS-native file locking for database locking
- Support custom page sizes (power of 2: 512-65536 bytes)

---

### 2.2 System Framework (SF)

**Purpose**: Core infrastructure and utilities for the database engine

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `log.go` | Level-based logging (Debug, Info, Warn, Error, Fatal) |

**Interface Design**:
```go
type Level int

const (
    LevelDebug Level = iota
    LevelInfo
    LevelWarn
    LevelError
    LevelFatal
)

func Debug(format string, args ...interface{})
func Info(format string, args ...interface{})
func Warn(format string, args ...interface{})
func Error(format string, args ...interface{})
func Fatal(format string, args ...interface{})
func SetLevel(level Level)
```

**Key Design Decisions**:
- Thread-safe implementation using mutex
- Configurable log levels for different environments

---

### 2.3 Data Storage (DS)

**Purpose**: Persistent data management using B-Tree structure

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `page.go` | Page structure definitions and SQLite header |
| `manager.go` | Page I/O coordination and allocation |
| `btree.go` | B-Tree implementation for tables/indexes |
| `cache.go` | Page cache / buffer pool |
| `table.go` | Table-specific B-Tree operations (TODO) |
| `index.go` | Index management (TODO) |

**Database File Format** (SQLite-compatible):

```
┌─────────────────────────────────────┐
│     Database Header (100 bytes)     │
├─────────────────────────────────────┤
│  Magic Header String "SQLite..."   │
│  Page Size (2 bytes, big-endian)   │
│  Write Format Version              │
│  Read Format Version               │
│  Reserved Space                   │
│  Max Embedded Payload Fraction    │
│  Min Embedded Payload Fraction    │
│  Leaf Payload Fraction            │
│  File Change Counter              │
│  Database Size (in pages)         │
│  First Freelist Trunk Page        │
│  Total Freelist Pages            │
│  Schema Cookie                   │
│  Schema Format Number             │
│  Default Page Cache Size          │
│  Largest B-Tree Root Page        │
│  Text Encoding (1=UTF-8)          │
│  User Version                     │
│  Incremental Vacuum Mode         │
│  Application ID                   │
│  Version Valid For                │
│  SQLite Version                   │
└─────────────────────────────────────┘
```

**Page Types**:

| Type | Code | Description |
|------|------|-------------|
| Interior Index | 0x02 | B-Tree interior node for index |
| Interior Table | 0x05 | B-Tree interior node for table |
| Leaf Index | 0x0a | B-Tree leaf node for index |
| Leaf Table | 0x0d | B-Tree leaf node for table |
| Lock Byte | 0xff | Locking page |
| Freelist | 0xfe | Freelist page |
| Pointer Map | 0xfd | Pointer map page |

**B-Tree Implementation**:

```go
type BTree struct {
    file     *os.File
    cache    *PageCache
    rootPage uint32
    isTable  bool  // true=table, false=index
}

type Page struct {
    Type     PageType
    Size     uint16
    FreeOffset uint16
    Cells    []Cell
    // ... page-specific data
}

type Cell struct {
    Key      []byte
    Payload  []byte
    Overflow uint32  // overflow page number
}
```

**Key Design Decisions**:
- Use copy-on-write B-Tree for MVCC support
- Implement page-level locking with latch crabbing
- Support both INTKEY (table) and INDEXKEY (index) modes
- Handle overflow pages for large values
- Maintain free list for space reuse

---

### 2.4 Query Processing (QP)

**Purpose**: Parse and plan SQL queries

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `tokenizer.go` | Lexical analysis of SQL |
| `parser.go` | SQL syntax parsing and AST building |
| `ast.go` | Abstract Syntax Tree node definitions |
| `planner.go` | Query planning (TODO) |
| `resolver.go` | Schema resolution (TODO) |

**SQL Parsing Architecture**:

```
SQL Text
    │
    ▼
┌─────────────┐
│  Tokenizer  │ ─── Token stream
└─────────────┘
    │
    ▼
┌─────────────┐
│   Parser    │ ─── AST (Abstract Syntax Tree)
└─────────────┘
    │
    ▼
┌─────────────┐
│  Resolver   │ ─── Resolved AST with types
└─────────────┘
    │
    ▼
┌─────────────┐
│   Planner   │ ─── Execution Plan
└─────────────┘
```

**Supported SQL Features** (Phase 1-2):

| Category | Features |
|----------|----------|
| DDL | CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX |
| DML | INSERT, SELECT, UPDATE, DELETE |
| Expressions | Literals, column refs, operators, functions |
| Clauses | WHERE, ORDER BY, LIMIT, OFFSET |
| Aggregates | COUNT, SUM, AVG, MIN, MAX |

**AST Node Types**:
```go
type Node interface {
    Pos() token.Pos
    End() token.Pos
}

type SelectStmt struct {
    Columns     []Expr
    From        *TableRef
    Where       Expr
    OrderBy     []SortSpec
    Limit       Expr
    Offset      Expr
}

type InsertStmt struct {
    Table       *TableRef
    Columns     []string
    Values      [][]Expr
}

type UpdateStmt struct {
    Table       *TableRef
    Set         []SetClause
    Where       Expr
}

type DeleteStmt struct {
    Table       *TableRef
    Where       Expr
}
```

**Key Design Decisions**:
- Use goyacc for parser generation
- Implement recursive descent tokenizer for speed
- Support SQLite's flexible typing (manifest typing)
- Build query plans as operator trees

---

### 2.5 Query Execution (QE)

**Purpose**: Execute query plans and produce results

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `vm.go` | Virtual machine for bytecode execution |
| `engine.go` | Query execution engine |
| `operators.go` | Physical operators (scan, filter, join, etc.) |
| `record.go` | Row/tuple representation |
| `expr.go` | Expression evaluator |

**Virtual Machine Architecture**:

```
Execution Plan (Operator Tree)
    │
    ▼
┌────────────────────────────────────┐
│         VM / Executor              │
├────────────────────────────────────┤
│  - Instruction dispatch            │
│  - Register management             │
│  - Cursor management              │
│  - Aggregate handling             │
└────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────┐
│          Operators                 │
├────────────────────────────────────┤
│  - TableScan                       │
│  - IndexScan                       │
│  - Filter                          │
│  - Project                         │
│  - Sort                            │
│  - Aggregate                       │
│  - Limit                           │
└────────────────────────────────────┘
    │
    ▼
Result Set
```

**VM Opcodes** (SQLite-compatible subset):

| Opcode | Description |
|--------|-------------|
| OpenRead | Open cursor for reading |
| OpenWrite | Open cursor for writing |
| Next | Advance cursor |
| Column | Get column value from cursor |
| Eq | Equality test |
| Lt, Le, Gt, Ge | Comparison operators |
| Add, Sub, Mul, Div | Arithmetic |
| Concat | String concatenation |
| Function | Call function |
| ResultRow | Return result row |
| Goto | Jump to instruction |

**Execution Example**:
```go
// SELECT * FROM users WHERE age > 18 ORDER BY name LIMIT 10

type ExecutionPlan struct {
    Limit: &LimitOp{
        Count: 10,
        Input: &SortOp{
            KeyCols: []int{1}, // name column
            Input: &FilterOp{
                Predicate: &ComparisonOp{
                    Op: GT,
                    Left:  &ColumnRef{Index: 2}, // age
                    Right: &Literal{Value: 18},
                },
                Input: &TableScanOp{
                    Table: "users",
                },
            },
        },
    },
}
```

**Key Design Decisions**:
- Use volcano-style iterator model
- Support both pull and push execution
- Implement materialized and streaming execution
- Use register-based VM for efficiency

---


### 2.6 Code Generator (CG)

**Purpose**: Compile AST to bytecode for VM execution

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `compiler.go` | Main compiler and SELECT statement compilation |
| `expr.go` | Expression compilation (literals, binary ops, functions) |
| `dml.go` | DML statement compilation (INSERT, UPDATE, DELETE) |
| `aggregate.go` | Aggregate function compilation (COUNT, SUM, AVG, etc.) |
| `optimizer.go` | Bytecode optimization passes (future) |

**Compilation Pipeline**:

```
QP.AST (SelectStmt, InsertStmt, etc.)
    │
    ▼
┌────────────────────────────────────┐
│       CG.Compiler                  │
├────────────────────────────────────┤
│  CompileSelect()                   │
│  CompileInsert()                   │
│  CompileUpdate()                   │
│  CompileDelete()                   │
│  CompileAggregate()                │
│                                    │
│  compileExpr()      ───┐           │
│  compileBinaryExpr()   │           │
│  compileFuncCall()     │ Internal  │
│  compileColumnRef()    │ helpers   │
│  compileLiteral()  ────┘           │
└────────────────────────────────────┘
    │
    ▼
VM.Program (Bytecode Instructions)
```

**Interface Design**:
```go
type Compiler struct {
    vmCompiler *VM.Compiler
}

func NewCompiler() *Compiler
func (c *Compiler) CompileSelect(stmt *QP.SelectStmt) *VM.Program
func (c *Compiler) CompileInsert(stmt *QP.InsertStmt) *VM.Program
func (c *Compiler) CompileUpdate(stmt *QP.UpdateStmt) *VM.Program
func (c *Compiler) CompileDelete(stmt *QP.DeleteStmt) *VM.Program
func Compile(sql string) (*VM.Program, error)
func CompileWithSchema(sql string, tableColumns []string) (*VM.Program, error)
```

**Key Design Decisions**:
- Separate compilation from execution (CG vs VM)
- Support table schema for SELECT * expansion
- Generate register-based bytecode
- Enable future optimization passes

---

### 2.7 Virtual Machine (VM)

**Purpose**: Execute bytecode programs produced by CG

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `engine.go` | VM engine core and execution context |
| `exec.go` | Instruction dispatcher and opcode handlers |
| `opcodes.go` | Opcode definitions (100+ opcodes) |
| `program.go` | Bytecode program representation |
| `registers.go` | Register allocator |
| `cursor.go` | Cursor management for table/index access |

**VM Architecture**:

```
VM.Program (Bytecode)
    │
    ▼
┌────────────────────────────────────┐
│         VM.Engine                  │
├────────────────────────────────────┤
│  Registers: []interface{}          │
│  Cursors:   *CursorArray          │
│  PC:        int                    │
│  Results:   [][]interface{}        │
└────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────┐
│     Instruction Dispatcher         │
├────────────────────────────────────┤
│  switch inst.Op {                  │
│    case OpOpenRead: ...            │
│    case OpColumn: ...              │
│    case OpEq: ...                  │
│    case OpResultRow: ...           │
│    case OpNext: ...                │
│    case OpHalt: ...                │
│  }                                 │
└────────────────────────────────────┘
    │
    ▼
Results / Side Effects
```

**Key Opcodes**:

| Category | Opcodes |
|----------|---------|
| **Cursor Ops** | OpOpenRead, OpOpenWrite, OpRewind, OpNext, OpClose |
| **Data Ops** | OpColumn, OpInsert, OpUpdate, OpDelete |
| **Control Flow** | OpGoto, OpIf, OpIfNot, OpHalt |
| **Comparison** | OpEq, OpNe, OpLt, OpLe, OpGt, OpGe |
| **Arithmetic** | OpAdd, OpSubtract, OpMultiply, OpDivide, OpRemainder |
| **String** | OpConcat, OpSubstr, OpUpper, OpLower, OpTrim |
| **Aggregate** | OpCount, OpSum, OpAvg, OpMin, OpMax |
| **Result** | OpResultRow, OpLoadConst, OpCopy |

**Instruction Format**:
```go
type Instruction struct {
    Op OpCode         // Opcode
    P1 int32          // First operand (often register or cursor ID)
    P2 int32          // Second operand
    P3 interface{}    // Third operand (string or other type)
    P4 interface{}    // Fourth operand (destination register or jump target)
}
```

**Execution Example**:
```
// SELECT id, name FROM users WHERE age > 18

OpInit                      // Initialize
OpOpenRead 0, "users"       // Open cursor 0 for users table
OpRewind 0                  // Position cursor at start
OpColumn 0, 2, r0           // Load age into r0
OpLoadConst r1, 18          // Load 18 into r1
OpGt r0, r1, r2             // Compare: r2 = (age > 18)
OpLoadConst r3, 0           // Load 0 into r3
OpEq r2, r3, skipRow        // If r2 == 0, jump to skipRow
OpColumn 0, 0, r4           // Load id into r4
OpColumn 0, 1, r5           // Load name into r5
OpResultRow [r4, r5]        // Emit result row
skipRow:
OpNext 0, loop              // Advance cursor, loop if more rows
OpHalt                      // Done
```

**Key Design Decisions**:
- Register-based VM (vs stack-based) for performance
- Cursor abstraction for table/index access
- SQLite-compatible opcode set
- Support for WHERE clause jump targets vs register destinations

---


### 2.8 Transaction Monitor (TM)

**Purpose**: ACID transaction management and concurrency control

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `transaction.go` | Transaction lifecycle management |
| `lock.go` | Lock manager (db, table, row locks) |
| `wal.go` | Write-Ahead Log implementation |
| `journal.go` | Rollback journal |
| `mvcc.go` | Multi-version concurrency control |
| `checkpoint.go` | WAL checkpoint management |

**Transaction States**:

```
START ─────▶ ACTIVE ─────▶ COMMITTED
  │              │              │
  │              │              │
  ▼              ▼              ▼
ROLLBACK    READ_ONLY      (end)
```

**Concurrency Model** (SQLite-compatible):

| Lock Type | Description | Compatible Locks |
|-----------|-------------|------------------|
| UNLOCKED | No lock | all |
| SHARED | Read lock | SHARED, RESERVED |
| RESERVED | Read with pending write | SHARED |
| EXCLUSIVE | Write lock | none |

**WAL Mode Structure**:

```
┌─────────────────────────────────────────────────────┐
│                    WAL File                          │
├─────────────────────────────────────────────────────┤
│  WAL Header (32 bytes)                              │
│  - Magic                                            │
│  - Page size                                        │
│  - Sequence number                                  │
│  - Checkpoint salt                                 │
│  - Salt sum                                        │
│  - Checksum 1, 2                                   │
├─────────────────────────────────────────────────────┤
│  Frame Header (24 bytes per frame)                 │
│  - Page number                                      │
│  - Commit size                                      │
│  - Checksum 1, 2                                   │
├─────────────────────────────────────────────────────┤
│  Page Data (page size bytes per frame)             │
└─────────────────────────────────────────────────────┘
```

**Key Design Decisions**:
- Support both rollback journal and WAL modes
- Use Checkpoint-After-Commit (default) for simplicity
- Implement deferred locks for better concurrency
- Support READ UNCOMMITTED, READ COMMITTED, SERIALIZABLE

---

## 3. Project Structure

```
sqlvibe/
├── cmd/
│   └── sqlvibe/          # CLI application
│       └── main.go
├── pkg/
│   └── sqlvibe/          # Public API
│       └── version.go
├── internal/
│   ├── pb/                # Platform Bridges
│   │   ├── file.go
│   │   └── file_test.go
│   ├── ds/                # Data Storage
│   │   ├── page.go
│   │   ├── page_test.go
│   │   ├── manager.go
│   │   ├── manager_test.go
│   │   ├── btree.go
│   │   ├── btree_test.go
│   │   └── cache.go
│   ├── qp/                # Query Processing
│   │   ├── tokenizer.go
│   │   ├── tokenizer_test.go
│   │   ├── parser.go
│   │   ├── ast.go
│   │   └── planner.go
│   ├── qe/                # Query Execution (TODO)
│   ├── tm/                # Transaction Monitor (TODO)
│   └── sf/                 # System Framework
│       └── log.go
├── test/
│   └── sqllogictest/     # SQLite logic tests
├── docs/
│   ├── ARCHITECTURE.md
│   └── PHASES.md
├── agents.md              # OpenCode agent guidance
├── go.mod
└── .gitignore
```

## 4. Subsystem Details

### 4.1 System Framework (SF)

**Purpose**: Core infrastructure and utilities

**Components**:

| Component | Responsibility |
|-----------|----------------|
| `log.go` | Level-based logging infrastructure |
| | |
│   └── PHASES.md
├── go.mod
├── go.sum
└── Makefile
```

---

## 4. Key Interfaces

### Database Connection

```go
type Database interface {
    // Lifecycle
    Open(path string) error
    Close() error
    
    // Transaction
    Begin() (*Transaction, error)
    BeginReadOnly() (*Transaction, error)
    
    // Execution
    Exec(sql string, args ...interface{}) (Result, error)
    Query(sql string, args ...interface{}) (Rows, error)
    Prepare(sql string) (Statement, error)
}

type Transaction interface {
    Commit() error
    Rollback() error
    
    Exec(sql string, args ...interface{}) (Result, error)
    Query(sql string, args ...interface{}) (Rows, error)
}

type Statement interface {
    Bind(...interface{}) error
    Execute() (Result, error)
    Query() (Rows, error)
    Close() error
}

type Rows interface {
    Next() bool
    Scan(...interface{}) error
    ColumnTypes() ([]ColumnType, error)
    Close() error
}
```

### Storage Engine

```go
type StorageEngine interface {
    // Page operations
    ReadPage(pageNum uint32) (*Page, error)
    WritePage(page *Page) error
    AllocatePage() (uint32, error)
    FreePage(pageNum uint32) error
    
    // B-Tree operations
    OpenBTree(rootPage uint32, isTable bool) BTree
    CreateBTree(isTable bool) (BTree, uint32, error)
    
    // Transaction
    Begin() error
    Commit() error
    Rollback() error
}

type BTree interface {
    Search(key []byte) ([]byte, error)
    Insert(key, value []byte) error
    Delete(key []byte) error
    First() ([]byte, []byte, error)
    Next(cursor *Cursor) ([]byte, []byte, error)
    Close() error
}
```

---

## 5. Testing Strategy

### Blackbox Testing with SQLite

The verification strategy compares execution results with real SQLite:

1. **SQL Logic Tests**: Use sqlite's sqllogictest format
2. **Result Comparison**: Run same SQL on both engines, compare outputs
3. **Edge Cases**: Focus on type handling, NULLs, boundary conditions

```go
// Testing approach
func TestSQLiteCompatibility(t *testing.T) {
    testCases := []struct {
        name string
        sql  string
    }{
        {"simple_select", "SELECT * FROM t1"},
        {"where_clause", "SELECT * FROM t1 WHERE a > 5"},
        {"join", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"},
        // ... more cases
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            // Execute on goLite
            goLiteResult := executeGoLite(tc.sql)
            
            // Execute on SQLite
            sqliteResult := executeSQLite(tc.sql)
            
            // Compare results
            assert.Equal(t, sqliteResult, goLiteResult)
        })
    }
}
```

### Test Coverage Goals

| Phase | Coverage Target | Focus Areas |
|-------|-----------------|-------------|
| 1 | Basic ops | DDL, simple SELECT |
| 2 | Core features | DML, WHERE, ORDER BY |
| 3 | Transactions | ACID, concurrent access |
| 4 | Advanced | JOINs, subqueries |

---

## 6. Design Rationale

### Why This Architecture?

1. **Modularity**: Each subsystem can be developed and tested independently
2. **SQLite Compatibility**: Following SQLite's well-documented architecture
3. **Go Idioms**: Using Go's strengths (goroutines for concurrency, interfaces)
4. **Testability**: Clear boundaries enable unit and integration testing

### Trade-offs

| Decision | Pros | Cons |
|----------|------|------|
| SQLite-compatible file format | Tools compatibility | Complex implementation |
| B-Tree (not LSM) | Range queries, proven | More complex than LSM |
| WAL (not rollback journal) | Better concurrency | More complex checkpoint |
| Register-based VM | Fast execution | More complex than stack |

### Future Enhancements

- FTS (Full-Text Search)
- JSON support
- Window functions
- R-Tree for spatial queries
- Foreign key constraints
