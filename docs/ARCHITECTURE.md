# SQLite-Compatible Database Engine Architecture

## Project Overview

**Project Name**: sqlvibe - SQLite-Compatible Database Engine in Go  
**Language**: Golang  
**Version**: v0.6.x (latest release: v0.5.2 — see [HISTORY.md](HISTORY.md))  
**Goal**: Achieve SQLite features and compatibility with blackbox-level correctness verification  
**SQL Compatibility**: SQL:1999 — 56/56 test suites passing (100%)

---

## 1. System Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           User Interface Layer                                │
├──────────────────────────────────────────────────────────────────────────────┤
│   pkg/sqlvibe (Public API / Library Binding)   │   cmd/sqlvibe (CLI Tool)    │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           SQL Processing Pipeline                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────┐   │
│  │  Query Processing│───▶│  Code Generator  │───▶│   Virtual Machine    │   │
│  │      (QP)        │    │      (CG)        │    │        (VM)          │   │
│  │                  │    │                  │    │                      │   │
│  │  - Tokenizer     │    │  - SELECT/DML    │    │  - Bytecode Executor │   │
│  │  - Parser (AST)  │    │    Compiler      │    │  - Register Manager  │   │
│  │  - JOIN / SET-OP │    │  - Aggregate     │    │  - Cursor Manager    │   │
│  │  - CTE / Window  │    │    Compiler      │    │  - SET Operations    │   │
│  │  - Subquery      │    │  - Expression    │    │  - Subquery Exec     │   │
│  └──────────────────┘    │    Compiler      │    │  - Window Functions  │   │
│                          │  - Optimizer     │    └──────────────────────┘   │
│                          └──────────────────┘                               │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Schema & Execution Services                           │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────────────────────┐    ┌────────────────────────────────────────┐  │
│  │  Information Schema (IS) │    │        Query Execution (QE)            │  │
│  │                          │    │                                        │  │
│  │  - Registry              │    │  - Expression evaluator                │  │
│  │  - TABLES view           │    │  - Operator engine                     │  │
│  │  - COLUMNS view          │    │  - String functions                    │  │
│  │  - CONSTRAINTS view      │    │  - Scalar sub-expressions              │  │
│  │  - VIEWS view            │    └────────────────────────────────────────┘  │
│  │  - Schema extractor      │                                                 │
│  └──────────────────────────┘                                                 │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Storage & Transaction Layer                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    Transaction Monitor (TM)                            │  │
│  │  - ACID transaction lifecycle   - Lock Manager (SHARED/RESERVED/EXCL) │  │
│  │  - Write-Ahead Log (WAL)        - Rollback support                     │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                               │                                               │
│                               ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                      Data Storage (DS)                                 │  │
│  │  - SQLite-compatible B-Tree     - Page cache / buffer pool             │  │
│  │  - Varint & record encoding     - Overflow page chains                 │  │
│  │  - Page balancing (split/merge) - Freelist management                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                               │                                               │
│                               ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    Platform Bridges (PB)                               │  │
│  │  - VFS abstraction layer        - Unix VFS implementation              │  │
│  │  - Memory VFS (:memory:)        - File locking                         │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                               │                                               │
│                               ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    System Framework (SF)                               │  │
│  │  - VFS interface definition     - Level-based logging                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Project File Structure

```
sqlvibe/
├── cmd/
│   └── sqlvibe/              # CLI application
│       └── main.go
├── pkg/
│   └── sqlvibe/              # Public API (library binding)
│       ├── database.go       # Database struct, Open/Close, Exec/Query
│       ├── vm_exec.go        # VM-based execution path (ExecVM)
│       ├── vm_context.go     # VmContext bridge to Database
│       ├── setops.go         # UNION/INTERSECT/EXCEPT post-processing
│       ├── window.go         # Window function (OVER) post-processing
│       ├── explain.go        # EXPLAIN support
│       ├── pragma.go         # PRAGMA statement handlers
│       └── version.go        # Version constant
├── internal/
│   ├── SF/                   # System Framework
│   │   ├── log.go            # Level-based structured logging
│   │   └── vfs/vfs.go        # VFS interface definition
│   ├── PB/                   # Platform Bridges
│   │   ├── file.go           # OS file operations wrapper
│   │   ├── vfs_unix.go       # Unix VFS implementation
│   │   ├── vfs_memory.go     # In-memory VFS implementation (:memory:)
│   │   └── vfs/vfs.go        # PB VFS interface
│   ├── DS/                   # Data Storage
│   │   ├── page.go           # Page struct, header, constants
│   │   ├── manager.go        # PageManager: allocate/read/write pages
│   │   ├── btree.go          # B-Tree: search, insert, delete, iterate
│   │   ├── balance.go        # Page balancing: split, merge, redistribute
│   │   ├── cache.go          # Page cache / buffer pool
│   │   ├── cell.go           # Cell encoding/decoding (all 4 page types)
│   │   ├── encoding.go       # Varint and SQLite record encoding
│   │   ├── overflow.go       # Overflow page chain management
│   │   └── freelist.go       # Freelist trunk/leaf management
│   ├── QP/                   # Query Processing
│   │   ├── tokenizer.go      # SQL lexer (tokenizer)
│   │   └── parser.go         # Recursive-descent parser, AST definitions
│   ├── CG/                   # Code Generator
│   │   ├── compiler.go       # Main compiler: SELECT, FROM, JOIN, SET-OPs, CTE
│   │   ├── expr.go           # Expression compilation (binary, unary, functions)
│   │   └── optimizer.go      # Bytecode optimizer (index usage, filters)
│   ├── VM/                   # Virtual Machine
│   │   ├── engine.go         # VM struct, error types, VmContext interface
│   │   ├── exec.go           # Instruction dispatcher and opcode handlers
│   │   ├── opcodes.go        # Opcode definitions (~200 opcodes)
│   │   ├── program.go        # Program (bytecode container), fixup support
│   │   ├── registers.go      # RegisterAllocator
│   │   ├── cursor.go         # CursorArray, cursor state (MaxCursors=256)
│   │   ├── compiler.go       # VM-level compiler wrapper
│   │   ├── instruction.go    # Instruction struct
│   │   ├── query_engine.go   # QueryEngine: runs programs with schema
│   │   ├── query_expr.go     # Expression evaluation at VM level
│   │   └── query_operators.go# Operator evaluation helpers
│   ├── QE/                   # Query Execution (expression engine)
│   │   ├── engine.go         # Expression engine, schema registration
│   │   ├── expr.go           # Low-level expression evaluation
│   │   └── operators.go      # Operator implementations
│   ├── TM/                   # Transaction Monitor
│   │   ├── transaction.go    # Transaction lifecycle (Begin/Commit/Rollback)
│   │   ├── lock.go           # Lock manager (SHARED/RESERVED/EXCLUSIVE)
│   │   └── wal.go            # Write-Ahead Log
│   ├── IS/                   # Information Schema
│   │   ├── registry.go       # Central schema registry
│   │   ├── information_schema.go # Type definitions and constants
│   │   ├── schema_extractor.go   # Extract schema from SQL DDL
│   │   ├── schema_parser.go      # Schema SQL parsing helpers
│   │   ├── tables_view.go        # INFORMATION_SCHEMA.TABLES
│   │   ├── columns_view.go       # INFORMATION_SCHEMA.COLUMNS
│   │   ├── constraints_view.go   # INFORMATION_SCHEMA.TABLE_CONSTRAINTS
│   │   ├── referential_view.go   # INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
│   │   └── views_view.go         # INFORMATION_SCHEMA.VIEWS
│   ├── TS/                   # Test Suites
│   │   ├── SQL1999/          # SQL:1999 compatibility tests (56 suites)
│   │   ├── Benchmark/        # Performance benchmarks
│   │   └── PlainFuzzer/      # Go native fuzzing harness
│   └── util/
│       ├── assert.go         # Defensive-programming assertion helpers
│       └── assert_test.go
├── docs/
│   ├── ARCHITECTURE.md       # This document
│   ├── HISTORY.md            # Release history
│   ├── SQL1999.md            # SQL:1999 test suite status
│   └── plan-v0.6.0.md        # Development plan
├── go.mod
├── go.sum
├── Makefile
└── AGENTS.md                 # OpenCode agent guidance
```

---

## 3. Subsystem Details

### 3.1 System Framework (SF)

**Purpose**: Core infrastructure shared by all subsystems

**Components**:

| File | Responsibility |
|------|----------------|
| `log.go` | Level-based logging: Debug, Info, Warn, Error, Fatal |
| `vfs/vfs.go` | VFS interface definition (implemented by PB) |

**Key Design Decisions**:
- Thread-safe logging via mutex
- Configurable log levels per environment
- VFS interface decouples storage from OS

---

### 3.2 Platform Bridges (PB)

**Purpose**: Portable OS-level I/O abstraction

**Components**:

| File | Responsibility |
|------|----------------|
| `file.go` | File open, read (ReadAt), write (WriteAt), sync, size, lock |
| `vfs_unix.go` | Unix implementation of the VFS interface |
| `vfs_memory.go` | In-memory VFS for `:memory:` databases |
| `vfs/vfs.go` | PB-level VFS interface |

**Key Design Decisions**:
- VFS plug-in model: swap Unix ↔ Memory without changing upper layers
- OS-native advisory file locking (POSIX `fcntl`)
- ReadAt/WriteAt semantics (no seek state, safe for concurrent use)

---

### 3.3 Data Storage (DS)

**Purpose**: SQLite-compatible persistent storage via B-Tree

**Components**:

| File | Responsibility |
|------|----------------|
| `page.go` | Page struct, 100-byte SQLite header, page-type constants |
| `manager.go` | PageManager: page allocation, read, write, flush |
| `btree.go` | B-Tree: search, insert, delete, full-scan cursor |
| `balance.go` | Page split, merge, and cell redistribution |
| `cache.go` | LRU page cache / buffer pool |
| `cell.go` | Cell encode/decode for all 4 page types |
| `encoding.go` | Varint (1-9 byte) and SQLite record-format encoding |
| `overflow.go` | Overflow page chain: read, write, free |
| `freelist.go` | Freelist trunk/leaf management: allocate/free pages |

**Database File Format** (SQLite-compatible):

```
┌────────────────────────────────────┐
│     Database Header (100 bytes)    │
├────────────────────────────────────┤
│  Magic: "SQLite format 3\x00"      │
│  Page Size (big-endian uint16)     │
│  Write/Read Format Version         │
│  Reserved Bytes per Page           │
│  Max/Min Embedded Payload Fraction │
│  File Change Counter               │
│  Database Size in Pages            │
│  First Freelist Trunk Page         │
│  Total Freelist Pages              │
│  Schema Cookie                     │
│  Schema Format Number              │
│  Default Page Cache Size           │
│  Text Encoding (1=UTF-8)           │
│  Application ID                    │
│  SQLite Version Number             │
└────────────────────────────────────┘
```

**Page Types**:

| Type | Code | Description |
|------|------|-------------|
| Interior Index | 0x02 | B-Tree interior node for index |
| Interior Table | 0x05 | B-Tree interior node for table |
| Leaf Index | 0x0a | B-Tree leaf node for index |
| Leaf Table | 0x0d | B-Tree leaf node for table |

**Page Header Layouts**:

*Leaf Page Header (8 bytes)*:
```
Offset  Size  Description
------  ----  -----------
0       1     Page type (0x0d / 0x0a)
1       2     First freeblock offset (0 if none)
3       2     Number of cells on page
5       2     Cell content area start (0 means 65536)
7       1     Fragmented free bytes
```

*Interior Page Header (12 bytes)*:
```
Offset  Size  Description
------  ----  -----------
0       1     Page type (0x05 / 0x02)
1-7           Same as leaf
8       4     Right-most child page number
```

**Cell Formats**:

| Cell Type | Format |
|-----------|--------|
| Table Leaf | payload_size(varint) + rowid(varint) + payload + [overflow_page] |
| Table Interior | left_child(4 bytes) + rowid(varint) |
| Index Leaf | payload_size(varint) + payload + [overflow_page] |
| Index Interior | left_child(4 bytes) + payload_size(varint) + payload + [overflow_page] |

**Varint Encoding**:
- 1–9 bytes per 64-bit integer
- Bits 0-6 of each byte are data; bit 7 signals continuation
- Maximum 9 bytes (final byte uses all 8 bits)

**Record Format**:
```
header_size(varint)  serial_type_1(varint) ... serial_type_N(varint)
data_1 ... data_N
```

**Serial Type Codes**:
- 0: NULL, 1–6: integers (1–8 bytes), 7: IEEE 754 float64
- 8, 9: integer constants 0, 1 (schema v4+)
- N≥12 even: BLOB of (N-12)/2 bytes
- N≥13 odd: TEXT of (N-13)/2 bytes

**Page Size Constraints**: 512–65536 bytes, must be a power of 2.

---

### 3.4 Transaction Monitor (TM)

**Purpose**: ACID transaction lifecycle and concurrency control

**Components**:

| File | Responsibility |
|------|----------------|
| `transaction.go` | Begin, Commit, Rollback; transaction state machine |
| `lock.go` | Lock manager: SHARED / RESERVED / EXCLUSIVE levels |
| `wal.go` | Write-Ahead Log (WAL) frame management |

**Transaction States**:
```
START ──▶ ACTIVE ──▶ COMMITTED
            │
            ▼
         ROLLED_BACK
```

**Concurrency Model** (SQLite-compatible):

| Lock Type | Description | Compatible With |
|-----------|-------------|-----------------|
| UNLOCKED | No lock held | all |
| SHARED | Read lock | SHARED, RESERVED |
| RESERVED | Pending write intent | SHARED |
| EXCLUSIVE | Write lock | none |

**WAL File Structure**:
```
WAL Header (32 bytes): magic, page size, sequence, salt, checksum
Frame Header (24 bytes each): page number, commit size, salt, checksum
Frame Data (page_size bytes each): page content
```

---

### 3.5 Query Processing (QP)

**Purpose**: SQL lexing, parsing, and AST construction

**Components**:

| File | Responsibility |
|------|----------------|
| `tokenizer.go` | Lexical analysis; produces `[]Token` |
| `parser.go` | Recursive-descent parser; produces AST nodes |

**SQL Processing Pipeline**:
```
SQL Text ──▶ Tokenizer ──▶ Token Stream ──▶ Parser ──▶ AST
```

**Key AST Node Types**:
```go
SelectStmt { Distinct, Columns, From, Where, GroupBy, Having,
             OrderBy, Limit, Offset, SetOp, CTEs }
InsertStmt { Table, Columns, Values, OnConflict }
UpdateStmt { Table, Set, Where }
DeleteStmt { Table, Where }
CreateTableStmt { Name, Columns, Constraints }
CreateIndexStmt { Name, Table, Columns, Unique }
AlterTableStmt  { Table, Action, ... }
```

**Supported SQL Features** (fully implemented):

| Category | Features |
|----------|----------|
| DDL | CREATE TABLE/INDEX/VIEW, DROP TABLE/INDEX/VIEW, ALTER TABLE (ADD/RENAME) |
| DML | INSERT (multi-row, ON CONFLICT), SELECT, UPDATE, DELETE |
| Query | DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET |
| JOIN | INNER JOIN, LEFT JOIN, CROSS JOIN, multiple tables, aliases |
| Subqueries | Scalar, EXISTS, IN/NOT IN, ALL/ANY, correlated subqueries |
| Set Ops | UNION, UNION ALL, INTERSECT, EXCEPT |
| Aggregates | COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT (with ALL/DISTINCT) |
| Window | OVER (PARTITION BY … ORDER BY …), LAG/LEAD, ROW_NUMBER, RANK |
| CTE | WITH … AS (SELECT …) – common table expressions |
| Expressions | Literals, operators, CASE, CAST, BETWEEN, LIKE/GLOB, IS NULL |
| Functions | String: LENGTH, SUBSTR, UPPER, LOWER, TRIM, INSTR, REPLACE, COALESCE |
|  | Math: ABS, ROUND, CEIL, FLOOR, MOD, POW, SQRT, trig functions |
|  | Date/Time: DATE, TIME, DATETIME, STRFTIME, CURRENT_DATE/TIME/TIMESTAMP |
|  | Type: TYPEOF, CAST |
| Transactions | BEGIN, COMMIT, ROLLBACK, SAVEPOINT |
| Constraints | NOT NULL, UNIQUE, PRIMARY KEY, CHECK, DEFAULT |
| PRAGMA | table_info, index_list, database_list |
| Schema | INFORMATION_SCHEMA views (TABLES, COLUMNS, CONSTRAINTS, VIEWS) |

---

### 3.6 Code Generator (CG)

**Purpose**: Compile QP AST nodes into VM bytecode programs

**Components**:

| File | Responsibility |
|------|----------------|
| `compiler.go` | Main compilation entry; SELECT, FROM, JOIN, SET-OPs, CTE |
| `expr.go` | Compile expressions, binary/unary ops, function calls |
| `optimizer.go` | Bytecode optimization: index scans, filter push-down |

**Compilation Pipeline**:
```
QP.AST ──▶ CG.Compiler ──▶ VM.Program (bytecode)
```

**Key Public API**:
```go
func Compile(sql string) (*VM.Program, error)
func CompileWithSchema(sql string, tableColumns []string) (*VM.Program, error)

type Compiler struct { ... }
func (c *Compiler) CompileSelect(stmt *QP.SelectStmt) *VM.Program
func (c *Compiler) CompileInsert(stmt *QP.InsertStmt) *VM.Program
func (c *Compiler) CompileUpdate(stmt *QP.UpdateStmt) *VM.Program
func (c *Compiler) CompileDelete(stmt *QP.DeleteStmt) *VM.Program
```

**Key Design Decisions**:
- Schema-aware: expands `SELECT *` to explicit column list
- JOIN compiled to nested-loop cursor opens
- Set operations compiled using ephemeral tables (OpEphemeral*)
- CTEs compiled as nested subqueries with bound column names
- `optimizer.go` detects index-eligible WHERE clauses → OpSeekGE/OpSeekGT

---

### 3.7 Virtual Machine (VM)

**Purpose**: Execute register-based bytecode programs

**Components**:

| File | Responsibility |
|------|----------------|
| `engine.go` | VM struct, error types, VmContext interface |
| `exec.go` | Instruction dispatcher (~200 opcode handlers) |
| `opcodes.go` | All opcode definitions (OpCode constants + name map) |
| `program.go` | Program container: instructions, fixup slots, metadata |
| `registers.go` | RegisterAllocator: tracks live/free registers |
| `cursor.go` | CursorArray, cursor state; MaxCursors = 256 |
| `compiler.go` | VM-internal compiler helpers (used by CG) |
| `instruction.go` | Instruction struct {Op, P1, P2, P3, P4} |
| `query_engine.go` | QueryEngine: runs a Program with live table data |
| `query_expr.go` | Expression evaluation (used by QE path) |
| `query_operators.go` | Low-level operator helpers |

**VM Architecture**:
```
VM.Program
    │
    ▼
┌───────────────────────────────────────┐
│            VM.VM                       │
├───────────────────────────────────────┤
│  registers []interface{}              │
│  cursors   *CursorArray (max 256)     │
│  pc        int                        │
│  results   [][]interface{}            │
│  ctx       VmContext                  │
└───────────────────────────────────────┘
    │
    ▼ (exec.go dispatch loop)
Opcode Handlers → results / side-effects
```

**Opcode Categories** (~200 total):

| Category | Examples |
|----------|---------|
| Control Flow | OpInit, OpGoto, OpGosub, OpReturn, OpHalt, OpIf, OpIfNot |
| Cursor Ops | OpOpenRead, OpOpenWrite, OpRewind, OpNext, OpPrev, OpSeek*, OpClose |
| Data Access | OpColumn, OpRowid, OpInsert, OpUpdate, OpDelete |
| Comparisons | OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpIs, OpIsNot, OpIsNull, OpNotNull |
| Arithmetic | OpAdd, OpSubtract, OpMultiply, OpDivide, OpRemainder |
| String | OpConcat, OpSubstr, OpLength, OpUpper, OpLower, OpTrim, OpLike, OpGlob |
| Math | OpAbs, OpRound, OpCeil, OpFloor, OpPow, OpSqrt, trig ops |
| Aggregate | OpAggStep, OpAggFinal, OpSum, OpAvg, OpMin, OpMax, OpCount |
| Set Ops | OpEphemeralCreate, OpEphemeralInsert, OpEphemeralFind |
| Subquery | OpScalarSubquery, OpExistsSubquery, OpInSubquery |
| Type/Misc | OpCast, OpTypeof, OpRandom, OpCallScalar, OpNoop |

**Instruction Format**:
```go
type Instruction struct {
    Op OpCode      // operation
    P1 int32       // first operand  (register / cursor ID)
    P2 int32       // second operand (jump target / register)
    P3 interface{} // third operand  (string constant, etc.)
    P4 interface{} // fourth operand (destination register / metadata)
}
```

**VmContext Interface** (bridge to `pkg/sqlvibe`):
```go
type VmContext interface {
    GetTableData(tableName string) ([]map[string]interface{}, error)
    GetTableColumns(tableName string) ([]string, error)
    InsertRow(tableName string, row map[string]interface{}) error
    UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error
    DeleteRow(tableName string, rowIndex int) error
}
```

---

### 3.8 Query Execution (QE)

**Purpose**: Low-level expression and operator evaluation (used alongside VM)

**Components**:

| File | Responsibility |
|------|----------------|
| `engine.go` | QueryEngine struct; schema registration; row evaluation |
| `expr.go` | Expression evaluation over a single row |
| `operators.go` | Operator implementations (comparison, arithmetic, string) |

---

### 3.9 Information Schema (IS)

**Purpose**: SQLite-compatible INFORMATION_SCHEMA views

**Components**:

| File | Responsibility |
|------|----------------|
| `registry.go` | Central in-memory schema registry for all tables/views |
| `information_schema.go` | Type definitions: TableInfo, ColumnInfo, ConstraintInfo |
| `schema_extractor.go` | Parse CREATE TABLE SQL to extract schema metadata |
| `schema_parser.go` | Helpers for schema SQL parsing |
| `tables_view.go` | INFORMATION_SCHEMA.TABLES virtual view |
| `columns_view.go` | INFORMATION_SCHEMA.COLUMNS virtual view |
| `constraints_view.go` | INFORMATION_SCHEMA.TABLE_CONSTRAINTS virtual view |
| `referential_view.go` | INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS virtual view |
| `views_view.go` | INFORMATION_SCHEMA.VIEWS virtual view |

---

### 3.10 Public API (pkg/sqlvibe)

**Purpose**: Library entry point — coordinates all subsystems for end users

**Components**:

| File | Responsibility |
|------|----------------|
| `database.go` | `Database` struct; Open/Close; DDL handling; transaction management |
| `vm_exec.go` | `ExecVM` — SQL → QP → CG → VM execution path |
| `vm_context.go` | `VmContext` implementation bridging Database to VM |
| `setops.go` | Post-processing for UNION/INTERSECT/EXCEPT deduplication |
| `window.go` | Post-processing for window functions (OVER clause) |
| `explain.go` | EXPLAIN statement: prints bytecode program |
| `pragma.go` | PRAGMA statement handlers (table_info, index_list, …) |
| `version.go` | `Version` constant |

**Database Struct** (main entry point):
```go
type Database struct {
    pm             *DS.PageManager
    engine         *VM.QueryEngine
    txMgr          *TM.TransactionManager
    isRegistry     *IS.Registry
    tables         map[string]map[string]string        // schema
    columnOrder    map[string][]string
    columnDefaults map[string]map[string]interface{}
    columnNotNull  map[string]map[string]bool
    data           map[string][]map[string]interface{} // in-memory rows
    indexes        map[string]*IndexInfo
    views          map[string]string
    tableBTrees    map[string]*DS.BTree
}
```

**Execution Flow for a SELECT**:
```
db.Exec(sql)
  │
  ├─▶ QP.NewTokenizer → QP.NewParser → AST
  │
  ├─▶ CG.CompileWithSchema(sql, cols) → VM.Program
  │
  ├─▶ VM.QueryEngine.RunProgram(program, tableData)
  │       └─▶ VM.VM.Execute() (dispatch loop in exec.go)
  │
  └─▶ post-processing: window functions, set-op dedup, ORDER BY + LIMIT
```

---

### 3.11 Test Suites (TS)

**Purpose**: Correctness verification against real SQLite

**Components**:

| Directory | Description |
|-----------|-------------|
| `TS/SQL1999/` | 56 test suites covering SQL:1999 E-series and F-series features |
| `TS/Benchmark/` | Performance benchmarks comparing sqlvibe vs SQLite |
| `TS/PlainFuzzer/` | Go native fuzzing harness generating random SQL |

**SQL:1999 Coverage** (56/56 passing):
- **E-series**: E011–E171 (numeric types, strings, identifiers, JOINs, aggregates, window functions, transactions, …)
- **F-series**: F011–F501 (information schema, joined tables, date/time, CAST, UNION, CASE, LIKE, …)

**Test Strategy**:
- Every test runs the same SQL against both sqlvibe and the real SQLite (via `go-sqlite`)
- Results are compared byte-for-byte (with normalization for implementation-defined ordering)
- Regressions captured in `TestRegression_*` tests named by feature and level

---

## 4. Key Interfaces

### Database Connection (pkg/sqlvibe)

```go
func Open(path string) (*Database, error)
func OpenMemory() (*Database, error)

func (db *Database) Exec(sql string, args ...interface{}) (*Rows, error)
func (db *Database) Query(sql string, args ...interface{}) (*Rows, error)
func (db *Database) Close() error
func (db *Database) Begin() error
func (db *Database) Commit() error
func (db *Database) Rollback() error
```

### Storage Engine (DS)

```go
type PageManager struct { ... }
func NewPageManager(vfsFile PB.VFSFile, pageSize int) *PageManager
func (pm *PageManager) ReadPage(pageNum uint32) (*Page, error)
func (pm *PageManager) WritePage(page *Page) error
func (pm *PageManager) AllocatePage() (uint32, error)
func (pm *PageManager) FreePage(pageNum uint32) error

type BTree struct { ... }
func NewBTree(pm *PageManager, rootPage uint32, isTable bool) *BTree
func (bt *BTree) Search(key []byte) ([]byte, error)
func (bt *BTree) Insert(key, value []byte) error
func (bt *BTree) Delete(key []byte) error
func (bt *BTree) First() (*BTreeCursor, error)
func (bt *BTree) Next(cursor *BTreeCursor) ([]byte, []byte, error)
```

---

## 5. Defensive Programming (Assertions)

All subsystems use `internal/util/assert.go` for aggressive bug detection:

```go
util.Assert(condition bool, format string, args ...interface{})
util.AssertNotNil(value interface{}, name string)
util.AssertTrue(condition bool, message string)
util.AssertFalse(condition bool, message string)
```

**Assertion patterns by subsystem**:

| Subsystem | Key Assertions |
|-----------|---------------|
| DS | Page type in {0x02,0x05,0x0a,0x0d}; page size [512,65536], power-of-2; key non-empty; cell offset bounds |
| VM | Cursor ID in [0, MaxCursors=256); register index bounds; PC validity |
| QP | Token slice non-nil; parser state invariants |
| QE | PageManager non-nil; table name non-empty; schema validity |
| TM | TransactionManager PageManager non-nil |
| PB | File offset ≥ 0; buffer non-nil; URI non-empty |
| pkg | Row scan index bounds |

Assertions fire on **programming errors** (bugs, violated invariants). Runtime errors (I/O failures, bad user input) use normal `error` returns.

---

## 6. Design Rationale

### Architecture Decisions

| Decision | Rationale |
|----------|-----------|
| Register-based VM | Faster than stack-based; closer to SQLite's VDBE |
| Separate CG from VM | Clean AST-to-bytecode separation; enables future optimization passes |
| SQLite-compatible file format | Existing tools (DB Browser, `.dump`, etc.) work on sqlvibe files |
| VFS abstraction (PB) | `:memory:` databases use same code path as on-disk; testability |
| In-memory rows + B-Tree | Pragmatic: rows kept in memory maps for fast iteration; B-Tree for persistence |
| IS as virtual views | Zero-cost schema introspection without extra storage |
| Aggressive assertions | Catch bugs at the invariant boundary, not deep inside data structures |

### Trade-offs

| Decision | Pros | Cons |
|----------|------|------|
| SQLite-compatible file format | Full tool ecosystem | Complex cell/page encoding |
| B-Tree (not LSM) | Range queries, proven design | More complex than hash/LSM |
| WAL mode | Better read concurrency | Requires checkpoint management |
| In-memory row store | Simple, fast for testing | Not persistent across restart |
| Register-based VM | Efficient execution | More complex register allocation |
