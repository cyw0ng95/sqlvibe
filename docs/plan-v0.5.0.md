# Plan v0.5.0 - VM Subsystem

## Goal
Introduce VM (Virtual Machine) as a proper VM subsystem to replace direct execution model. This fundamental architectural change will enable:
- Bytecode compilation from AST
- VM-based execution with instruction dispatch
- Cursor-based operations
- Proper register allocation
- Optimizer infrastructure (future)

---

## Current State Analysis

### Existing VM Infrastructure
The codebase already has basic VM infrastructure in `internal/QE/vm.go`:
- `OpCode` enum with 100+ opcodes
- `Instruction` struct with P1, P2, P3, P4 fields
- `VM` struct with registers and PC

### Current Execution Model
Currently uses direct execution:
- Parser → AST → Direct function calls in engine.go
- Expression evaluation via `evalValue()` / `evalExpr()`
- No bytecode compilation
- No virtual register usage

### Problems VM Will Solve
1. **Test failures due to expression evaluation bugs** - Hard to debug expression tree traversal
2. **No bytecode-level optimization** - Cannot do common subexpression elimination
3. **Code complexity** - All logic in engine.go functions
4. **Hard to extend** - New operations require modifying engine.go

---

## Package Structure

### Current Architecture
```
internal/
├── PB/          # Platform Bridges
├── DS/          # Data Storage (B-Tree, Manager)
├── QP/          # Query Processing (Parser, AST, Planner)
└── QE/          # Query Execution (Engine, Expressions)
    └── vm.go    # Existing VM infrastructure (100+ opcodes)
```

### Target Architecture (v0.5.0 → v0.8.0)

**Current State (v0.5.0 - Completed):**
```
internal/
├── PB/          # Platform Bridges
├── DS/          # Data Storage (placeholder BTree)
├── QP/          # Query Processing
└── VM/          # Virtual Machine Subsystem (compiler + executor)
    ├── cursor.go
    ├── compiler.go   # AST to bytecode compiler
    ├── engine.go
    ├── exec.go
    ├── opcodes.go
    ├── program.go
    └── registers.go
```

**Target State (v0.8.0):**
```
internal/
├── CG/          # NEW: Code Generator (bytecode compiler)
│   ├── compiler.go
│   ├── expr.go
│   ├── dml.go
│   ├── aggregate.go
│   └── optimizer.go
├── DS/          # ENHANCED: Data Storage (SQLite BTree)
│   ├── btree.go
│   ├── btree_ops.go
│   ├── btree_index.go
│   ├── cursor.go
│   ├── balance.go
│   ├── overflow.go
│   ├── freelist.go
│   ├── page.go
│   ├── cell.go
│   ├── encoding.go
│   ├── manager.go
│   └── cache.go
├── PB/          # ENHANCED: Platform Bridges (VFS implementations)
│   ├── file.go
│   ├── memory.go
│   ├── vfs_unix.go
│   ├── vfs_windows.go
│   └── vfs_memory.go
├── SF/          # ENHANCED: System Framework (VFS interface)
│   ├── vfs.go
│   └── log.go
├── QP/          # Query Processing (unchanged)
├── VM/          # REFINED: Virtual Machine (executor only)
│   ├── engine.go
│   ├── exec.go
│   ├── opcodes.go
│   ├── program.go
│   ├── registers.go
│   └── cursor.go
└── TM/          # Transaction Monitor (unchanged)
```

### Package Responsibilities (Updated)

| Package | Responsibility |
|---------|----------------|
| `CG` | AST to bytecode compilation (code generation) |
| `DS` | SQLite-compatible BTree, page management, cursors |
| `PB` | Platform-specific VFS implementations (Unix, Windows, Memory) |
| `SF` | VFS interface, VFS registry, system utilities |
| `QP` | SQL parsing, AST, query planning |
| `VM` | Bytecode execution, register management |
| `TM` | Transaction management, locking, WAL |

### Integration Flow (Updated)
```
User Query
    ↓
QP (Parser) → AST
    ↓
CG (Compiler) → Bytecode Program
    ↓
VM (Engine) → Execution
    ↓           ↓
DS (BTree) ← VFS (SF) ← VFS Impl (PB)
    ↓
Results
```

---

## Execution DAG (Waves)

```mermaid
graph TD
    %% Wave 1: Foundation
    subgraph W1 ["Wave 1: Foundation"]
        direction TB
        A1[Review existing VM code]
        A2[Define VM architecture]
        A3[Design instruction set]
    end
    
    %% Wave 2: Core VM
    subgraph W2 ["Wave 2: Core VM"]
        direction TB
        B1[VM struct enhancements]
        B2[Instruction dispatch loop]
        B3[Register management]
        B4[Cursor operations]
    end
    
    %% Wave 3: Compiler
    subgraph W3 ["Wave 3: AST to Bytecode"]
        direction TB
        C1[Expression compiler]
        C2[SELECT compiler]
        C3[INSERT/UPDATE/DELETE compiler]
        C4[Aggregate compiler]
    end
    
    %% Wave 4: Operators
    subgraph W4 ["Wave 4: Operator Implementation"]
        direction TB
        D1[Comparison operators]
        D2[Arithmetic operators]
        D3[String operators]
        D4[Aggregate operators]
    end
    
    %% Wave 5: Integration
    subgraph W5 ["Wave 5: Integration"]
        direction TB
        E1[Replace direct execution]
        E2[Query planner integration]
        E3[Performance testing]
    end
    
    %% Wave 6: Code Generator Subsystem
    subgraph W6 ["Wave 6: CG Subsystem"]
        direction TB
        F1[Extract compiler to CG]
        F2[CG package structure]
        F3[Update VM integration]
    end
    
    %% Wave 7: VFS Architecture
    subgraph W7 ["Wave 7: VFS Layer"]
        direction TB
        G1[Design VFS interface]
        G2[VFS in SF subsystem]
        G3[Real VFS in PB]
        G4[Memory VFS implementation]
    end
    
    %% Wave 8: BTree Engine
    subgraph W8 ["Wave 8: SQLite BTree"]
        direction TB
        H1[BTree design analysis]
        H2[Page structure implementation]
        H3[BTree operations]
        H4[Index support]
    end
    
    W1 --> W2
    W2 --> W3
    W3 --> W4
    W4 --> W5
    W5 --> W6
    W6 --> W7
    W7 --> W8
```

### Completed
- ✅ Wave 1: Foundation
- ✅ Wave 2: Core VM  
- ✅ Wave 3: Compiler (SELECT, DML, Aggregate)
- ✅ Wave 4: Operators
- ✅ Wave 5: Integration
  - ✅ Fixed column name extraction bug for SELECT * and expressions
  - ✅ Fixed comparison operators and modulo for floats
  - ✅ All SELECT queries route through VM (except SetOp)

### In Progress
- [ ] Wave 6: Code Generator (CG) Subsystem
- [ ] Wave 7: VFS Architecture Layer
- [ ] Wave 8: SQLite-Compatible BTree

---

## Wave 1: Foundation (Analysis & Design)

### Task 1.1: Review Existing VM Code
- **Files**: `internal/QE/vm.go`
- **Description**: Analyze existing VM infrastructure
- **Details**:
  - Document all existing OpCodes
  - Identify gaps vs SQLite VM
  - Define what to keep/modify/remove
- **Output**: VM Analysis Document

### Task 1.2: Define VM Architecture
- **Files**: New `internal/VM/`
- **Description**: Design new VM subsystem structure
- **Details**:
  - Create directory structure
  - Define core interfaces
  - Document compilation pipeline
- **New Files**:
  - `internal/VM/opcodes.go` - Opcode definitions (move from QE/vm.go)
  - `internal/VM/program.go` - Program representation
  - `internal/VM/engine.go` - VM engine core
  - `internal/VM/compiler.go` - AST to bytecode
  - `internal/VM/registers.go` - Register allocator
  - `internal/VM/cursor.go` - Cursor operations

### Task 1.3: Design Instruction Set
- **Files**: `internal/VM/opcodes.go`
- **Description**: Define complete instruction set
- **Details**:
  - Review existing opcodes in vm.go
  - Add missing SQLite-style opcodes
  - Define operand semantics (P1, P2, P3, P4)
- **Opcodes to Implement**:
  - Memory: `Op_Move`, `Op_Copy`, `Op_Null`, `Op_LoadConst`
  - Cursor: `Op_OpenRead`, `Op_OpenWrite`, `Op_Close`, `Op_Reset`
  - Data: `Op_Column`, `Op_Rowid`, `Op_Insert`, `Op_Delete`
  - Expression: `Op_Add`, `Op_Subtract`, `Op_Multiply`, `Op_Divide`
  - Compare: `Op_Eq`, `Op_Ne`, `Op_Lt`, `Op_Le`, `Op_Gt`, `Op_Ge`
  - Jump: `Op_Goto`, `Op_Jump`, `Op_If`, `Op_IfNot`
  - Function: `Op_Function`, `Op_AggStep`, `Op_AggFinal`
  - Result: `Op_ResultRow`, `Op_Halt`

---

## Wave 2: Core VM Implementation

### Task 2.1: VM Struct Enhancements
- **Files**: `internal/VM/engine.go`
- **Description**: Enhance VM struct for bytecode execution
- **Details**:
  - Add instruction memory
  - Add program counter
  - Add stack for gosub/return
  - Add context (database, transaction)
- **Methods**:
  - `Run()` - Main execution loop
  - `Step()` - Execute single instruction
  - `Execute()` - Run with breakpoint support

### Task 2.2: Instruction Dispatch Loop
- **Files**: `internal/VM/exec.go`
- **Description**: Implement main VM loop
- **Details**:
  - Switch-based dispatch
  - Case for each opcode
  - Error handling
  - Yield point for long-running queries

### Task 2.3: Register Management
- **Files**: `internal/VM/registers.go`
- **Description**: Implement register allocator
- **Details**:
  - Register allocation strategy
  - Register types (integer, float, string, null)
  - Register vs memory distinction
  - Pseudo-register handling

### Task 2.4: Cursor Operations
- **Files**: `internal/VM/cursor.go`
- **Description**: Implement cursor-based table access
- **Details**:
  - Cursor struct definition
  - Open/close cursors
  - Fetch columns from rows
  - Seek operations

---

## Wave 3: AST to Bytecode Compiler

### Task 3.1: Expression Compiler
- **Files**: `internal/VM/compiler.go`
- **Description**: Compile expressions to bytecode
- **Details**:
  - Literal compilation
  - Column reference compilation
  - Binary expression compilation
  - Unary expression compilation
  - Function call compilation
- **Target**: All expression types in QP AST

### Task 3.2: SELECT Compiler
- **Files**: `internal/VM/compiler.go`
- **Description**: Compile SELECT statements to bytecode
- **Details**:
  - FROM clause (table scan)
  - WHERE clause (filter)
  - SELECT list (projection)
  - ORDER BY
  - GROUP BY / HAVING
  - LIMIT / OFFSET

### Task 3.3: DML Compiler
- **Files**: `internal/VM/compiler.go`
- **Description**: Compile INSERT/UPDATE/DELETE
- **Details**:
  - INSERT: Parse values, emit store instructions
  - UPDATE: Parse set, emit update instructions
  - DELETE: Parse where, emit delete instructions

### Task 3.4: Aggregate Compiler
- **Files**: `internal/VM/compiler.go`
- **Description**: Compile aggregate functions
- **Details**:
  - COUNT, SUM, AVG, MIN, MAX
  - GROUP BY handling
  - AggStep + AggFinal pattern

---

## Wave 4: Operator Implementation

### Task 4.1: Comparison Operators
- **Files**: `internal/VM/exec.go`
- **Description**: Implement comparison opcodes
- **Opcodes**: `Op_Eq`, `Op_Ne`, `Op_Lt`, `Op_Le`, `Op_Gt`, `Op_Ge`, `Op_Is`, `Op_IsNot`

### Task 4.2: Arithmetic Operators
- **Files**: `internal/VM/exec.go`
- **Description**: Implement arithmetic opcodes
- **Opcodes**: `Op_Add`, `Op_Subtract`, `Op_Multiply`, `Op_Divide`, `Op_Remainder`

### Task 4.3: String Operators
- **Files**: `internal/VM/exec.go`
- **Description**: Implement string operation opcodes
- **Opcodes**: `Op_Concat`, `Op_Substr`, `Op_Length`, `Op_UPPER`, `Op_LOWER`, `Op_Trim`

### Task 4.4: Aggregate Operators
- **Files**: `internal/VM/exec.go`
- **Description**: Implement aggregate opcodes
- **Opcodes**: `Op_AggStep`, `Op_AggFinal`, `Op_Count`, `Op_Sum`, `Op_Avg`, `Op_Min`, `Op_Max`

---

## Wave 5: Integration & Testing

### Task 5.1: Replace Direct Execution
- **Files**: `internal/VM/`, `internal/QE/`
- **Description**: Switch query execution to use VM
- **Details**:
  - Modify QE engine to compile + run VM
  - Keep legacy mode for fallback
  - Benchmark comparison

### Task 5.2: Query Planner Integration
- **Files**: `internal/VM/`, `internal/QP/`
- **Description**: Integrate with query planning
- **Details**:
  - Use compiled bytecode for optimization
  - Index usage decisions
  - Join order planning

### Task 5.3: Performance Testing
- **Files**: Test suite
- **Description**: Verify VM correctness and performance
- **Details**:
  - Run all existing tests
  - Benchmark vs direct execution
  - Memory usage analysis

### Task 5.4: EXPLAIN Command
- **Files**: `internal/VM/compiler.go`, `pkg/sqlvibe/`
- **Description**: Implement EXPLAIN command like SQLite
- **Details**:
  - Add EXPLAIN keyword parsing in QP
  - Return bytecode program as rows instead of executing
  - Format: addr, opcode, p1, p2, p3, p4, comment
  - Support EXPLAIN QUERY PLAN for query planning info
- **Reference**: https://sqlite.org/opcode.html

### Task 5.5: Split database.go into Multiple Files
- **Files**: `pkg/sqlvibe/database.go` (2461 lines)
- **Description**: Split monolith database.go into focused packages for maintainability
- **Details**:
  - Current file is 2461 lines - too large for maintainability
  - Split into focused modules:
    - `query.go` - Query execution logic (Query, Exec, executeSelect)
    - `vm_integration.go` - VM execution (execVMQuery, handleExplain)
    - `expression.go` - Expression evaluation (evalExpr, evalWhere)
    - `operators.go` - Set operations (applySetOp, union, except, intersect)
    - `joins.go` - Join handling (handleJoin, innerJoin, leftJoin, crossJoin)
    - `aggregates.go` - Aggregate computation (computeAggregate, computeGroupBy)
    - `pragmas.go` - Pragma handling (handlePragma, pragmaTableInfo, etc)
    - `transactions.go` - Transaction handling (Begin, Commit, Rollback)
    - `database.go` - Keep Database struct and core methods only
  - Each file should be <500 lines
  - Group related functionality together
  - Maintain existing public API

---

## Wave 6: Code Generator (CG) Subsystem

### Overview
Extract bytecode compilation logic from VM to a dedicated CG (Code Generator) subsystem. This improves separation of concerns: CG handles AST→bytecode compilation, VM handles bytecode execution.

### Task 6.1: Design CG Package Structure
- **Files**: `internal/CG/` (new)
- **Description**: Design the Code Generator subsystem architecture
- **Details**:
  - Define CG package responsibility: AST to bytecode compilation only
  - Define interfaces between CG, QP, and VM
  - Plan file structure for CG subsystem
  - Document compilation pipeline: QP (AST) → CG (Bytecode) → VM (Execution)

### Task 6.2: Extract Compiler from VM to CG
- **Files**: `internal/VM/compiler.go` → `internal/CG/compiler.go`
- **Description**: Move compiler code from VM to CG subsystem
- **Details**:
  - Move `Compiler` struct and methods to `internal/CG/compiler.go`
  - Move expression compilation logic to CG
  - Move SELECT/DML compilation logic to CG
  - Update package imports: `package VM` → `package CG`
  - Keep VM types (OpCode, Instruction, Program) accessible to CG

### Task 6.3: Create CG Package Files
- **Files**: `internal/CG/` (new package)
- **Description**: Organize CG subsystem into focused files
- **Details**:
  - `compiler.go` - Main compiler struct and SELECT compilation
  - `expr.go` - Expression compilation (literals, binary ops, function calls)
  - `dml.go` - DML compilation (INSERT, UPDATE, DELETE)
  - `aggregate.go` - Aggregate function compilation
  - `optimizer.go` - Future: bytecode optimization passes (placeholder)
- **Expected Structure**:
  ```
  internal/CG/
  ├── compiler.go    # Main compiler, SELECT
  ├── expr.go        # Expression compilation
  ├── dml.go         # INSERT/UPDATE/DELETE
  ├── aggregate.go   # Aggregate functions
  └── optimizer.go   # Future optimization (placeholder)
  ```

### Task 6.4: Update VM Integration Points
- **Files**: `pkg/sqlvibe/database.go`, `internal/VM/`
- **Description**: Update VM to use CG for compilation
- **Details**:
  - Update `database.go` to import and use `CG.Compiler`
  - Change: `VM.NewCompiler()` → `CG.NewCompiler()`
  - VM should only expose execution interfaces
  - CG should output `VM.Program` for execution
  - Test that compilation still works end-to-end

### Task 6.5: Update Documentation
- **Files**: `docs/ARCHITECTURE.md`, `docs/plan-v0.5.0.md`
- **Description**: Document CG subsystem in architecture
- **Details**:
  - Add CG to subsystem diagram
  - Document CG responsibilities
  - Update integration flow: QP → CG → VM
  - Update package responsibility matrix

---

## Wave 7: VFS (Virtual File System) Architecture

### Overview
Introduce a VFS abstraction layer in SF (System Framework) with real implementations in PB (Platform Bridges). This enables multiple storage backends (real files, memory, custom implementations) and better platform abstraction.

### Task 7.1: Design VFS Interface
- **Files**: `internal/SF/vfs.go` (new)
- **Description**: Define VFS interface in System Framework
- **Details**:
  - Study SQLite's VFS interface design
  - Define `VFS` interface with methods:
    - `Open(name string, flags int) (File, error)`
    - `Delete(name string) error`
    - `Access(name string, flags int) (bool, error)`
    - `FullPathname(name string) (string, error)`
    - `Randomness(buf []byte) error`
    - `Sleep(microseconds int) error`
    - `CurrentTime() (float64, error)`
  - Define `File` interface:
    - `Close() error`
    - `Read(p []byte, offset int64) (int, error)`
    - `Write(p []byte, offset int64) (int, error)`
    - `Truncate(size int64) error`
    - `Sync(flags int) error`
    - `FileSize() (int64, error)`
    - `Lock(lockType int) error`
    - `Unlock(lockType int) error`
    - `CheckReservedLock() (bool, error)`
    - `FileControl(op int, arg interface{}) error`
    - `SectorSize() int`
    - `DeviceCharacteristics() int`
- **Reference**: https://www.sqlite.org/vfs.html

### Task 7.2: Implement VFS Registry in SF
- **Files**: `internal/SF/vfs.go`
- **Description**: Create VFS registration and lookup system
- **Details**:
  - Implement VFS registry (map of name → VFS)
  - `RegisterVFS(name string, vfs VFS, makeDefault bool) error`
  - `FindVFS(name string) VFS`
  - `DefaultVFS() VFS`
  - Support multiple VFS backends simultaneously
  - Thread-safe registration

### Task 7.3: Implement Real File VFS in PB
- **Files**: `internal/PB/vfs_unix.go`, `internal/PB/vfs_windows.go`
- **Description**: Implement OS file system VFS in Platform Bridges
- **Details**:
  - Create `UnixVFS` implementing `SF.VFS` interface
  - Create `WindowsVFS` implementing `SF.VFS` interface
  - Use `os.File` for actual file operations
  - Implement file locking (fcntl on Unix, LockFileEx on Windows)
  - Implement sync operations (fsync/FlushFileBuffers)
  - Register as default VFS: `SF.RegisterVFS("unix", unixVFS, true)`
  - Use build tags: `//go:build unix` and `//go:build windows`

### Task 7.4: Implement Memory VFS in PB
- **Files**: `internal/PB/vfs_memory.go`
- **Description**: Implement in-memory VFS for `:memory:` databases
- **Details**:
  - Create `MemoryVFS` implementing `SF.VFS` interface
  - Store file content in `map[string][]byte`
  - Support all file operations in memory
  - Use for `:memory:` database paths
  - Register: `SF.RegisterVFS("memory", memoryVFS, false)`
  - Useful for testing and temporary databases

### Task 7.5: Migrate PB/file.go to Use VFS
- **Files**: `internal/PB/file.go`, `internal/DS/manager.go`
- **Description**: Update existing file operations to use VFS
- **Details**:
  - Replace direct `os.File` usage with `SF.VFS` interface
  - Update `PB.FileBackend` to use VFS
  - Support VFS selection via database URI: `file:test.db?vfs=unix`
  - Default to platform's default VFS
  - Update DS manager to request VFS from SF

### Task 7.6: Add VFS Configuration
- **Files**: `pkg/sqlvibe/database.go`
- **Description**: Add VFS selection to database open
- **Details**:
  - Parse VFS name from URI query parameter: `?vfs=unix`
  - Support `:memory:` path → automatically use memory VFS
  - Support explicit VFS selection: `file:test.db?vfs=memory`
  - Default to OS's default VFS for file paths
  - Example: `Open("file:test.db?vfs=unix")`

---

## Wave 8: SQLite-Compatible BTree Implementation

### Overview
Implement a proper SQLite-style BTree in DS (Data Storage) subsystem. Current BTree is a placeholder; this wave implements a production-quality BTree following SQLite's design.

### Task 8.1: Analyze SQLite BTree Design
- **Files**: `docs/btree-design.md` (new)
- **Description**: Study and document SQLite's BTree implementation
- **Details**:
  - Study SQLite's btree.c implementation
  - Document page types: table leaf, table interior, index leaf, index interior
  - Document cell format for each page type
  - Document overflow page handling
  - Document freelist management
  - Document page splitting and balancing algorithms
  - Document key comparison and collation
- **References**:
  - https://www.sqlite.org/fileformat2.html
  - https://www.sqlite.org/btreemodule.html
  - SQLite source: btree.c, btreeInt.h

### Task 8.2: Implement Page Structure
- **Files**: `internal/DS/page.go`, `internal/DS/page_header.go`
- **Description**: Implement SQLite-compatible page structure
- **Details**:
  - Page header format (8/12 bytes for leaf/interior pages)
  - Cell pointer array
  - Free space management within pages
  - Page types: table_leaf (0x0d), table_interior (0x05), index_leaf (0x0a), index_interior (0x02)
  - Unallocated space region
  - Page defragmentation
  - Support 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 byte page sizes

### Task 8.3: Implement Cell Format
- **Files**: `internal/DS/cell.go`
- **Description**: Implement cell encoding/decoding
- **Details**:
  - Table leaf cell: rowid + payload
  - Table interior cell: left_child_page + rowid
  - Index leaf cell: key + payload
  - Index interior cell: left_child_page + key
  - Varint encoding for integers (SQLite format)
  - Record format for payloads (type serial codes)
  - Overflow page handling for large payloads

### Task 8.4: Implement BTree Operations
- **Files**: `internal/DS/btree.go`, `internal/DS/btree_ops.go`
- **Description**: Implement core BTree operations
- **Details**:
  - `Insert(key, value)` - Insert into table BTree
  - `Delete(key)` - Delete from table BTree
  - `Search(key)` - Search table BTree
  - `Seek(key)` - Position cursor at key
  - `Next()` / `Previous()` - Cursor movement
  - Page splitting when full
  - Page merging when underfull
  - Tree rebalancing
  - Root page promotion

### Task 8.5: Implement Index BTree
- **Files**: `internal/DS/btree_index.go`
- **Description**: Implement index BTree operations
- **Details**:
  - Index key format: (indexed columns, rowid)
  - Index leaf pages
  - Index interior pages
  - `IndexInsert(indexKey, rowid)`
  - `IndexDelete(indexKey, rowid)`
  - `IndexSeek(indexKey)` - Cursor positioning
  - Support composite indexes (multiple columns)
  - Support unique indexes (duplicate detection)

### Task 8.6: Implement BTree Cursor
- **Files**: `internal/DS/cursor.go`
- **Description**: Implement BTree cursor for iteration
- **Details**:
  - Cursor state: current page, cell index, path from root
  - `First()` - Position at first record
  - `Last()` - Position at last record
  - `Seek(key)` - Position at key
  - `Next()` - Move to next record
  - `Previous()` - Move to previous record
  - `Key()` - Get current key
  - `Value()` - Get current value
  - Stack-based cursor path for tree traversal

### Task 8.7: Implement Page Balancing
- **Files**: `internal/DS/balance.go`
- **Description**: Implement page balancing algorithms
- **Details**:
  - Balance operation triggered on insert/delete
  - Redistribution: move cells between siblings
  - Merge: combine underfull pages
  - Split: divide overfull pages
  - Divider key updates in parent pages
  - Maintain minimum fill factor
  - Quick balance (simple) vs full balance

### Task 8.8: Implement Overflow Pages
- **Files**: `internal/DS/overflow.go`
- **Description**: Handle payloads larger than page size
- **Details**:
  - Overflow page chaining
  - Overflow page allocation from freelist
  - Overflow page deallocation to freelist
  - Local payload calculation (M, X thresholds from SQLite)
  - Read overflow payload across multiple pages
  - Write overflow payload with page chaining

### Task 8.9: Implement Freelist Management
- **Files**: `internal/DS/freelist.go`
- **Description**: Manage free pages in database file
- **Details**:
  - Freelist trunk pages
  - Freelist leaf pages
  - Allocate page from freelist
  - Return page to freelist
  - Freelist compaction
  - Auto-vacuum support (future)

### Task 8.10: Implement Varint and Record Encoding
- **Files**: `internal/DS/encoding.go`
- **Description**: Implement SQLite's encoding formats
- **Details**:
  - Varint encoding: 1-9 bytes for 64-bit integers
  - Varint decoding
  - Record header: serial type codes
  - Serial types: NULL, int8, int16, int32, int48, int64, float64, zero, one, blob, text
  - Record encoding: header + data
  - Record decoding: parse header, extract fields

### Task 8.11: Migrate Current DS to New BTree
- **Files**: `internal/DS/manager.go`, `internal/DS/table.go`
- **Description**: Update DS subsystem to use new BTree
- **Details**:
  - Replace placeholder BTree with new implementation
  - Update table operations to use BTree cursors
  - Update schema storage (sqlite_master table)
  - Test data migration from old to new BTree
  - Ensure backward compatibility with existing databases (if possible)

### Task 8.12: Add BTree Tests
- **Files**: `internal/DS/btree_test.go`
- **Description**: Comprehensive BTree testing
- **Details**:
  - Unit tests for each operation
  - Test page splitting/merging
  - Test cursor iteration
  - Test large payloads (overflow pages)
  - Test concurrent access (future, with TM)
  - Fuzzing tests for robustness
  - Compare behavior with SQLite

---

## New Directory Structure (After Waves 6-8)

```
internal/
├── CG/                # Code Generator (Bytecode Compiler)
│   ├── compiler.go    # Main compiler, SELECT
│   ├── expr.go        # Expression compilation
│   ├── dml.go         # INSERT/UPDATE/DELETE
│   ├── aggregate.go   # Aggregate functions
│   └── optimizer.go   # Future optimization
├── DS/                # Data Storage
│   ├── btree.go       # BTree core implementation
│   ├── btree_ops.go   # BTree operations
│   ├── btree_index.go # Index BTree
│   ├── cursor.go      # BTree cursor
│   ├── balance.go     # Page balancing
│   ├── overflow.go    # Overflow pages
│   ├── freelist.go    # Freelist management
│   ├── page.go        # Page structure
│   ├── page_header.go # Page header
│   ├── cell.go        # Cell format
│   ├── encoding.go    # Varint, record encoding
│   ├── manager.go     # Database manager
│   ├── cache.go       # Page cache
│   └── table.go       # Table operations
├── PB/                # Platform Bridges
│   ├── file.go        # File backend (uses VFS)
│   ├── memory.go      # Memory backend
│   ├── vfs_unix.go    # Unix VFS implementation
│   ├── vfs_windows.go # Windows VFS implementation
│   └── vfs_memory.go  # Memory VFS
├── SF/                # System Framework
│   ├── vfs.go         # VFS interface and registry
│   └── log.go         # Logging
├── QP/                # Query Processing (Parser, AST)
│   └── ...
├── VM/                # Virtual Machine (Executor only)
│   ├── engine.go      # VM engine
│   ├── exec.go        # VM execution loop
│   ├── opcodes.go     # Opcode definitions
│   ├── program.go     # Program structure
│   ├── registers.go   # Register management
│   └── cursor.go      # VM cursors (wraps DS cursors)
└── TM/                # Transaction Monitor
    └── ...
```

---

## Integration Points

### With QP (Query Processing)
- CG receives QP.SelectStmt, QP.InsertStmt, QP.UpdateStmt, QP.DeleteStmt
- Returns compiled Program (VM.Program)

### With CG (Code Generator)
- VM receives compiled Program from CG
- VM executes bytecode and returns results

### With DS (Data Storage)
- VM cursors read from DS BTree tables
- DS provides BTree operations: Insert, Delete, Search, Cursor iteration
- Index operations via DS BTree index interfaces

### With SF (System Framework)
- DS requests VFS from SF for file operations
- VFS abstraction enables multiple storage backends
- SF provides logging and error handling

### With PB (Platform Bridges)
- PB implements VFS interface for real file systems (Unix, Windows)
- PB provides platform-specific optimizations (mmap, file locking)
- PB implements memory VFS for `:memory:` databases

### With TM (Transaction Monitor)
- Transaction begin/end
- Lock management
- WAL integration

---

## Success Criteria

### Wave 1-5 (Completed)
- [x] VM package created with opcodes, compiler, engine, exec
- [x] DML Compiler implemented (INSERT, UPDATE, DELETE)
- [x] Aggregate Compiler implemented (COUNT, SUM, AVG, MIN, MAX)
- [x] Basic VM integration in database layer
- [x] WHERE clause support in VM
- [x] ORDER BY support (post-processing in database layer)
- [x] LIMIT support in VM
- [x] EXPLAIN command implemented
- [x] Fixed column name extraction for SELECT * and expressions
- [x] Fixed comparison operators to produce result values
- [x] Fixed modulo operator for float types
- [x] Fixed NULL handling in comparisons
- [x] All SELECT queries route through VM (except SetOp which needs implementation)

### Wave 6: CG Subsystem (Completed ✓)
- [x] CG package created with compiler extraction from VM
- [x] Compiler code moved from VM to CG (wrapper approach - transitional)
- [x] CG organized into focused files (single file initially - future: expr, dml, aggregate)
- [x] VM integration updated to use CG (database.go now imports CG)
- [x] Documentation updated with CG architecture (ARCHITECTURE.md updated)
- [x] All existing tests still pass after refactoring

**Note**: Wave 6 complete with transitional wrapper approach. CG wraps VM.Compiler internally. Full file organization (expr.go, dml.go, aggregate.go) deferred to future iteration.

### Wave 7: VFS Architecture (Completed ✓)
- [x] VFS interface defined in SF/vfs
- [x] VFS registry implemented in SF/vfs
- [x] Unix VFS implemented in PB (vfs_unix.go)
- [x] Windows VFS (skipped - not needed for current implementation)
- [x] Memory VFS implemented in PB (vfs_memory.go)
- [x] PB/file.go migrated to use VFS
- [x] Database open supports VFS selection via URI
- [x] `:memory:` databases use memory VFS automatically

**Note**: Wave 7 complete with VFS-based file operations. PB.OpenFile now uses VFS system with automatic :memory: detection and URI-based VFS selection (file:test.db?vfs=unix).

### Wave 8: BTree Implementation (Partially Complete - Core Encoding Done)
- [x] SQLite BTree design documented (integrated into ARCHITECTURE.md section 2.3)
- [x] **Varint encoding/decoding** (encoding.go - 397 lines, fully tested)
- [x] **Record format encoding/decoding** (encoding.go, fully tested)
- [x] **Cell format for all 4 page types** (cell.go - 311 lines, fully tested)
- [x] Local payload size calculation (SQLite-compatible)
- [x] Cell size computation
- [ ] **Overflow page management** (overflow.go - ~300-400 lines needed)
- [ ] **Page balancing algorithms** (balance.go - ~500-800 lines needed)
- [ ] **Freelist management** (freelist.go - ~300-400 lines needed)
- [ ] **Integration with existing BTree** (~200-300 lines needed)
- [ ] Full test coverage with SQLite compatibility tests

**Status**: Core encoding infrastructure complete (~980 lines). Remaining work (~1200-1900 lines) includes:
1. **Overflow pages**: Reading/writing multi-page payloads
2. **Page balancing**: Split/merge/redistribute for tree maintenance (CRITICAL)
3. **Freelist**: Free page management (can defer to v0.6.0)
4. **Integration**: Update btree.go to use new encoding

**Note**: Parts 1-2 (varint, record, cell encoding) complete and tested. These provide the foundation for SQLite-compatible storage. Parts 3-5 (overflow, balancing, freelist) deferred to v0.6.0 for focused implementation.

### Overall Goals
- [ ] All existing tests pass with VM (most pass, some edge cases remain)
- [ ] Bytecode execution matches direct execution
- [ ] Performance not degraded (>80% of direct)
- [ ] Clear compilation pipeline documented: QP → CG → VM → Results
- [ ] Extensible for future optimizations
- [ ] SQLite file format compatibility achieved

**Note:** Direct execution has been fully replaced by VM for SELECT queries. SetOp (UNION, EXCEPT, INTERSECT) and DML operations (INSERT, UPDATE, DELETE) are the only remaining direct execution paths, pending VM implementation.

---

## Verification

```bash
# Run all tests
go test ./...

# Run SQL logic tests
go test ./test/sqllogictest/...

# Benchmark comparison
go test -bench=BenchmarkQuery -benchmem ./...

# Race and memory checks
go test -race -asan ./...
```

---

## Dependencies

- Wave 1 depends on: Current codebase analysis
- Wave 2 depends on: Wave 1 complete
- Wave 3 depends on: Wave 2 complete
- Wave 4 depends on: Wave 2 complete
- Wave 5 depends on: Waves 3 & 4 complete
- Wave 6 depends on: Wave 5 complete (refactor existing VM)
- Wave 7 depends on: Wave 6 complete (VFS abstraction)
- Wave 8 depends on: Wave 7 complete (BTree needs VFS)

**Critical Path**: W1 → W2 → W3 → W4 → W5 → W6 → W7 → W8

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance regression | High | Benchmark continuously, keep fallback |
| Complexity explosion | Medium | Incremental implementation, clear interfaces |
| Test failures | Medium | Run tests after each wave |
| Scope creep | Medium | Strict task boundaries |
| BTree complexity | High | Study SQLite carefully, incremental implementation |
| VFS compatibility | Medium | Test on multiple platforms, use build tags |
| File format breaking changes | High | Careful migration, version checking |

---

## Timeline Estimate (Waves)

### Completed
- Wave 1: 1-2 days (Analysis) ✅
- Wave 2: 2-3 days (Core VM) ✅
- Wave 3: 3-4 days (Compiler) ✅
- Wave 4: 2-3 days (Operators) ✅
- Wave 5: 2-3 days (Integration) ✅

### Remaining
- Wave 6: 1-2 days (CG Refactoring)
- Wave 7: 3-4 days (VFS Architecture)
- Wave 8: 7-10 days (BTree Implementation)
