# Implementation Phases and Tasks

## Project: sqlvibe - SQLite-Compatible Database Engine

**Philosophy**: Each iteration must deliver a valid, testable result. No partial implementations that cannot be verified.

---

## Phase 1: Foundation (Week 1-2)

**Goal**: Core infrastructure and basic file I/O working

### Phase 1.1: Project Setup

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T1.1.1 | Initialize Go module | `go.mod` with module path | `go build` passes |
| T1.1.2 | Create directory structure | All directories per ARCHITECTURE.md | Directory tree matches spec |
| T1.1.3 | Set up logging infrastructure | Logger in `internal/sf/` | Basic logging works |
| T1.1.4 | Create .gitignore | Standard Go .gitignore | No binaries in git |

### Phase 1.2: Platform Bridges (PB)

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T1.2.1 | Implement File interface | `internal/pb/file.go` | Can open/close files |
| T1.2.2 | Implement basic I/O operations | Read/Write/Sync work | Unit tests pass |
| T1.2.3 | Implement file locking | Lock/Unlock operations | Lock conflict handling works |
| T1.2.4 | Add page size support | Configurable page sizes | 4KB, 1KB, 64KB pages work |

### Phase 1.3: Data Storage - Page Management

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T1.3.1 | Define page types and structures | `internal/ds/page.go` | All page types defined |
| T1.3.2 | Implement database header parsing | SQLite header compatible | Can read existing SQLite files |
| T1.3.3 | Implement page read/write | Read/write pages to disk | Read/write cycle preserves data |
| T1.3.4 | Implement free list management | Free page tracking | Space reuse works |

**Phase 1 Deliverable**: Can create, open, and close SQLite-compatible database files

---

## Phase 2: Storage Engine (Week 3-4)

**Goal**: B-Tree storage working with basic CRUD operations

### Phase 2.1: B-Tree Implementation

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T2.1.1 | Implement B-Tree node structure | Node types (leaf/interior) | Correct page format |
| T2.1.2 | Implement B-Tree search | Search operation | Find by key works |
| T2.1.3 | Implement B-Tree insert | Insert with split | Insert preserves order |
| T2.1.4 | Implement B-Tree delete | Delete with merge | Delete maintains tree balance |
| T2.1.5 | Implement cursor operations | Traversal (first, next, prev) | Range queries work |

### Phase 2.2: Table and Index Operations

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T2.2.1 | Implement table B-Tree | INTKEY mode | Table stores rows |
| T2.2.2 | Implement index B-Tree | INDEXKEY mode | Indexes work |
| T2.2.3 | Implement auto-increment | ROWID generation | Unique IDs generated |
| T2.2.4 | Implement overflow pages | Large value storage | > page size values work |

### Phase 2.3: Page Cache

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T2.3.1 | Implement buffer pool | Page cache | LRU eviction works |
| T2.3.2 | Implement cache lookup | Pin/unpin pages | Cache hits/misses tracked |
| T2.3.3 | Implement dirty page writeback | Write dirty pages on flush | Data persisted correctly |

**Phase 2 Deliverable**: Can store and retrieve data using B-Tree, passes basic CRUD tests

---

## Phase 3: Query Processing (Week 5-7)

**Goal**: SQL parsing and query planning working

### Phase 3.1: Tokenizer and Parser

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T3.1.1 | Implement SQL tokenizer | Token stream from SQL | Tokenizes correctly |
| T3.1.2 | Implement parser (goyacc) | Parse tree from tokens | Parses DDL/DML |
| T3.1.3 | Implement AST nodes | AST structure | AST represents SQL |
| T3.1.4 | Implement error handling | Parse errors with position | Good error messages |

### Phase 3.2: Semantic Analysis

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T3.2.1 | Implement schema storage | Schema in sqlite_master | Schema persists |
| T3.2.2 | Implement table resolution | Find table by name | Table lookups work |
| T3.2.3 | Implement column resolution | Resolve column refs | Column binding works |
| T3.2.4 | Implement type checking | Type inference | Types resolved |

### Phase 3.3: Query Planning

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T3.3.1 | Implement table scan planning | Scan plan | Full table scan works |
| T3.3.2 | Implement index selection | Use index if available | Index used when beneficial |
| T3.3.3 | Implement filter planning | WHERE clause plan | Filters pushed down |
| T3.3.4 | Implement sort planning | ORDER BY plan | Sorting works |

**Phase 3 Deliverable**: Can parse and plan basic SQL (CREATE, INSERT, SELECT, UPDATE, DELETE)

---

## Phase 4: Query Execution (Week 8-9)

**Goal**: Execute queries and return results

### Phase 4.1: Virtual Machine

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T4.1.1 | Implement VM architecture | Instruction dispatcher | Can run bytecode |
| T4.1.2 | Implement cursor operations | Open/Close/Next cursors | Cursor management works |
| T4.1.3 | Implement register system | Register allocation | Registers work |
| T4.1.4 | Implement basic opcodes | OpenRead, Next, Column | Can iterate rows |

### Phase 4.2: Expression Evaluation

| Task | Description | Deliverable | Verification |
|------|-------------|--------------|--------------|
| T4.2.1 | Implement literal evaluation | Constants work | Literals evaluate |
| T4.2.2 | Implement column references | Column values | Column refs work |
| T4.2.3 | Implement operators | +, -, *, /, =, <, > | Operators work |
| T4.2.4 | Implement built-in functions | COUNT, SUM, AVG, etc | Functions work |

### Phase 4.3: Query Operators

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T4.3.1 | Implement table scan operator | Full table scan | Returns all rows |
| T4.3.2 | Implement filter operator | WHERE execution | Filters correctly |
| T4.3.3 | Implement projection operator | SELECT columns | Projection works |
| T4.3.4 | Implement sort operator | ORDER BY execution | Sorting works |
| T4.3.5 | Implement aggregate operator | GROUP BY, aggregates | Aggregation works |
| T4.3.6 | Implement limit operator | LIMIT/OFFSET | Limits work |

**Phase 4 Deliverable**: Can execute SELECT queries and return correct results

---

## Phase 5: Transaction Support (Week 10-11)

**Goal**: ACID transactions with concurrency control

### Phase 5.1: Lock Manager

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T5.1.1 | Implement lock types | SHARED/RESERVED/EXCLUSIVE | Lock escalation works |
| T5.1.2 | Implement lock acquisition | Deadlock detection | Correct lock behavior |
| T5.1.3 | Implement database locks | DB-level locking | Only one writer |

### Phase 5.2: Write-Ahead Log (WAL)

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T5.2.1 | Implement WAL format | WAL file structure | Can write frames |
| T5.2.2 | Implement WAL append | Log writes | WAL grows correctly |
| T5.2.3 | Implement WAL recovery | Recover from crash | Data recoverable |
| T5.2.4 | Implement checkpoint | Checkpoint operation | WAL checkpoint works |

### Phase 5.3: Transaction Manager

| Task | Description | Deliverable | Verification |
|------|-------------|--------------|--------------|
| T5.3.1 | Implement BEGIN | Start transaction | Transaction starts |
| T5.3.2 | Implement COMMIT | Commit changes | Changes persisted |
| T5.3.3 | Implement ROLLBACK | Rollback changes | Changes undone |
| T5.3.4 | Implement auto-commit | Default behavior | Auto-commit works |

**Phase 5 Deliverable**: ACID transactions work correctly, survives crashes

---

## Phase 6: Integration and Testing (Week 12)

**Goal**: Full integration and SQLite compatibility testing

### Phase 6.1: CLI and Library Interface

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T6.1.1 | Implement CLI tool | `cmd/sqlvibe` CLI | Can run SQL from CLI |
| T6.1.2 | Implement Go library API | Public API in `pkg/sqlvibe` | Can embed in Go apps |
| T6.1.3 | Implement prepared statements | Statement preparation | Parameters work |
| T6.1.4 | Implement transaction API | High-level transactions | User transactions work |

### Phase 6.2: SQLite Compatibility Testing

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T6.2.1 | Implement sqllogictest runner | Test framework | Can run tests |
| T6.2.2 | Run basic SQL tests | Simple SELECTs | Results match |
| T6.2.3 | Run DML tests | INSERT/UPDATE/DELETE | Results match |
| T6.2.4 | Run transaction tests | ACID tests | Passes |
| T6.2.5 | Run edge case tests | NULLs, types, limits | Correct handling |

### Phase 6.3: Performance Optimization

| Task | Description | Deliverable | Verification |
|------|-------------|-------------|--------------|
| T6.3.1 | Benchmark suite | Performance benchmarks | Baseline metrics |
| T6.3.2 | Optimize hot paths | Performance improvements | Measurable gains |
| T6.3.3 | Memory optimization | Reduce allocations | Lower memory use |

**Phase 6 Deliverable**: Functional database with SQLite-compatible results

---

## Task Dependencies

```
Phase 1: Foundation
├── T1.1.1 ──▶ T1.1.2 ──▶ T1.1.3 ──▶ T1.1.4
├── T1.2.1 ──▶ T1.2.2 ──▶ T1.2.3 ──▶ T1.2.4
└── T1.3.1 ──▶ T1.3.2 ──▶ T1.3.3 ──▶ T1.3.4

Phase 2: Storage Engine
├── T2.1.1 ──▶ T2.1.2 ──▶ T2.1.3 ──▶ T2.1.4 ──▶ T2.1.5
├── T2.2.1 ──▶ T2.2.2 ──▶ T2.2.3 ──▶ T2.2.4
└── T2.3.1 ──▶ T2.3.2 ──▶ T2.3.3

Phase 3: Query Processing
├── T3.1.1 ──▶ T3.1.2 ──▶ T3.1.3 ──▶ T3.1.4
├── T3.2.1 ──▶ T3.2.2 ──▶ T3.2.3 ──▶ T3.2.4
└── T3.3.1 ──▶ T3.3.2 ──▶ T3.3.3 ──▶ T3.3.4

Phase 4: Query Execution
├── T4.1.1 ──▶ T4.1.2 ──▶ T4.1.3 ──▶ T4.1.4
├── T4.2.1 ──▶ T4.2.2 ──▶ T4.2.3 ──▶ T4.2.4
└── T4.3.1 ──▶ T4.3.2 ──▶ T4.3.3 ──▶ T4.3.4 ──▶ T4.3.5 ──▶ T4.3.6

Phase 5: Transactions
├── T5.1.1 ──▶ T5.1.2 ──▶ T5.1.3
├── T5.2.1 ──▶ T5.2.2 ──▶ T5.2.3 ──▶ T5.2.4
└── T5.3.1 ──▶ T5.3.2 ──▶ T5.3.3 ──▶ T5.3.4

Phase 6: Integration
├── T6.1.1 ──▶ T6.1.2 ──▶ T6.1.3 ──▶ T6.1.4
├── T6.2.1 ──▶ T6.2.2 ──▶ T6.2.3 ──▶ T6.2.4 ──▶ T6.2.5
└── T6.3.1 ──▶ T6.3.2 ──▶ T6.3.3

Cross-phase dependencies:
- Phase 2 needs Phase 1 complete
- Phase 3 needs Phase 1.3 (page ops) complete
- Phase 4 needs Phase 2 (storage) + Phase 3 (planning) complete
- Phase 5 needs Phase 2 (storage) complete
- Phase 6 needs all previous phases
```

---

## Verification Criteria

Each phase is considered complete when:

1. **Unit tests pass**: >80% coverage on the phase's code
2. **Integration tests pass**: Components work together
3. **SQLite comparison passes**: Blackbox tests match SQLite output
4. **No regressions**: Previous phase features still work

### Test Categories

| Category | Description | Target |
|----------|-------------|--------|
| Unit Tests | Individual component tests | >80% coverage |
| Integration Tests | Component interaction tests | All paths tested |
| SQLite Comparison | Blackbox SQL tests | Match SQLite 100% |
| Stress Tests | Large data, concurrent access | No data corruption |
| Edge Cases | NULLs, empty tables, boundaries | Correct handling |

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| B-Tree complexity | High | Use well-documented algorithms, extensive testing |
| Parser generation | Medium | Use goyacc with good error handling |
| SQLite compatibility | High | Extensive comparison testing from day 1 |
| Performance | Medium | Profile early, optimize hot paths |

---

## Success Criteria

The project is considered successful when:

1. Can create/open SQLite-compatible database files
2. Can execute basic SQL (CREATE TABLE, SELECT, INSERT, UPDATE, DELETE)
3. ACID transactions work correctly
4. SQL logic tests pass with SQLite comparison
5. Can be used as embedded library in Go applications
