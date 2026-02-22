# Plan v0.8.10 - Extension Framework

## Summary

Design and implement an extension framework for sqlvibe. Extensions add functionality to the core (VM, CG, etc.) and are statically compiled in at build time.

**Architecture**: Build Tags + 条件编译
- Extensions in `ext/` directory (source root)
- Extensions can modify VM, CG, and other internal packages
- Use build tags to include extensions at compile time
- Virtual table `sqlitevibe_extensions` shows loaded extensions
- CLI command `.ext` shows extensions in sv-cli

**Directory Structure**:
```
sqlvibe/
├── ext/                      # Extension packages (source root)
│   ├── ext.go               # Entry (build tags controlled)
│   └── json/                # JSON extension
│       ├── json.go         # Extension init
│       ├── json_funcs.go   # JSON functions
│       ├── vm_ops.go       # JSON VM ops (+build SVDB_EXT_JSON)
│       └── cg_funcs.go     # JSON codegen (+build SVDB_EXT_JSON)
├── pkg/sqlvibe/            # Core library
├── internal/
│   ├── VM/                 # VM operations
│   │   ├── ops.go          # Base ops
│   │   └── ops_json.go     # JSON ops (+build SVDB_EXT_JSON)
│   └── CG/                 # Code generator
│       ├── codegen.go      # Base codegen
│       └── codegen_json.go # JSON codegen (+build SVDB_EXT_JSON)
└── cmd/                   # CLI tools
```

### sqlitevibe_extensions Virtual Table

A read-only virtual table that lists all loaded extensions.

```sql
-- Query loaded extensions
SELECT * FROM sqlitevibe_extensions;

-- Result example:
-- name    | version | description       | functions
-- json    | 1.0.0   | JSON extension    | json_extract,json_array,json_object,json_valid
-- fts5    | 1.0.0   | Full-text search| fts5,match,snippet
```

**Table Schema**:
| Column | Type | Description |
|--------|------|-------------|
| name | TEXT | Extension name |
| version | TEXT | Extension version |
| description | TEXT | Extension description |
| functions | TEXT | Comma-separated list of functions |

**Implementation**:
```go
// pkg/sqlvibe/sqlitevibe_extensions.go

type sqlitevibeExtensionsTable struct{}

func (t *sqlitevibeExtensionsTable) Columns() []string {
    return []string{"name", "version", "description", "functions"}
}

func (t *sqlitevibeExtensionsTable) Next() ([]interface{}, error) {
    // Iterate through registered extensions
    exts := ext.List()
    if len(exts) == 0 {
        return nil, io.EOF
    }
    // Return extension info
    return []interface{}{
        ext.Name(),
        ext.Version(),
        ext.Description(),
        strings.Join(ext.Functions(), ","),
    }, nil
}
```

**Registration**:
```go
func init() {
    // Register virtual table
    RegisterVirtualTable("sqlitevibe_extensions", &sqlitevibeExtensionsTable{})
}
```

### CLI .ext Command

```bash
sqlvibe> .ext
name    | version | description    | functions
--------+---------+---------------+-------------------------
json    | 1.0.0  | JSON extension | json_extract,json_array,json_object,json_valid
```

**Implementation**:
```go
func handleMetaCommand(line string) bool {
    switch strings.ToLower(line) {
    case ".ext":
        showExtensions()
        return false
    }
}

func showExtensions() {
    rows, err := db.Query("SELECT * FROM sqlitevibe_extensions")
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        return
    }
    // Print formatted table
}
```

**Previous**: v0.8.7 delivers VIEW, VACUUM, ANALYZE, PRAGMAs, builtin functions

**v0.8.10 Scope**:
- Extension Framework: 8h
- JSON Extension: 10h
- sqlitevibe_extensions Table: 4h
- CLI .ext Command: 2h
- Testing: 4h

---

## Phase 1: Extension Framework (8h)

### Overview

Create core extension infrastructure with Build Tags pattern.

### 1.1 Build Tags Structure

```go
// ext/ext.go

// +build SVDB_EXT_JSON

package ext

import (
    _ "github.com/cyw0ng95/sqlvibe/ext/json"
)
```

### 1.2 Extension Interface

```go
// ext/extension.go

package ext

import "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"

type Extension interface {
    Name() string
    Version() string
    Description() string      // Description for sqlitevibe_extensions
    Functions() []string     // List of functions for sqlitevibe_extensions
    Register(db *sqlvibe.Database) error
    Close() error
}
```

### 1.3 Registry Pattern

```go
// ext/registry.go

package ext

import (
    "fmt"
    "sync"
)

var (
    registry = make(map[string]func() Extension)
    mu       sync.RWMutex
    loaded   = make(map[string]Extension)
)

func Register(name string, fn func() Extension) {
    mu.Lock()
    defer mu.Unlock()
    if _, ok := registry[name]; ok {
        panic(fmt.Sprintf("extension %q already registered", name))
    }
    registry[name] = fn
}

func Get(name string) (Extension, bool) {
    mu.RLock()
    defer mu.RUnlock()
    fn, ok := registry[name]
    if !ok {
        return nil, false
    }
    return fn(), true
}

func LoadAll(db *sqlvibe.Database) error {
    mu.RLock()
    defer mu.RUnlock()
    for name, fn := range registry {
        ext := fn()
        if err := ext.Register(db); err != nil {
            return fmt.Errorf("failed to register %s: %w", name, err)
        }
        loaded[name] = ext
    }
    return nil
}

func List() []string {
    mu.RLock()
    defer mu.RUnlock()
    names := make([]string, 0, len(registry))
    for name := range registry {
        names = append(names, name)
    }
    return names
}
```

### 1.4 VM Extension Support

```go
// internal/VM/ops.go

var opHandlers = make(map[OpCode]func(*VM, Instruction) error)

func registerOp(code OpCode, handler func(*VM, Instruction) error) {
    opHandlers[code] = handler
}

// +build SVDB_EXT_JSON

package vm

func init() {
    registerOp(OpJSONExtract, evalJSONExtract)
    registerOp(OpJSONArray, evalJSONArray)
    registerOp(OpJSONObject, evalJSONObject)
}
```

### 1.5 CG Extension Support

```go
// internal/CG/codegen.go

var funcCompilers = make(map[string]func(*Expr, *Program) error)

func registerFunc(name string, compiler func(*Expr, *Program) error) {
    funcCompilers[name] = compiler
}

// +build SVDB_EXT_JSON

package cg

func init() {
    registerFunc("json_extract", compileJSONExtract)
    registerFunc("json_array", compileJSONArray)
    registerFunc("json_object", compileJSONObject)
}
```

### Tasks

- [ ] Create `ext/ext.go` with build tags
- [ ] Create `ext/extension.go` with interface
- [ ] Create `ext/registry.go` with registry pattern
- [ ] Add VM extension support (ops registration)
- [ ] Add CG extension support (func registration)
- [ ] Test build with/without extensions

**Workload:** ~8 hours

---

## Phase 2: JSON Extension (10h)

### Overview

Implement JSON extension with VM operations and code generation.

### Directory

```
ext/json/
├── json.go           # Extension init
├── json_funcs.go     # JSON functions
├── vm_ops.go         # VM operations (+build SVDB_EXT_JSON)
└── cg_funcs.go      # Code generation (+build SVDB_EXT_JSON)
```

### Functions

| Function | Description |
|----------|-------------|
| `json_extract(json, path)` | Extract value from JSON |
| `json_array(args...)` | Create JSON array |
| `json_object(args...)` | Create JSON object |
| `json_valid(json)` | Validate JSON |
| `json_type(json, path)` | Get JSON type |
| `json_quote(value)` | Quote as JSON string |

### Implementation

```go
// ext/json/json.go

package json

import (
    "github.com/cyw0ng95/sqlvibe/ext"
    "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type JSONExtension struct{}

func (e *JSONExtension) Name() string    { return "json" }
func (e *JSONExtension) Version() string { return "1.0.0" }

func (e *JSONExtension) Register(db *sqlvibe.Database) error {
    return nil
}

func (e *JSONExtension) Close() error { return nil }

func init() {
    ext.Register("json", func() ext.Extension {
        return &JSONExtension{}
    })
}
```

```go
// internal/VM/ops_json.go

// +build SVDB_EXT_JSON

package vm

const (
    OpJSONExtract OpCode = 256 + iota
    OpJSONArray
    OpJSONObject
    OpJSONValid
    OpJSONType
    OpJSONQuote
)

func evalJSONExtract(vm *VM, inst Instruction) error {
    // Implementation
    return nil
}
```

```go
// internal/CG/codegen_json.go

// +build SVDB_EXT_JSON

package cg

func compileJSONExtract(expr *Expr, prog *Program) error {
    // Implementation
    return nil
}
```

### Tasks

- [ ] Create `ext/json/json.go`
- [ ] Add JSON VM operations in `internal/VM/ops_json.go`
- [ ] Add JSON code generation in `internal/CG/codegen_json.go`
- [ ] Implement json_extract function
- [ ] Implement json_array function
- [ ] Implement json_object function
- [ ] Implement json_valid function
- [ ] Test with/without build tag

**Workload:** ~10 hours

---

## Phase 3: Testing (4h)

### Overview

Test extension framework and JSON extension. Use L2 temp files only.

### Test Files

```
ext/
├── extension_test.go   # Registry tests
└── json/
    └── json_test.go   # JSON function tests
```

### Test Requirements

- Test with `-tags SVDB_EXT_JSON`
- Test without tags (extensions not loaded)
- Use `t.TempDir()` for all file operations

### Tasks

- [ ] Test extension registry
- [ ] Test JSON functions
- [ ] Test build with tags
- [ ] Test build without tags

**Workload:** ~4 hours

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | Extension Framework | 8 |
| 2 | JSON Extension | 10 |
| 3 | sqlitevibe_extensions Table | 4 |
| 4 | CLI .ext Command | 2 |
| 5 | Testing | 4 |

**Total:** ~28 hours

---

## Building

```bash
# Default (no extensions)
go build -o sqlvibe .

# With JSON extension
go build -tags "SVDB_EXT_JSON" -o sqlvibe .

# With multiple extensions
go build -tags "SVDB_EXT_JSON SVDB_EXT_MATH" -o sqlvibe .
```

### Build Tags Reference

| Tag | Extensions |
|-----|-----------|
| (none) | No extensions |
| `SVDB_EXT_JSON` | JSON extension |
| `SVDB_EXT_MATH` | Math extension (future) |

---

## Success Criteria

### Phase 1: Extension Framework

| Criteria | Target | Status |
|----------|--------|--------|
| Build tags structure | Works | [ ] |
| Extension interface | Works | [ ] |
| Registry pattern | Works | [ ] |
| VM ops registration | Works | [ ] |
| CG func registration | Works | [ ] |

### Phase 2: JSON Extension

| Criteria | Target | Status |
|----------|--------|--------|
| json_extract | Works | [ ] |
| json_array | Works | [ ] |
| json_object | Works | [ ] |
| json_valid | Works | [ ] |
| VM integration | Works | [ ] |
| CG integration | Works | [ ] |

### Phase 3: sqlitevibe_extensions Table

| Criteria | Target | Status |
|----------|--------|--------|
| Virtual table | Works | [ ] |
| Query returns extensions | Works | [ ] |
| Columns correct | Works | [ ] |

### Phase 4: CLI .ext Command

| Criteria | Target | Status |
|----------|--------|--------|
| .ext command | Works | [ ] |
| Shows extensions | Works | [ ] |

### Phase 5: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| Build with tags | Works | [ ] |
| Build without tags | Works | [ ] |
| All tests pass | 100% | [ ] |

---

## Future Extensions

| Extension | Tag | Description |
|-----------|-----|-------------|
| UUID | `SVDB_EXT_UUID` | UUID generation |
| Math | `SVDB_EXT_MATH` | Math functions |
| Regex | `SVDB_EXT_REGEX` | Regex functions |

---

## Notes

- Build tags enable conditional compilation (like C #ifdef)
- Extensions can modify VM, CG, and other internals
- Static linking - extensions compiled into binary
- Test both with and without build tags
- Use L2 temp files only for tests
