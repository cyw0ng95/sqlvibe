# Plan v0.8.10 - Extension Framework

## Summary

Design and implement an extension framework for sqlvibe. Extensions add functionality to the core (VM, CG, etc.) and are statically compiled in at build time.

**Architecture**: Build Tags + 条件编译
- Extensions in `ext/` directory (source root)
- Extensions can modify VM, CG, and other internal packages
- Use build tags to include extensions at compile time
- Virtual table `sqlitevibe_extensions` shows loaded extensions
- CLI command `.ext` shows extensions in sv-cli
- **JSON extension aligns with SQLite JSON1**: https://sqlite.org/json1.html

**Directory Structure**:
```
sqlvibe/
├── ext/                      # Extension packages (source root)
│   ├── ext.go               # Entry (build tags controlled)
│   ├── extension.go         # Extension interface + Opcode struct
│   ├── registry.go         # Unified registry
│   └── json/               # JSON extension
│       └── json.go         # Extension with Opcodes + Functions
├── pkg/sqlvibe/            # Core library (auto-registers extensions)
└── cmd/                   # CLI tools
```

No separate ops_*.go or cg_*.go files needed - extensions declare Opcodes/Functions directly.

### sqlitevibe_extensions Virtual Table

A read-only virtual table that lists all loaded extensions.

```sql
-- Query loaded extensions
SELECT * FROM sqlitevibe_extensions;

-- Result example:
-- name    | description       | functions
-- json    | JSON extension    | json,json_array,json_extract,json_invalid,json_isvalid,json_length,json_object,json_quote,json_remove,json_replace,json_set,json_type,json_update
```

**Table Schema**:
| Column | Type | Description |
|--------|------|-------------|
| name | TEXT | Extension name |
| description | TEXT | Extension description |
| functions | TEXT | Comma-separated list of functions |

**Implementation**:
```go
// pkg/sqlvibe/sqlitevibe_extensions.go

type sqlitevibeExtensionsTable struct{}

func (t *sqlitevibeExtensionsTable) Columns() []string {
    return []string{"name", "description", "functions"}
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
name    | description    | functions
--------+---------------+----------------------------------
json    | JSON extension | json,json_array,json_extract,json_invalid,json_isvalid,json_length,json_object,json_quote,json_remove,json_replace,json_set,json_type,json_update
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

Create core extension infrastructure with unified registration pattern.

### 1.1 Extension Interface

Extensions declare themselves with Opcodes and Functions included:

```go
// ext/extension.go

package ext

import "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"

type Opcode struct {
    Name    string
    Code    int
    Handler func(*VM, *Instruction) error
}

type Extension interface {
    Name() string
    Description() string
    Functions() []string
    Opcodes() []Opcode
    Register(db *sqlvibe.Database) error
    Close() error
}
```

### 1.2 Registry Pattern

Single registration point - extensions declare themselves, core auto-discovers:

```go
// ext/registry.go

package ext

import (
    "sync"
)

var (
    extensions = make(map[string]Extension)
    mu         sync.RWMutex
)

func Register(name string, ext Extension) {
    mu.Lock()
    defer mu.Unlock()
    extensions[name] = ext
}

func Get(name string) (Extension, bool) {
    mu.RLock()
    defer mu.RUnlock()
    ext, ok := extensions[name]
    return ext, ok
}

func List() []Extension {
    mu.RLock()
    defer mu.RUnlock()
    list := make([]Extension, 0, len(extensions))
    for _, ext := range extensions {
        list = append(list, ext)
    }
    return list
}

func AllOpcodes() []Opcode {
    var ops []Opcode
    for _, ext := range List() {
        ops = append(ops, ext.Opcodes()...)
    }
    return ops
}

func AllFunctions() []string {
    var funcs []string
    for _, ext := range List() {
        funcs = append(funcs, ext.Functions()...)
    }
    return funcs
}
```

### 1.3 Build Tags Entry Point

Only the entry point uses build tags:

```go
// ext/ext.go

// +build SVDB_EXT_JSON

package ext

import _ "github.com/cyw0ng95/sqlvibe/ext/json"
```

### 1.4 Auto-Registration

Database automatically registers all extensions:

```go
// pkg/sqlvibe/database.go additions

func init() {
    // Auto-register all extensions
    for _, ext := range ext.List() {
        // Register functions
        for _, fn := range ext.Functions() {
            db.RegisterFunction(fn, getFuncHandler(ext.Name(), fn))
        }
        
        // Register VM opcodes
        for _, op := range ext.Opcodes() {
            vm.RegisterOp(op.Code, op.Handler)
        }
        
        // Call extension init
        ext.Register(db)
    }
}
```

### 1.5 sqlitevibe_extensions Virtual Table

Uses the registry directly:

```go
func (t *sqlitevibeExtensionsTable) Next() ([]interface{}, error) {
    for _, ext := range ext.List() {
        return []interface{}{
            ext.Name(),
            ext.Description(),
            strings.Join(ext.Functions(), ","),
        }, nil
    }
    return nil, io.EOF
}
```

### Tasks

- [ ] Create `ext/extension.go` with Opcode struct and interface
- [ ] Create `ext/registry.go` with unified registry
- [ ] Create `ext/ext.go` build tags entry
- [ ] Add auto-registration to Database
- [ ] Create sqlitevibe_extensions virtual table

---

## Phase 2: JSON Extension (10h)

### Overview

Implement JSON extension with unified registration - no separate VM/CG files needed.

### Directory

```
ext/json/
├── json.go     # Extension init with Opcodes and Functions
└── json_test.go
```

### Implementation (Unified Registration)

```go
// ext/json/json.go

package json

import (
    "github.com/cyw0ng95/sqlvibe/ext"
    "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type JSONExtension struct{}

func (e *JSONExtension) Name() string    { return "json" }
func (e *JSONExtension) Description() string { return "JSON extension" }

func (e *JSONExtension) Functions() []string {
    return []string{
        "json", "json_array", "json_extract", "json_invalid",
        "json_isvalid", "json_length", "json_object", "json_quote",
        "json_remove", "json_replace", "json_set", "json_type", "json_update",
    }
}

func (e *JSONExtension) Opcodes() []ext.Opcode {
    return []ext.Opcode{
        {Name: "JSONExtract", Code: 256, Handler: evalJSONExtract},
        {Name: "JSONArray", Code: 257, Handler: evalJSONArray},
        {Name: "JSONObject", Code: 258, Handler: evalJSONObject},
        // ...
    }
}

func (e *JSONExtension) Register(db *sqlvibe.Database) error {
    return nil
}

func (e *JSONExtension) Close() error { return nil }

func init() {
    ext.Register("json", &JSONExtension{})
}
```

### Functions (Aligned with SQLite JSON1)

Reference: https://sqlite.org/json1.html

| Function | Description |
|----------|-------------|
| `json(JSON)` | Validate and return JSON |
| `json_array(VALUE...)` | Create JSON array |
| `json_extract(JSON, PATH...)` | Extract value(s) from JSON |
| `json_invalid(JSON)` | Return JSON with invalid UTF-16 replaced |
| `json_isvalid(JSON)` | Return 1 if valid JSON |
| `json_length(JSON, PATH?)` | Return number of elements |
| `json_object(VALUE...)` | Create JSON object |
| `json_quote(VALUE)` | Quote value as JSON |
| `json_remove(JSON, PATH...)` | Remove elements from JSON |
| `json_replace(JSON, PATH, VALUE...)` | Replace values in JSON |
| `json_set(JSON, PATH, VALUE...)` | Set values in JSON |
| `json_type(JSON, PATH?)` | Return type of value |
| `json_update(JSON, PATH, VALUE)` | Alias for json_set |

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
func (e *JSONExtension) Description() string { return "JSON extension" }

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
| Extension interface with Opcode | Works | [ ] |
| Unified registry | Works | [ ] |
| Build tags entry | Works | [ ] |
| Auto-registration in DB | Works | [ ] |
| sqlitevibe_extensions table | Works | [ ] |

### Phase 2: JSON Extension

| Criteria | Target | Status |
|----------|--------|--------|
| json | Works | [ ] |
| json_array | Works | [ ] |
| json_extract | Works | [ ] |
| json_invalid | Works | [ ] |
| json_isvalid | Works | [ ] |
| json_length | Works | [ ] |
| json_object | Works | [ ] |
| json_quote | Works | [ ] |
| json_remove | Works | [ ] |
| json_replace | Works | [ ] |
| json_set | Works | [ ] |
| json_type | Works | [ ] |
| json_update | Works | [ ] |
| SQLite JSON1 compatibility | Works | [ ] |

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

- **Unified registration**: Extensions declare Opcodes/Functions in one place
- No separate ops_*.go or cg_*.go files needed
- Build tags only for entry point (ext/ext.go)
- Auto-discovery: Database finds all extensions at init
- Easy to add new extensions (just add ext/json/, ext/yaml/, etc.)
- Static linking - extensions compiled into binary
- Test both with and without build tags
- Use L2 temp files only for tests
