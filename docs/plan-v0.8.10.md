# Plan v0.8.10 - Extension Framework

## Summary

Design and implement an extension framework for sqlvibe. Extensions are optional packages that add functionality without increasing base binary size.

**Architecture**: Registry Pattern + Build Tags
- Extensions live in `ext/` directory (source root)
- Core binary stays small by default
- Use build tags to include extensions

**Directory Structure**:
```
sqlvibe/
├── ext/                      # Extension packages (source root)
│   ├── extension.go          # Core extension interface & registry
│   ├── json/                 # JSON extension
│   ├── fts5/                # Full-text search extension
│   └── uuid/                 # UUID extension
├── pkg/sqlvibe/              # Core library
└── cmd/                     # CLI tools
```

**Previous**: v0.8.7 delivers VIEW, VACUUM, ANALYZE, PRAGMAs, builtin functions

**v0.8.10 Scope**:
- Extension Framework: 6h
- JSON Extension: 6h
- FTS5 Extension: 8h
- Testing: 4h

---

## Phase 1: Extension Framework (6h)

### Overview

Create core extension infrastructure with Registry Pattern.

### 1.1 Core Interface

```go
// ext/extension.go

package ext

import "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"

type Extension interface {
    Name() string
    Version() string
    Register(db *sqlvibe.Database) error
    Close() error
}
```

### 1.2 Registry

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

func Load(name string, db *sqlvibe.Database) error {
    ext, ok := Get(name)
    if !ok {
        return fmt.Errorf("extension %q not found", name)
    }
    if err := ext.Register(db); err != nil {
        return fmt.Errorf("failed to register %s: %w", name, err)
    }
    mu.Lock()
    loaded[name] = ext
    mu.Unlock()
    return nil
}

func Unload(name string) error {
    mu.Lock()
    defer mu.Unlock()
    ext, ok := loaded[name]
    if !ok {
        return nil
    }
    delete(loaded, name)
    return ext.Close()
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

func Loaded() []string {
    mu.RLock()
    defer mu.RUnlock()
    names := make([]string, 0, len(loaded))
    for name := range loaded {
        names = append(names, name)
    }
    return names
}
```

### 1.3 Build Tags Integration

```go
// ext/extensions.go

// +build extensions

package ext

import (
    _ "github.com/cyw0ng95/sqlvibe/ext/json"
    _ "github.com/cyw0ng95/sqlvibe/ext/fts5"
    _ "github.com/cyw0ng95/sqlvibe/ext/uuid"
)
```

### 1.4 Database Integration

```go
// pkg/sqlvibe/database.go additions

type ExtensionManager interface {
    LoadExtension(name string) error
    UnloadExtension(name string) error
    ListExtensions() []string
}

func (db *Database) LoadExtension(name string) error {
    return ext.Load(name, db)
}

func (db *Database) UnloadExtension(name string) error {
    return ext.Unload(name)
}

func (db *Database) ListExtensions() []string {
    return ext.Loaded()
}
```

### 1.5 PRAGMA Extension Support

```sql
PRAGMA extension_list;           -- List loaded extensions
PRAGMA extension_load('json');   -- Load an extension
```

### Tasks

- [ ] Create `ext/extension.go` with interface
- [ ] Create `ext/registry.go` with registry pattern
- [ ] Create `ext/extensions.go` with build tags
- [ ] Add extension loading to Database
- [ ] Add PRAGMA extension support

**Workload:** ~6 hours

---

## Phase 2: JSON Extension (6h)

### Overview

Implement JSON extension for JSON data type and functions.

### Directory

```
ext/json/
├── json.go
└── json_test.go
```

### Functions

| Function | Description | Example |
|----------|-------------|---------|
| `json(type)` | JSON type | `json('{"a":1}')` |
| `json_extract()` | Extract value | `json_extract('{"a":1}', '$.a')` |
| `json_array()` | Create array | `json_array(1,2,3)` |
| `json_object()` | Create object | `json_object('a',1)` |
| `json_valid()` | Validate JSON | `json_valid('{}')` |

### Implementation

```go
// ext/json/json.go

package json

import (
    "encoding/json"
    "fmt"

    "github.com/cyw0ng95/sqlvibe/ext"
    "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type JSONExtension struct{}

func (e *JSONExtension) Name() string    { return "json" }
func (e *JSONExtension) Version() string { return "1.0.0" }

func (e *JSONExtension) Register(db *sqlvibe.Database) error {
    db.RegisterFunction("json_extract", jsonExtract)
    db.RegisterFunction("json_array", jsonArray)
    db.RegisterFunction("json_object", jsonObject)
    db.RegisterFunction("json_valid", jsonValid)
    return nil
}

func (e *JSONExtension) Close() error { return nil }

func jsonExtract(args ...interface{}) (interface{}, error) {
    if len(args) < 2 {
        return nil, fmt.Errorf("json_extract requires 2 arguments")
    }
    jsonStr, ok := args[0].(string)
    if !ok {
        return nil, fmt.Errorf("first argument must be string")
    }
    path, ok := args[1].(string)
    if !ok {
        return nil, fmt.Errorf("second argument must be string")
    }
    
    var data map[string]interface{}
    if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
        return nil, err
    }
    
    // Parse path like "$.a.b"
    return getByPath(data, path[2:])
}

func getByPath(data map[string]interface{}, path string) interface{} {
    // Simplified: just return the value at path
    keys := strings.Split(path, ".")
    current := interface{}(data)
    for _, key := range keys {
        if m, ok := current.(map[string]interface{}); ok {
            current = m[key]
        }
    }
    return current
}

func jsonArray(args ...interface{}) (interface{}, error) {
    return json.Marshal(args)
}

func jsonObject(args ...interface{}) (interface{}, error) {
    if len(args)%2 != 0 {
        return nil, fmt.Errorf("json_object requires even number of arguments")
    }
    obj := make(map[string]interface{})
    for i := 0; i < len(args); i += 2 {
        key, ok := args[i].(string)
        if !ok {
            return nil, fmt.Errorf("odd arguments must be strings")
        }
        obj[key] = args[i+1]
    }
    return json.Marshal(obj)
}

func jsonValid(args ...interface{}) (interface{}, error) {
    if len(args) < 1 {
        return nil, fmt.Errorf("json_valid requires 1 argument")
    }
    str, ok := args[0].(string)
    if !ok {
        return nil, fmt.Errorf("argument must be string")
    }
    var v interface{}
    return json.Valid([]byte(str)), json.Unmarshal([]byte(str), &v)
}

func init() {
    ext.Register("json", func() ext.Extension {
        return &JSONExtension{}
    })
}
```

### Tasks

- [ ] Create `ext/json/json.go`
- [ ] Implement `json_extract` function
- [ ] Implement `json_array` function
- [ ] Implement `json_object` function
- [ ] Implement `json_valid` function
- [ ] Add tests

**Workload:** ~6 hours

---

## Phase 3: FTS5 Extension (8h)

### Overview

Implement Full-Text Search extension using existing RoaringBitmap.

### Directory

```
ext/fts5/
├── fts5.go
└── fts5_test.go
```

### Features

| Feature | Description |
|---------|-------------|
| CREATE VIRTUAL TABLE | Create FTS5 virtual table |
| MATCH | Full-text search |
| Snippet | Highlight matching text |

### Implementation

```go
// ext/fts5/fts5.go

package fts5

import (
    "fmt"
    "strings"

    "github.com/cyw0ng95/sqlvibe/ext"
    "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
    "github.com/cyw0ng95/sqlvibe/internal/DS"
)

type FTS5Extension struct{}

func (e *FTS5Extension) Name() string    { return "fts5" }
func (e *FTS5Extension) Version() string  { return "1.0.0" }

func (e *FTS5Extension) Register(db *sqlvibe.Database) error {
    // Register virtual table handler
    return nil
}

func (e *FTS5Extension) Close() error { return nil }

// FTS5Table implements virtual table for FTS5
type FTS5Table struct {
    name    string
    columns []string
    index   *DS.RoaringBitmap
    docs    []string
}

func CreateFTS5Table(db *sqlvibe.Database, name string, columns []string) (*FTS5Table, error) {
    ft := &FTS5Table{
        name:    name,
        columns: columns,
        index:   DS.NewRoaringBitmap(),
        docs:    make([]string, 0),
    }
    return ft, nil
}

func (ft *FTS5Table) Insert(docID int64, content string) error {
    words := tokenize(content)
    for _, word := range words {
        // Index each word
    }
    ft.docs = append(ft.docs, content)
    return nil
}

func (ft *FTS5Table) Search(query string) ([]int64, error) {
    terms := tokenize(query)
    // Search using index
    return results, nil
}

func tokenize(text string) []string {
    text = strings.ToLower(text)
    return strings.Fields(text)
}

func init() {
    ext.Register("fts5", func() ext.Extension {
        return &FTS5Extension{}
    })
}
```

### Tasks

- [ ] Create `ext/fts5/fts5.go`
- [ ] Implement virtual table interface
- [ ] Implement tokenization
- [ ] Implement FTS MATCH operator
- [ ] Implement Snippet function
- [ ] Add tests

**Workload:** ~8 hours

---

## Phase 4: Testing (4h)

### Overview

Test extension framework and each extension. Use L2 temp files only.

### Test File: ext/extension_test.go

```go
package ext

import (
    "testing"
    "os"
    "path/filepath"

    "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func setupTestDB(t *testing.T) *sqlvibe.Database {
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")
    db, err := sqlvibe.Open(dbPath)
    if err != nil {
        t.Fatalf("Failed to open database: %v", err)
    }
    return db
}

func TestExtensionRegistry_Register(t *testing.T) {
    called := false
    Register("test_ext", func() Extension {
        return &testExtension{called: &called}
    })
    if !called {
        t.Error("Factory not called")
    }
}

func TestExtensionRegistry_Get(t *testing.T) {
    Register("get_test", func() Extension {
        return &testExtension{}
    })
    ext, ok := Get("get_test")
    if !ok {
        t.Fatal("Extension not found")
    }
    if ext.Name() != "get_test" {
        t.Errorf("Name = %s, want get_test", ext.Name())
    }
}

func TestExtensionRegistry_List(t *testing.T) {
    Register("list_test_1", func() Extension { return &testExtension{} })
    Register("list_test_2", func() Extension { return &testExtension{} })
    
    list := List()
    if len(list) < 2 {
        t.Errorf("Expected at least 2 extensions, got %d", len(list))
    }
}

type testExtension struct {
    called *bool
}

func (e *testExtension) Name() string    { return "test" }
func (e *testExtension) Version() string { return "1.0.0" }
func (e *testExtension) Register(db *sqlvibe.Database) error {
    if e.called != nil {
        *e.called = true
    }
    return nil
}
func (e *testExtension) Close() error { return nil }
```

### Test File: ext/json/json_test.go

```go
package json

import (
    "testing"
)

func TestJSONExtract(t *testing.T) {
    result, err := jsonExtract(`{"a":1}`, "$.a")
    if err != nil {
        t.Fatal(err)
    }
    if result != 1 {
        t.Errorf("Expected 1, got %v", result)
    }
}

func TestJSONArray(t *testing.T) {
    result, err := jsonArray(1, 2, 3)
    if err != nil {
        t.Fatal(err)
    }
    expected := "[1,2,3]"
    if string(result) != expected {
        t.Errorf("Expected %s, got %s", expected, result)
    }
}

func TestJSONValid(t *testing.T) {
    result, _ := jsonValid(`{"a":1}`)
    if result != true {
        t.Error("Expected valid JSON to return true")
    }
    
    result, _ = jsonValid(`not json`)
    if result != false {
        t.Error("Expected invalid JSON to return false")
    }
}
```

### Tasks

- [ ] Test extension registry (register, get, list)
- [ ] Test JSON extension functions
- [ ] Test FTS5 tokenization
- [ ] Test FTS5 search
- [ ] Test build tags integration

**Workload:** ~4 hours

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | Extension Framework | 6 |
| 2 | JSON Extension | 6 |
| 3 | FTS5 Extension | 8 |
| 4 | Testing | 4 |

**Total:** ~24 hours

---

## Success Criteria

### Phase 1: Extension Framework

| Criteria | Target | Status |
|----------|--------|--------|
| Extension interface | Works | [ ] |
| Registry pattern | Works | [ ] |
| Build tags | Works | [ ] |
| PRAGMA support | Works | [ ] |

### Phase 2: JSON Extension

| Criteria | Target | Status |
|----------|--------|--------|
| json_extract | Works | [ ] |
| json_array | Works | [ ] |
| json_object | Works | [ ] |
| json_valid | Works | [ ] |

### Phase 3: FTS5 Extension

| Criteria | Target | Status |
|----------|--------|--------|
| Virtual table | Works | [ ] |
| MATCH operator | Works | [ ] |
| Tokenization | Works | [ ] |
| Snippet | Works | [ ] |

### Phase 4: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| Registry tests | 3 tests | [ ] |
| JSON tests | 4 tests | [ ] |
| FTS5 tests | 3 tests | [ ] |
| All tests pass | 100% | [ ] |

---

## Building with Extensions

```bash
# Default (no extensions)
go build -o sqlvibe .

# With extensions
go build -tags "extensions" -o sqlvibe .

# Run with extensions
sqlvibe> PRAGMA extension_load('json');
sqlvibe> SELECT json_extract('{"a":1}', '$.a');
```

---

## Future Extensions (v0.9.0+)

| Extension | Description |
|-----------|-------------|
| `uuid` | UUID generation functions |
| `math` | Mathematical functions |
| `regex` | Regular expression functions |
| `csv` | CSV virtual table |
| `http` | HTTP virtual table |

---

## Notes

- Extensions use Registry Pattern for simplicity
- Build tags control inclusion to keep base binary small
- Extensions can be loaded at runtime via PRAGMA
- All tests use L2 temp files only
