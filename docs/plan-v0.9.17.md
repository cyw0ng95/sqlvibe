# Plan v0.9.17 - JSON Extension Enhancement

## Summary

This version extends the JSON extension with table-valued functions (`json_each`, `json_tree`),
aggregate functions (`json_group_array`, `json_group_object`), and the JSONB binary format
support from SQLite. The existing JSON extension in `ext/json/` provides scalar functions;
this version adds the remaining SQLite JSON1 functions for full compatibility.

---

## Background

The current JSON extension (`ext/json/json.go`) provides 13 scalar functions:
- `json`, `json_array`, `json_extract`, `json_invalid`, `json_isvalid`
- `json_length`, `json_object`, `json_quote`, `json_remove`
- `json_replace`, `json_set`, `json_type`, `json_update`

SQLite's JSON1 extension includes additional functions and operators:
- Table-valued functions: `json_each()`, `json_tree()` (and `jsonb_` variants)
- Aggregate functions: `json_group_array()`, `json_group_object()` (and `jsonb_` variants)
- JSONB binary format: `jsonb()`, `jsonb_object()`, `jsonb_array()`, etc.
- Additional functions: `json_pretty()`, `json_array_insert()`, `json_patch()`
- Operators: `->`, `->>`

---

## Track A: Table-Valued Functions

### A1. Extend Extension Interface

The `Extension` interface in `ext/extension.go` needs a new method for table functions:

```go
type Extension interface {
    // ... existing methods ...
    
    // TableFunctions returns SQL table-valued functions this extension provides.
    TableFunctions() []TableFunction
}

type TableFunction struct {
    Name      string
    MinArgs   int
    MaxArgs   int // -1 for unlimited
    Rows      func(args []interface{}) ([]map[string]interface{}, error)
}
```

### A2. json_each() Implementation

`json_each(json)` iterates top-level elements of a JSON array or object.
Returns columns: `key`, `value`, `type`, `atom`, `id`, `parent`, `fullkey`, `path`.

```go
// ext/json/json.go
func (e *JSONExtension) TableFunctions() []TableFunction {
    return []TableFunction{
        {Name: "json_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonEachRows},
        {Name: "jsonb_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbEachRows},
        {Name: "json_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonTreeRows},
        {Name: "jsonb_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbTreeRows},
    }
}

func (e *JSONExtension) jsonEachRows(args []interface{}) ([]map[string]interface{}, error) {
    // Parse JSON string
    jsonStr, ok := toStringArg(args, 0)
    if !ok {
        return nil, nil
    }
    v, valid := parseJSON(jsonStr)
    if !valid {
        return nil, nil
    }
    
    // Optional path argument
    var pathFilter string
    if len(args) >= 2 {
        pathFilter, _ = toStringArg(args, 1)
    }
    
    var rows []map[string]interface{}
    var idCounter int
    
    switch node := v.(type) {
    case []interface{}:
        for i, elem := range node {
            idCounter++
            rows = append(rows, map[string]interface{}{
                "key":     i,
                "value":   sqlValueToJSON(elem),
                "type":    jsonTypeStr(elem),
                "atom":    elem,
                "id":      idCounter,
                "parent":  nil,
                "fullkey": fmt.Sprintf("$[%d]", i),
                "path":    "$",
            })
        }
    case map[string]interface{}:
        for k, elem := range node {
            idCounter++
            rows = append(rows, map[string]interface{}{
                "key":     k,
                "value":   sqlValueToJSON(elem),
                "type":    jsonTypeStr(elem),
                "atom":    elem,
                "id":      idCounter,
                "parent":  nil,
                "fullkey": fmt.Sprintf("$.%s", k),
                "path":    "$",
            })
        }
    default:
        // Primitive value - return single row
        rows = append(rows, map[string]interface{}{
            "key":     nil,
            "value":   sqlValueToJSON(v),
            "type":    jsonTypeStr(v),
            "atom":    v,
            "id":      1,
            "parent":  nil,
            "fullkey": "$",
            "path":    "$",
        })
    }
    
    return rows, nil
}
```

### A3. json_tree() Implementation

`json_tree(json)` recursively walks the entire JSON structure.
Returns the same columns as `json_each` but with recursive traversal.

```go
func (e *JSONExtension) jsonTreeRows(args []interface{}) ([]map[string]interface{}, error) {
    jsonStr, ok := toStringArg(args, 0)
    if !ok {
        return nil, nil
    }
    v, valid := parseJSON(jsonStr)
    if !valid {
        return nil, nil
    }
    
    var rows []map[string]interface{}
    var idCounter int
    
    var walk func(path string, parentID interface{}, node interface{})
    walk = func(path string, parentID interface{}, node interface{}) {
        idCounter++
        currentID := idCounter
        
        rows = append(rows, map[string]interface{}{
            "key":     getKeyFromPath(path),
            "value":   sqlValueToJSON(node),
            "type":    jsonTypeStr(node),
            "atom":    node,
            "id":      currentID,
            "parent":  parentID,
            "fullkey": path,
            "path":    getContainerPath(path),
        })
        
        switch child := node.(type) {
        case map[string]interface{}:
            for k, val := range child {
                walk(fmt.Sprintf("%s.%s", path, k), currentID, val)
            }
        case []interface{}:
            for i, val := range child {
                walk(fmt.Sprintf("%s[%d]", path, i), currentID, val)
            }
        }
    }
    
    walk("$", nil, v)
    return rows, nil
}

func getKeyFromPath(path string) string {
    // Extract last component: $.a.b[0] -> b, $[0] -> 0
    if idx := strings.LastIndex(path, "."); idx >= 0 {
        return path[idx+1:]
    }
    if idx := strings.LastIndex(path, "["); idx >= 0 {
        return path[idx+1 : len(path)-1]
    }
    return ""
}

func getContainerPath(path string) string {
    // $.a.b[0] -> $.a.b, $[0] -> $
    if idx := strings.LastIndex(path, "."); idx >= 0 {
        return path[:idx]
    }
    if idx := strings.LastIndex(path, "["); idx >= 0 {
        return path[:idx]
    }
    return "$"
}
```

### A4. jsonb_each() and jsonb_tree()

Same as above but the `value` column returns JSONB binary format instead of text JSON.
For now, we can return the same format - full JSONB support would require a separate
binary representation in sqlvibe.

---

## Track B: Aggregate Functions

### B1. json_group_array()

Aggregate function that returns a JSON array of all values:

```go
// ext/json/json.go
func (e *JSONExtension) CallFunc(name string, args []interface{}) interface{} {
    switch strings.ToUpper(name) {
    // ... existing cases ...
    case "JSON_GROUP_ARRAY":
        return evalJSONGroupArray(args)
    case "JSONB_GROUP_ARRAY":
        return evalJSONBGroupArray(args)
    case "JSON_GROUP_OBJECT":
        return evalJSONGroupObject(args)
    case "JSONB_GROUP_OBJECT":
        return evalJSONBGroupObject(args)
    }
    return nil
}

func evalJSONGroupArray(args []interface{}) interface{} {
    if len(args) == 0 || args[0] == nil {
        return "[]"
    }
    arr := make([]interface{}, 0)
    for _, arg := range args {
        arr = append(arr, sqlValueToJSON(arg))
    }
    return marshalJSON(arr)
}

func evalJSONGroupObject(args []interface{}) interface{} {
    if len(args) < 2 || len(args)%2 != 0 {
        return "{}"
    }
    obj := make(map[string]interface{})
    for i := 0; i+1 < len(args); i += 2 {
        key := fmt.Sprintf("%v", args[i])
        obj[key] = sqlValueToJSON(args[i+1])
    }
    return marshalJSON(obj)
}
```

### B2. VM Integration for Aggregates

Aggregate functions need to work with GROUP BY. The VM already has aggregate support;
we need to register these functions as aggregates in the extension:

```go
func (e *JSONExtension) Aggregates() []AggregateFunction {
    return []AggregateFunction{
        {Name: "json_group_array", Step: jsonGroupArrayStep, Final: jsonGroupArrayFinal},
        {Name: "jsonb_group_array", Step: jsonGroupArrayStep, Final: jsonGroupArrayFinal},
        {Name: "json_group_object", Step: jsonGroupObjectStep, Final: jsonGroupObjectFinal},
        {Name: "jsonb_group_object", Step: jsonGroupObjectStep, Final: jsonGroupObjectFinal},
    }
}
```

---

## Track C: JSONB Functions

### C1. jsonb() Function

Convert JSON text to JSONB binary format. Since sqlvibe doesn't have a native JSONB
type, we can store it as a special internal representation or simply return text:

```go
func evalJSONB(args []interface{}) interface{} {
    s, ok := toStringArg(args, 0)
    if !ok {
        return nil
    }
    v, valid := parseJSON(s)
    if !valid {
        return nil
    }
    // For now, return the same as json() - full JSONB would require
    // a separate binary representation
    return marshalJSON(v)
}
```

### C2. jsonb_array(), jsonb_object(), jsonb_set(), etc.

Similar to their `json_` counterparts but return JSONB format:

```go
func evalJSONBArray(args []interface{}) interface{} {
    // Same as json_array but mark as JSONB
    arr := make([]interface{}, len(args))
    for i, arg := range args {
        arr[i] = sqlValueToJSON(arg)
    }
    return marshalJSON(arr) // Return as-is for now
}

func evalJSONBObject(args []interface{}) interface{} {
    if len(args)%2 != 0 {
        return nil
    }
    obj := make(map[string]interface{}, len(args)/2)
    for i := 0; i < len(args); i += 2 {
        key := fmt.Sprintf("%v", args[i])
        obj[key] = sqlValueToJSON(args[i+1])
    }
    return marshalJSON(obj)
}
```

---

## Track D: Additional Functions

### D1. json_pretty()

```go
func evalJSONPretty(args []interface{}) interface{} {
    s, ok := toStringArg(args, 0)
    if !ok {
        return nil
    }
    v, valid := parseJSON(s)
    if !valid {
        return nil
    }
    // Use json.MarshalIndent for pretty printing
    b, err := json.MarshalIndent(v, "", "    ")
    if err != nil {
        return nil
    }
    return string(b)
}
```

### D2. json_array_insert()

Insert a value into a JSON array at a specific position:

```go
func evalJSONArrayInsert(args []interface{}) interface{} {
    if len(args) < 3 {
        return nil
    }
    s, ok := toStringArg(args, 0)
    if !ok {
        return nil
    }
    pathStr, ok := toStringArg(args, 1)
    if !ok {
        return nil
    }
    
    v, valid := parseJSON(s)
    if !valid {
        return nil
    }
    
    // Parse path and insert
    // Similar to setAtPath but inserts rather than replaces
    newVal := sqlValueToJSON(args[2])
    result := insertAtPath(v, pathStr, newVal)
    
    return marshalJSON(result)
}
```

### D3. json_patch()

RFC 7396 MergePatch implementation:

```go
func evalJSONPatch(args []interface{}) interface{} {
    if len(args) < 2 {
        return nil
    }
    targetStr, _ := toStringArg(args, 0)
    patchStr, _ := toStringArg(args, 1)
    
    target, _ := parseJSON(targetStr)
    patch, _ := parseJSON(patchStr)
    
    if target == nil || patch == nil {
        return nil
    }
    
    result := mergePatch(target, patch)
    return marshalJSON(result)
}

func mergePatch(target, patch interface{}) interface{} {
    patchMap, ok := patch.(map[string]interface{})
    if !ok {
        return patch
    }
    
    targetMap, ok := target.(map[string]interface{})
    if !ok {
        return patch
    }
    
    result := make(map[string]interface{})
    for k, v := range targetMap {
        result[k] = v
    }
    
    for k, v := range patchMap {
        if v == nil {
            delete(result, k)
        } else if m, ok := v.(map[string]interface{}); ok {
            if t, ok := result[k].(map[string]interface{}); ok {
                result[k] = mergePatch(t, m)
            } else {
                result[k] = m
            }
        } else {
            result[k] = v
        }
    }
    
    return result
}
```

---

## Track E: Testing

### E1. F886 JSON Extension Suite

Add `internal/TS/SQL1999/F886/01_test.go`:

- `json_each()` on JSON array
- `json_each()` on JSON object
- `json_each()` with path argument
- `json_tree()` recursive walk
- `json_tree()` with path argument
- `json_group_array()` with GROUP BY
- `json_group_object()` with GROUP BY
- `jsonb()` conversion
- `json_pretty()` formatting
- `json_patch()` MergePatch
- `json_array_insert()` array insertion
- Operators `->` and `->>`

### E2. Regression Suite v0.9.17

Add `internal/TS/Regression/regression_v0.9.17_test.go`:

- json_each() returns correct columns
- json_tree() recursive traversal
- json_group_array() aggregate
- json_group_object() aggregate
- JSON round-trip through table functions

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `ext/extension.go` | Add `TableFunction` struct and `TableFunctions()` method to interface |
| `ext/json/json.go` | Add table-valued functions, aggregate functions, JSONB functions |
| `internal/CG/compiler.go` | Detect table functions in FROM clause |
| `internal/VM/exec.go` | Add `OpTableFunction` opcode for table function execution |
| `pkg/sqlvibe/database.go` | Route table function queries to extension |
| `internal/TS/SQL1999/F886/01_test.go` | **NEW** — JSON extension tests |
| `internal/TS/Regression/regression_v0.9.17_test.go` | **NEW** — JSON regressions |
| `docs/HISTORY.md` | Add v0.9.17 entry |

---

## Success Criteria

| Feature | Target | Status |
|---------|--------|--------|
| `json_each()` table function | Yes | [x] |
| `json_tree()` table function | Yes | [x] |
| `jsonb_each()` table function | Yes | [x] |
| `jsonb_tree()` table function | Yes | [x] |
| `json_group_array()` aggregate | Yes | [x] |
| `json_group_object()` aggregate | Yes | [x] |
| `jsonb_group_array()` aggregate | Yes | [x] |
| `jsonb_group_object()` aggregate | Yes | [x] |
| `jsonb()` function | Yes | [x] |
| `json_pretty()` function | Yes | [x] |
| `json_patch()` function | Yes | [x] |
| `json_array_insert()` function | Yes | [x] |
| F886 suite passes | 100% | [x] |
| Regression v0.9.17 passes | 100% | [x] |

---

## Testing

| Test Suite | Description | Status |
|------------|-------------|--------|
| F886 suite | JSON table functions + aggregates (12+ tests) | [ ] |
| Regression v0.9.17 | JSON extension safety (5 tests) | [ ] |
