// Package json implements the sqlvibe JSON extension, providing JSON1-compatible
// SQL functions aligned with https://sqlite.org/json1.html.
//
// Register this extension by building with the SVDB_EXT_JSON build tag:
//
//	go build -tags SVDB_EXT_JSON ./...
package json

import (
	gojson "encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cyw0ng95/sqlvibe/ext"
)

// JSONExtension implements the JSON1-compatible extension.
type JSONExtension struct{}

func (e *JSONExtension) Name() string        { return "json" }
func (e *JSONExtension) Description() string { return "JSON extension" }

func (e *JSONExtension) Functions() []string {
	return []string{
		"json", "json_array", "json_extract", "json_invalid",
		"json_isvalid", "json_valid", "json_length", "json_array_length",
		"json_object", "json_quote", "json_keys",
		"json_remove", "json_replace", "json_set", "json_type", "json_update",
		// v0.9.17 additions
		"jsonb", "jsonb_array", "jsonb_object",
		"json_pretty", "json_patch", "json_array_insert",
		"json_group_array", "json_group_object",
		"jsonb_group_array", "jsonb_group_object",
	}
}

func (e *JSONExtension) Opcodes() []ext.Opcode { return nil }

func (e *JSONExtension) Register(db interface{}) error { return nil }

func (e *JSONExtension) Close() error { return nil }

func (e *JSONExtension) CallFunc(name string, args []interface{}) interface{} {
	switch strings.ToUpper(name) {
	case "JSON":
		return evalJSON(args)
	case "JSON_ARRAY":
		return evalJSONArray(args)
	case "JSON_EXTRACT":
		return evalJSONExtract(args)
	case "JSON_INVALID":
		return evalJSONInvalid(args)
	case "JSON_ISVALID", "JSON_VALID":
		return evalJSONIsValid(args)
	case "JSON_LENGTH", "JSON_ARRAY_LENGTH":
		return evalJSONLength(args)
	case "JSON_OBJECT":
		return evalJSONObject(args)
	case "JSON_QUOTE":
		return evalJSONQuote(args)
	case "JSON_KEYS":
		return evalJSONKeys(args)
	case "JSON_REMOVE":
		return evalJSONRemove(args)
	case "JSON_REPLACE":
		return evalJSONModify(args, "replace")
	case "JSON_SET":
		return evalJSONModify(args, "set")
	case "JSON_TYPE":
		return evalJSONType(args)
	case "JSON_UPDATE":
		return evalJSONModify(args, "update")
	case "JSONB":
		return evalJSONB(args)
	case "JSONB_ARRAY":
		return evalJSONBArray(args)
	case "JSONB_OBJECT":
		return evalJSONBObject(args)
	case "JSON_PRETTY":
		return evalJSONPretty(args)
	case "JSON_PATCH":
		return evalJSONPatch(args)
	case "JSON_ARRAY_INSERT":
		return evalJSONArrayInsert(args)
	case "JSON_GROUP_ARRAY", "JSONB_GROUP_ARRAY":
		return evalJSONGroupArray(args)
	case "JSON_GROUP_OBJECT", "JSONB_GROUP_OBJECT":
		return evalJSONGroupObject(args)
	}
	return nil
}

func init() {
	ext.Register("json", &JSONExtension{})
}

// TableFunctions returns the table-valued functions provided by the JSON extension.
func (e *JSONExtension) TableFunctions() []ext.TableFunction {
	return []ext.TableFunction{
		{Name: "json_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonEachRows},
		{Name: "jsonb_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbEachRows},
		{Name: "json_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonTreeRows},
		{Name: "jsonb_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbTreeRows},
	}
}

// Aggregates returns the aggregate functions provided by the JSON extension.
func (e *JSONExtension) Aggregates() []ext.AggregateFunction {
	return []ext.AggregateFunction{
		{Name: "json_group_array"},
		{Name: "jsonb_group_array"},
		{Name: "json_group_object"},
		{Name: "jsonb_group_object"},
	}
}

// ---------- helpers ----------

// toGoValue converts a SQL value to a Go value suitable for JSON encoding.
func toGoValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return x
	case bool:
		return x
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// toStringArg returns the string value of a SQL argument (first arg).
func toStringArg(args []interface{}, i int) (string, bool) {
	if i >= len(args) {
		return "", false
	}
	v := args[i]
	if v == nil {
		return "", false
	}
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return fmt.Sprintf("%v", x), true
	}
}

// parseJSON parses a JSON string into a Go value.
func parseJSON(s string) (interface{}, bool) {
	var v interface{}
	if err := gojson.Unmarshal([]byte(s), &v); err != nil {
		return nil, false
	}
	return v, true
}

// marshalJSON encodes a Go value as a compact JSON string.
func marshalJSON(v interface{}) string {
	b, err := gojson.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

// parsePath parses a SQLite-style JSON path (e.g. "$.a.b[0]") into segments.
// Each segment is either a string (object key) or an int (array index).
func parsePath(path string) ([]interface{}, bool) {
	if path == "" {
		return nil, false
	}
	if path == "$" {
		return []interface{}{}, true
	}
	if !strings.HasPrefix(path, "$") {
		return nil, false
	}
	rest := path[1:] // drop "$"
	var segments []interface{}
	for rest != "" {
		if rest[0] == '.' {
			rest = rest[1:]
			// Read key up to next '.' or '['
			end := strings.IndexAny(rest, ".[")
			if end < 0 {
				end = len(rest)
			}
			key := rest[:end]
			if key == "" {
				return nil, false
			}
			segments = append(segments, key)
			rest = rest[end:]
		} else if rest[0] == '[' {
			end := strings.Index(rest, "]")
			if end < 0 {
				return nil, false
			}
			idxStr := rest[1:end]
			if idxStr == "#" {
				// SQLite uses "#" to mean append (last+1), but we represent as -1
				segments = append(segments, -1)
			} else if strings.HasPrefix(idxStr, "#-") {
				n, err := strconv.Atoi(idxStr[1:]) // e.g. "#-1" → -1
				if err != nil {
					return nil, false
				}
				segments = append(segments, n)
			} else {
				idx, err := strconv.Atoi(idxStr)
				if err != nil {
					return nil, false
				}
				segments = append(segments, idx)
			}
			rest = rest[end+1:]
		} else {
			return nil, false
		}
	}
	return segments, true
}

// getAtPath navigates a parsed JSON value along the given path segments.
// Returns (value, true) if found, or (nil, false).
func getAtPath(v interface{}, segments []interface{}) (interface{}, bool) {
	if len(segments) == 0 {
		return v, true
	}
	seg := segments[0]
	rest := segments[1:]
	switch node := v.(type) {
	case map[string]interface{}:
		key, ok := seg.(string)
		if !ok {
			return nil, false
		}
		child, exists := node[key]
		if !exists {
			return nil, false
		}
		return getAtPath(child, rest)
	case []interface{}:
		idx, ok := seg.(int)
		if !ok {
			return nil, false
		}
		if idx < 0 {
			idx = len(node) + idx
		}
		if idx < 0 || idx >= len(node) {
			return nil, false
		}
		return getAtPath(node[idx], rest)
	default:
		return nil, false
	}
}

// setAtPath sets a value at the given path segments (deep copy of v is NOT made;
// the function mutates in place and returns the updated root).
// mode: "set" (always create), "replace" (only if exists), "update" (only if exists, no insert)
func setAtPath(v interface{}, segments []interface{}, newVal interface{}, mode string) interface{} {
	if len(segments) == 0 {
		return newVal
	}
	seg := segments[0]
	rest := segments[1:]

	switch node := v.(type) {
	case map[string]interface{}:
		key, ok := seg.(string)
		if !ok {
			return v
		}
		if mode == "replace" || mode == "update" {
			_, exists := node[key]
			if !exists {
				return v
			}
		}
		newNode := make(map[string]interface{}, len(node))
		for k, val := range node {
			newNode[k] = val
		}
		if len(rest) == 0 {
			if mode == "replace" || mode == "update" {
				if _, exists := newNode[key]; !exists {
					return v
				}
			}
			newNode[key] = newVal
		} else {
			child := newNode[key]
			newNode[key] = setAtPath(child, rest, newVal, mode)
		}
		return newNode

	case []interface{}:
		idx, ok := seg.(int)
		if !ok {
			return v
		}
		if idx == -1 {
			// append
			if mode == "replace" || mode == "update" {
				return v
			}
			if len(rest) == 0 {
				return append(node, newVal)
			}
			return v
		}
		if idx < 0 {
			idx = len(node) + idx
		}
		if idx < 0 || idx >= len(node) {
			if mode == "replace" || mode == "update" {
				return v
			}
			return v
		}
		newSlice := make([]interface{}, len(node))
		copy(newSlice, node)
		if len(rest) == 0 {
			newSlice[idx] = newVal
		} else {
			newSlice[idx] = setAtPath(newSlice[idx], rest, newVal, mode)
		}
		return newSlice

	default:
		if mode == "set" && len(segments) == 1 {
			// Create a new object with this key
			if key, ok := seg.(string); ok {
				if len(rest) == 0 {
					return map[string]interface{}{key: newVal}
				}
			}
		}
		return v
	}
}

// removeAtPath removes the value at the given path segments.
func removeAtPath(v interface{}, segments []interface{}) interface{} {
	if len(segments) == 0 {
		return v
	}
	seg := segments[0]
	rest := segments[1:]

	switch node := v.(type) {
	case map[string]interface{}:
		key, ok := seg.(string)
		if !ok {
			return v
		}
		newNode := make(map[string]interface{}, len(node))
		for k, val := range node {
			newNode[k] = val
		}
		if len(rest) == 0 {
			delete(newNode, key)
		} else {
			child := newNode[key]
			newNode[key] = removeAtPath(child, rest)
		}
		return newNode

	case []interface{}:
		idx, ok := seg.(int)
		if !ok {
			return v
		}
		if idx < 0 {
			idx = len(node) + idx
		}
		if idx < 0 || idx >= len(node) {
			return v
		}
		if len(rest) == 0 {
			newSlice := make([]interface{}, 0, len(node)-1)
			newSlice = append(newSlice, node[:idx]...)
			newSlice = append(newSlice, node[idx+1:]...)
			return newSlice
		}
		newSlice := make([]interface{}, len(node))
		copy(newSlice, node)
		newSlice[idx] = removeAtPath(newSlice[idx], rest)
		return newSlice

	default:
		return v
	}
}

// jsonTypeStr returns the SQLite JSON type string for a Go value.
func jsonTypeStr(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case bool:
		return "true" // SQLite returns "true"/"false" for boolean
	case float64, int64, int:
		return "integer"
	case string:
		return "text"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	}
	return "text"
}

// sqlValueToJSON converts a SQL value (from Go) to a JSON-compatible value.
func sqlValueToJSON(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return x
	case bool:
		return x
	case string:
		// Try to parse as JSON if it looks like a JSON value
		if len(x) > 0 && (x[0] == '{' || x[0] == '[' || x[0] == '"') {
			var parsed interface{}
			if err := gojson.Unmarshal([]byte(x), &parsed); err == nil {
				return parsed
			}
		}
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// ---------- function implementations ----------

// evalJSON validates and canonicalizes a JSON string.
func evalJSON(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	v, valid := parseJSON(s)
	if !valid {
		return nil
	}
	return marshalJSON(v)
}

// evalJSONArray creates a JSON array from its arguments.
func evalJSONArray(args []interface{}) interface{} {
	arr := make([]interface{}, len(args))
	for i, arg := range args {
		arr[i] = sqlValueToJSON(arg)
	}
	return marshalJSON(arr)
}

// evalJSONExtract extracts one or more values from a JSON string.
func evalJSONExtract(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	root, valid := parseJSON(s)
	if !valid {
		return nil
	}

	if len(args) == 2 {
		// Single path → return scalar or JSON
		pathStr, ok2 := toStringArg(args, 1)
		if !ok2 {
			return nil
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 {
			return nil
		}
		val, found := getAtPath(root, segs)
		if !found {
			return nil
		}
		switch val.(type) {
		case nil:
			return nil
		case string:
			return val
		case int64, float64, int, bool:
			return toGoValue(val)
		default:
			return marshalJSON(val)
		}
	}

	// Multiple paths → return JSON array
	results := make([]interface{}, 0, len(args)-1)
	for _, pathArg := range args[1:] {
		pathStr, ok2 := toStringArg([]interface{}{pathArg}, 0)
		if !ok2 {
			results = append(results, nil)
			continue
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 {
			results = append(results, nil)
			continue
		}
		val, found := getAtPath(root, segs)
		if !found {
			results = append(results, nil)
		} else {
			results = append(results, val)
		}
	}
	return marshalJSON(results)
}

// evalJSONInvalid returns 1 if the argument is NOT valid JSON, 0 otherwise.
// (SQLite's json_invalid was added in 3.38.0 to replace use of json_valid.)
func evalJSONInvalid(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return int64(1)
	}
	_, valid := parseJSON(s)
	if valid {
		return int64(0)
	}
	return int64(1)
}

// evalJSONIsValid returns 1 if the argument is valid JSON, 0 otherwise.
func evalJSONIsValid(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return int64(0)
	}
	_, valid := parseJSON(s)
	if valid {
		return int64(1)
	}
	return int64(0)
}

// evalJSONLength returns the number of top-level elements.
func evalJSONLength(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	root, valid := parseJSON(s)
	if !valid {
		return nil
	}

	var target interface{} = root
	if len(args) >= 2 {
		pathStr, ok2 := toStringArg(args, 1)
		if !ok2 {
			return nil
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 {
			return nil
		}
		val, found := getAtPath(root, segs)
		if !found {
			return nil
		}
		target = val
	}

	switch t := target.(type) {
	case map[string]interface{}:
		return int64(len(t))
	case []interface{}:
		return int64(len(t))
	default:
		return int64(1)
	}
}

// evalJSONObject creates a JSON object from key-value pairs.
func evalJSONObject(args []interface{}) interface{} {
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

// evalJSONQuote converts a value to its JSON representation.
func evalJSONQuote(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v := args[0]
	if v == nil {
		return "null"
	}
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case string:
		b, _ := gojson.Marshal(x)
		return string(b)
	case []byte:
		b, _ := gojson.Marshal(string(x))
		return string(b)
	default:
		b, _ := gojson.Marshal(fmt.Sprintf("%v", x))
		return string(b)
	}
}

// evalJSONKeys returns an array of keys from a JSON object.
func evalJSONKeys(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	root, valid := parseJSON(s)
	if !valid {
		return nil
	}

	var target interface{} = root
	if len(args) >= 2 {
		pathStr, ok2 := toStringArg(args, 1)
		if !ok2 {
			return nil
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 {
			return nil
		}
		val, found := getAtPath(root, segs)
		if !found {
			return nil
		}
		target = val
	}

	obj, ok := target.(map[string]interface{})
	if !ok {
		return nil
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	b, _ := gojson.Marshal(keys)
	return string(b)
}

// evalJSONRemove removes one or more paths from a JSON value.
func evalJSONRemove(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	root, valid := parseJSON(s)
	if !valid {
		return nil
	}

	for _, pathArg := range args[1:] {
		pathStr, ok2 := toStringArg([]interface{}{pathArg}, 0)
		if !ok2 {
			continue
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 || len(segs) == 0 {
			continue
		}
		root = removeAtPath(root, segs)
	}
	return marshalJSON(root)
}

// evalJSONModify handles json_set, json_replace, and json_update.
// mode: "set" (SQLite json_set - create or replace), "replace" (only update existing), "update" (alias for set)
func evalJSONModify(args []interface{}, mode string) interface{} {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	root, valid := parseJSON(s)
	if !valid {
		return nil
	}

	for i := 1; i+1 < len(args); i += 2 {
		pathStr, ok2 := toStringArg([]interface{}{args[i]}, 0)
		if !ok2 {
			continue
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 {
			continue
		}
		newVal := sqlValueToJSON(args[i+1])
		root = setAtPath(root, segs, newVal, mode)
	}
	return marshalJSON(root)
}

// evalJSONType returns the JSON type of a value or value at a path.
func evalJSONType(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	root, valid := parseJSON(s)
	if !valid {
		return nil
	}

	var target interface{} = root
	if len(args) >= 2 {
		pathStr, ok2 := toStringArg(args, 1)
		if !ok2 {
			return nil
		}
		segs, ok3 := parsePath(pathStr)
		if !ok3 {
			return nil
		}
		val, found := getAtPath(root, segs)
		if !found {
			return nil
		}
		target = val
	}

	switch t := target.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		// Check if it's actually an integer
		if t == float64(int64(t)) {
			return "integer"
		}
		return "real"
	case int64, int:
		return "integer"
	case string:
		return "text"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "text"
	}
}

// ---------- v0.9.17 new function implementations ----------

// evalJSONB validates and returns JSON in binary-compatible format (text in sqlvibe).
func evalJSONB(args []interface{}) interface{} {
return evalJSON(args)
}

// evalJSONBArray creates a JSON array (JSONB variant).
func evalJSONBArray(args []interface{}) interface{} {
return evalJSONArray(args)
}

// evalJSONBObject creates a JSON object (JSONB variant).
func evalJSONBObject(args []interface{}) interface{} {
return evalJSONObject(args)
}

// evalJSONPretty returns pretty-printed JSON.
func evalJSONPretty(args []interface{}) interface{} {
s, ok := toStringArg(args, 0)
if !ok {
return nil
}
v, valid := parseJSON(s)
if !valid {
return nil
}
b, err := gojson.MarshalIndent(v, "", "    ")
if err != nil {
return nil
}
return string(b)
}

// evalJSONPatch applies an RFC 7396 MergePatch to a JSON document.
func evalJSONPatch(args []interface{}) interface{} {
if len(args) < 2 {
return nil
}
targetStr, ok1 := toStringArg(args, 0)
patchStr, ok2 := toStringArg(args, 1)
if !ok1 || !ok2 {
return nil
}
target, valid1 := parseJSON(targetStr)
patch, valid2 := parseJSON(patchStr)
if !valid1 || !valid2 {
return nil
}
result := jsonMergePatch(target, patch)
return marshalJSON(result)
}

func jsonMergePatch(target, patch interface{}) interface{} {
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
} else {
existing, exists := result[k]
if exists {
result[k] = jsonMergePatch(existing, v)
} else {
result[k] = v
}
}
}
return result
}

// evalJSONArrayInsert inserts a value into a JSON array at a path.
func evalJSONArrayInsert(args []interface{}) interface{} {
if len(args) < 3 || len(args)%2 == 0 {
return nil
}
s, ok := toStringArg(args, 0)
if !ok {
return nil
}
root, valid := parseJSON(s)
if !valid {
return nil
}
for i := 1; i+1 < len(args); i += 2 {
pathStr, ok2 := toStringArg([]interface{}{args[i]}, 0)
if !ok2 {
continue
}
segs, ok3 := parsePath(pathStr)
if !ok3 || len(segs) == 0 {
continue
}
newVal := sqlValueToJSON(args[i+1])
root = insertAtPath(root, segs, newVal)
}
return marshalJSON(root)
}

func insertAtPath(v interface{}, segments []interface{}, newVal interface{}) interface{} {
if len(segments) == 0 {
return v
}
seg := segments[0]
rest := segments[1:]
switch node := v.(type) {
case []interface{}:
idx, ok := seg.(int)
if !ok {
return v
}
if idx < 0 {
idx = len(node) + 1 + idx
}
if idx < 0 {
idx = 0
}
if len(rest) == 0 {
newSlice := make([]interface{}, 0, len(node)+1)
if idx >= len(node) {
newSlice = append(newSlice, node...)
newSlice = append(newSlice, newVal)
} else {
newSlice = append(newSlice, node[:idx]...)
newSlice = append(newSlice, newVal)
newSlice = append(newSlice, node[idx:]...)
}
return newSlice
}
if idx >= len(node) {
return v
}
newSlice := make([]interface{}, len(node))
copy(newSlice, node)
newSlice[idx] = insertAtPath(newSlice[idx], rest, newVal)
return newSlice
case map[string]interface{}:
key, ok := seg.(string)
if !ok {
return v
}
newNode := make(map[string]interface{}, len(node))
for k, val := range node {
newNode[k] = val
}
if len(rest) == 0 {
newNode[key] = newVal
} else {
newNode[key] = insertAtPath(newNode[key], rest, newVal)
}
return newNode
}
return v
}

// evalJSONGroupArray aggregates values into a JSON array.
// When used as a regular scalar (non-aggregate context), wraps all args in array.
func evalJSONGroupArray(args []interface{}) interface{} {
arr := make([]interface{}, 0, len(args))
for _, arg := range args {
if arg != nil {
arr = append(arr, sqlValueToJSON(arg))
}
}
return marshalJSON(arr)
}

// evalJSONGroupObject aggregates key-value pairs into a JSON object.
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

// jsonEachRows implements json_each() table function.
func (e *JSONExtension) jsonEachRows(args []interface{}) ([]map[string]interface{}, error) {
jsonStr, ok := toStringArg(args, 0)
if !ok {
return nil, nil
}
var root interface{}
if len(args) >= 2 {
pathStr, ok2 := toStringArg(args, 1)
if ok2 {
parsed, valid := parseJSON(jsonStr)
if !valid {
return nil, nil
}
segs, ok3 := parsePath(pathStr)
if !ok3 {
return nil, nil
}
val, found := getAtPath(parsed, segs)
if !found {
return nil, nil
}
root = val
}
} else {
parsed, valid := parseJSON(jsonStr)
if !valid {
return nil, nil
}
root = parsed
}
return jsonEachRowsFromValue(root, "$"), nil
}

func jsonEachRowsFromValue(root interface{}, basePath string) []map[string]interface{} {
var rows []map[string]interface{}
idCounter := 0
switch node := root.(type) {
case []interface{}:
for i, elem := range node {
idCounter++
atomVal := jsonAtomValue(elem)
rows = append(rows, map[string]interface{}{
"key":     int64(i),
"value":   jsonValueStr(elem),
"type":    jsonTypeStr(elem),
"atom":    atomVal,
"id":      int64(idCounter * 2),
"parent":  nil,
"fullkey": fmt.Sprintf("%s[%d]", basePath, i),
"path":    basePath,
})
}
case map[string]interface{}:
keys := make([]string, 0, len(node))
for k := range node {
keys = append(keys, k)
}
sortStrings(keys)
for _, k := range keys {
elem := node[k]
idCounter++
atomVal := jsonAtomValue(elem)
rows = append(rows, map[string]interface{}{
"key":     k,
"value":   jsonValueStr(elem),
"type":    jsonTypeStr(elem),
"atom":    atomVal,
"id":      int64(idCounter * 2),
"parent":  nil,
"fullkey": fmt.Sprintf("%s.%s", basePath, k),
"path":    basePath,
})
}
default:
rows = append(rows, map[string]interface{}{
"key":     nil,
"value":   jsonValueStr(root),
"type":    jsonTypeStr(root),
"atom":    root,
"id":      int64(1),
"parent":  nil,
"fullkey": basePath,
"path":    getContainerPath(basePath),
})
}
return rows
}

// jsonbEachRows implements jsonb_each() - same as json_each for now.
func (e *JSONExtension) jsonbEachRows(args []interface{}) ([]map[string]interface{}, error) {
return e.jsonEachRows(args)
}

// jsonTreeRows implements json_tree() table function.
func (e *JSONExtension) jsonTreeRows(args []interface{}) ([]map[string]interface{}, error) {
jsonStr, ok := toStringArg(args, 0)
if !ok {
return nil, nil
}
parsed, valid := parseJSON(jsonStr)
if !valid {
return nil, nil
}
var root interface{} = parsed
if len(args) >= 2 {
pathStr, ok2 := toStringArg(args, 1)
if ok2 {
segs, ok3 := parsePath(pathStr)
if ok3 {
val, found := getAtPath(parsed, segs)
if found {
root = val
}
}
}
}
var rows []map[string]interface{}
idCounter := 0
var walk func(path string, parentID interface{}, node interface{})
walk = func(path string, parentID interface{}, node interface{}) {
idCounter++
currentID := int64(idCounter * 2)
atomVal := jsonAtomValue(node)
rows = append(rows, map[string]interface{}{
"key":     getKeyFromPath(path),
"value":   jsonValueStr(node),
"type":    jsonTypeStr(node),
"atom":    atomVal,
"id":      currentID,
"parent":  parentID,
"fullkey": path,
"path":    getContainerPath(path),
})
switch child := node.(type) {
case map[string]interface{}:
keys := make([]string, 0, len(child))
for k := range child {
keys = append(keys, k)
}
sortStrings(keys)
for _, k := range keys {
walk(fmt.Sprintf("%s.%s", path, k), currentID, child[k])
}
case []interface{}:
for i, val := range child {
walk(fmt.Sprintf("%s[%d]", path, i), currentID, val)
}
}
}
walk("$", nil, root)
return rows, nil
}

// jsonbTreeRows implements jsonb_tree() - same as json_tree for now.
func (e *JSONExtension) jsonbTreeRows(args []interface{}) ([]map[string]interface{}, error) {
return e.jsonTreeRows(args)
}

// jsonValueStr returns the JSON string representation of a value, or the value itself for scalars.
func jsonValueStr(v interface{}) interface{} {
if v == nil {
return nil
}
switch v.(type) {
case map[string]interface{}, []interface{}:
return marshalJSON(v)
default:
return v
}
}

// jsonAtomValue returns the atomic (scalar) value, or nil for containers.
func jsonAtomValue(v interface{}) interface{} {
switch v.(type) {
case map[string]interface{}, []interface{}:
return nil
}
return v
}

// sortStrings sorts a string slice in place using insertion sort.
func sortStrings(ss []string) {
for i := 1; i < len(ss); i++ {
for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
ss[j], ss[j-1] = ss[j-1], ss[j]
}
}
}

func getKeyFromPath(path string) interface{} {
if path == "$" {
return nil
}
if strings.HasSuffix(path, "]") {
if start := strings.LastIndex(path, "["); start >= 0 {
end := strings.LastIndex(path, "]")
if end > start {
idxStr := path[start+1 : end]
if n, err := strconv.Atoi(idxStr); err == nil {
return int64(n)
}
}
}
return nil
}
if idx := strings.LastIndex(path, "."); idx >= 0 {
return path[idx+1:]
}
return nil
}

func getContainerPath(path string) string {
if path == "$" {
return "$"
}
if strings.HasSuffix(path, "]") {
if idx := strings.LastIndex(path, "["); idx >= 0 {
if idx == 0 {
return "$"
}
return path[:idx]
}
}
if idx := strings.LastIndex(path, "."); idx >= 0 {
if idx == 0 {
return "$"
}
return path[:idx]
}
return "$"
}
