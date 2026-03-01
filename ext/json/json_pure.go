//go:build !SVDB_ENABLE_CGO
// +build !SVDB_ENABLE_CGO

package json

import (
	gojson "encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cyw0ng95/sqlvibe/ext"
)

// CallFunc implements ext.Extension.CallFunc for pure Go build.
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
		return evalJSON(args)
	case "JSONB_ARRAY":
		return evalJSONArray(args)
	case "JSONB_OBJECT":
		return evalJSONObject(args)
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

// TableFunctions implements ext.Extension.TableFunctions for pure Go build.
func (e *JSONExtension) TableFunctions() []ext.TableFunction {
	return []ext.TableFunction{
		{Name: "json_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonEachRows},
		{Name: "jsonb_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbEachRows},
		{Name: "json_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonTreeRows},
		{Name: "jsonb_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbTreeRows},
	}
}

// jsonEachRows implements json_each() table function.
func (e *JSONExtension) jsonEachRows(args []interface{}) ([]map[string]interface{}, error) {
	return jsonEachRowsPure(args)
}

func (e *JSONExtension) jsonbEachRows(args []interface{}) ([]map[string]interface{}, error) {
	return e.jsonEachRows(args)
}

func (e *JSONExtension) jsonTreeRows(args []interface{}) ([]map[string]interface{}, error) {
	return jsonTreeRowsPure(args)
}

func (e *JSONExtension) jsonbTreeRows(args []interface{}) ([]map[string]interface{}, error) {
	return e.jsonTreeRows(args)
}

// ---------- helpers ----------

// setAtPath sets a value at the given path segments.
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

// sqlValueToJSON converts a SQL value to a JSON-compatible value.
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

func evalJSONArray(args []interface{}) interface{} {
	arr := make([]interface{}, len(args))
	for i, arg := range args {
		arr[i] = sqlValueToJSON(arg)
	}
	return marshalJSON(arr)
}

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

func evalJSONB(args []interface{}) interface{} {
	return evalJSON(args)
}

func evalJSONBArray(args []interface{}) interface{} {
	return evalJSONArray(args)
}

func evalJSONBObject(args []interface{}) interface{} {
	return evalJSONObject(args)
}

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

func evalJSONGroupArray(args []interface{}) interface{} {
	arr := make([]interface{}, 0, len(args))
	for _, arg := range args {
		if arg != nil {
			arr = append(arr, sqlValueToJSON(arg))
		}
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
