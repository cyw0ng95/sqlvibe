// Package json helpers - shared code for both pure Go and CGO builds
package json

import (
	gojson "encoding/json"
	"fmt"
	"strconv"
	"strings"
)

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
				segments = append(segments, -1)
			} else if strings.HasPrefix(idxStr, "#-") {
				n, err := strconv.Atoi(idxStr[1:])
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

// jsonTypeStr returns the SQLite JSON type string for a Go value.
func jsonTypeStr(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case bool:
		return "true"
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

// jsonEachRowsPure implements json_each() table function.
func jsonEachRowsPure(args []interface{}) ([]map[string]interface{}, error) {
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

// jsonTreeRowsPure implements json_tree() table function.
func jsonTreeRowsPure(args []interface{}) ([]map[string]interface{}, error) {
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

func jsonAtomValue(v interface{}) interface{} {
	switch v.(type) {
	case map[string]interface{}, []interface{}:
		return nil
	}
	return v
}

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
