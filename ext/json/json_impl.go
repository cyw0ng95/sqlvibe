package json

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ext_json
#cgo CFLAGS: -I${SRCDIR}
#include "json.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cyw0ng95/sqlvibe/ext"
)

// CallFunc implements ext.Extension.CallFunc for CGO build.
func (e *JSONExtension) CallFunc(name string, args []interface{}) interface{} {
	switch name {
	case "json", "JSON":
		return callJSON(args)
	case "json_array", "JSON_ARRAY":
		return callJSONArray(args)
	case "json_extract", "JSON_EXTRACT":
		return callJSONExtract(args)
	case "json_invalid", "JSON_INVALID":
		return callJSONInvalid(args)
	case "json_isvalid", "json_valid", "JSON_ISVALID", "JSON_VALID":
		return callJSONIsValid(args)
	case "json_length", "json_array_length", "JSON_LENGTH", "JSON_ARRAY_LENGTH":
		return callJSONLength(args)
	case "json_object", "JSON_OBJECT":
		return callJSONObject(args)
	case "json_quote", "JSON_QUOTE":
		return callJSONQuote(args)
	case "json_keys", "JSON_KEYS":
		return callJSONKeys(args)
	case "json_remove", "JSON_REMOVE":
		return callJSONRemove(args)
	case "json_replace", "JSON_REPLACE":
		return callJSONReplace(args)
	case "json_set", "JSON_SET":
		return callJSONSet(args)
	case "json_type", "JSON_TYPE":
		return callJSONType(args)
	case "json_update", "JSON_UPDATE":
		return callJSONUpdate(args)
	case "jsonb", "JSONB":
		return callJSON(args)
	case "jsonb_array", "JSONB_ARRAY":
		return callJSONArray(args)
	case "jsonb_object", "JSONB_OBJECT":
		return callJSONObject(args)
	case "json_pretty", "JSON_PRETTY":
		return callJSONPretty(args)
	case "json_patch", "JSON_PATCH":
		return callJSONPatch(args)
	case "json_array_insert", "JSON_ARRAY_INSERT":
		return callJSONArrayInsert(args)
	case "json_group_array", "jsonb_group_array", "JSON_GROUP_ARRAY", "JSONB_GROUP_ARRAY":
		return callJSONGroupArray(args)
	case "json_group_object", "jsonb_group_object", "JSON_GROUP_OBJECT", "JSONB_GROUP_OBJECT":
		return callJSONGroupObject(args)
	}
	return nil
}

func (e *JSONExtension) TableFunctions() []ext.TableFunction {
	return []ext.TableFunction{
		{Name: "json_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonEachRows},
		{Name: "jsonb_each", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbEachRows},
		{Name: "json_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonTreeRows},
		{Name: "jsonb_tree", MinArgs: 1, MaxArgs: 2, Rows: e.jsonbTreeRows},
	}
}

// Helper functions to convert Go strings to C strings
func goStringToC(s string) *C.char {
	return C.CString(s)
}

func cStringToGo(cstr *C.char) string {
	if cstr == nil {
		return ""
	}
	return C.GoString(cstr)
}

func freeCString(cstr *C.char) {
	if cstr != nil {
		C.free(unsafe.Pointer(cstr))
	}
}

// sqlValueToJSON converts a SQL value to a JSON-compatible string.
func sqlValueToJSON(v interface{}) string {
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
		// Check if already JSON
		if len(x) > 0 && (x[0] == '{' || x[0] == '[' || x[0] == '"') {
			return x
		}
		// Quote as string
		b, _ := json.Marshal(x)
		return string(b)
	case []byte:
		b, _ := json.Marshal(string(x))
		return string(b)
	default:
		b, _ := json.Marshal(fmt.Sprintf("%v", x))
		return string(b)
	}
}

// parseJSONResult parses a JSON string result from C++ and returns appropriate Go type.
func parseJSONResult(jsonStr string) interface{} {
	if jsonStr == "" {
		return nil
	}
	
	var v interface{}
	if err := json.Unmarshal([]byte(jsonStr), &v); err != nil {
		return nil
	}
	return v
}

// sqlValueToGoValue converts a SQL value to a Go value suitable for JSON.
func sqlValueToGoValue(v interface{}) interface{} {
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

// callJSON validates and canonicalizes a JSON string.
func callJSON(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	
	// First check if it's valid JSON
	cstr := goStringToC(s)
	defer freeCString(cstr)
	
	valid := C.svdb_json_validate(cstr)
	if valid == 0 {
		return nil
	}

	result := C.svdb_json_minify(cstr)
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	jsonStr := cStringToGo(result)
	if jsonStr == "" {
		return nil
	}
	return jsonStr
}

// callJSONArray creates a JSON array from its arguments.
func callJSONArray(args []interface{}) interface{} {
	if len(args) == 0 {
		return "[]"
	}

	arr := make([]interface{}, len(args))
	for i, arg := range args {
		arr[i] = sqlValueToGoValue(arg)
	}
	b, _ := json.Marshal(arr)
	return string(b)
}

// callJSONExtract extracts one or more values from a JSON string.
func callJSONExtract(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	if len(args) == 2 {
		// Single path
		path, ok2 := toStringArg(args, 1)
		if !ok2 {
			return nil
		}

		cJSON := goStringToC(s)
		cPath := goStringToC(path)
		defer freeCString(cJSON)
		defer freeCString(cPath)

		result := C.svdb_json_extract(cJSON, cPath)
		if result == nil {
			return nil
		}
		defer C.svdb_json_free(result)

		jsonStr := cStringToGo(result)
		if jsonStr == "" {
			return nil
		}
		
		// C++ returns raw strings without quotes, so we need to detect the type
		// Try to parse as JSON first
		var v interface{}
		if err := json.Unmarshal([]byte(jsonStr), &v); err == nil {
			// Valid JSON - could be number, bool, object, array, or quoted string
			switch val := v.(type) {
			case nil:
				return nil
			case bool:
				return val
			case float64:
				// Return as float64 to match pure Go behavior (encoding/json returns float64)
				return val
			case string:
				// This was a quoted JSON string, return as-is
				return val
			default:
				// Object or array - return JSON string
				return jsonStr
			}
		}

		// Not valid JSON - must be a raw string value from C++
		return jsonStr
	}

	// Multiple paths - return JSON array
	paths := make([]*C.char, len(args)-1)
	for i, pathArg := range args[1:] {
		if path, ok := toStringArg([]interface{}{pathArg}, 0); ok {
			paths[i] = goStringToC(path)
		}
	}
	defer func() {
		for _, cstr := range paths {
			if cstr != nil {
				freeCString(cstr)
			}
		}
	}()

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	result := C.svdb_json_extract_multi(cJSON, &paths[0], C.int(len(paths)))
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONInvalid returns 1 if the argument is NOT valid JSON.
func callJSONInvalid(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return int64(1)
	}
	cstr := goStringToC(s)
	defer freeCString(cstr)

	valid := C.svdb_json_validate(cstr)
	if valid == 1 {
		return int64(0)
	}
	return int64(1)
}

// callJSONIsValid returns 1 if the argument is valid JSON.
func callJSONIsValid(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return int64(0)
	}
	cstr := goStringToC(s)
	defer freeCString(cstr)

	valid := C.svdb_json_validate(cstr)
	return int64(valid)
}

// callJSONLength returns the number of top-level elements.
func callJSONLength(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	var path string
	if len(args) >= 2 {
		path, _ = toStringArg(args, 1)
	}

	var cPath *C.char
	if path != "" && path != "$" {
		cPath = goStringToC(path)
		defer freeCString(cPath)
	}

	length := C.svdb_json_length(cJSON, cPath)
	if length < 0 {
		return nil
	}
	return int64(length)
}

// callJSONObject creates a JSON object from key-value pairs.
func callJSONObject(args []interface{}) interface{} {
	if len(args)%2 != 0 {
		return nil
	}

	nPairs := len(args) / 2
	cKeys := make([]*C.char, nPairs)
	cValues := make([]*C.char, nPairs)

	for i := 0; i < nPairs; i++ {
		key := fmt.Sprintf("%v", args[i*2])
		cKeys[i] = goStringToC(key)
		cValues[i] = goStringToC(sqlValueToJSON(args[i*2+1]))
	}
	defer func() {
		for _, cstr := range cKeys {
			freeCString(cstr)
		}
		for _, cstr := range cValues {
			freeCString(cstr)
		}
	}()

	result := C.svdb_json_object(&cKeys[0], &cValues[0], C.int(nPairs))
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONQuote converts a value to its JSON representation.
func callJSONQuote(args []interface{}) interface{} {
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
		b, _ := json.Marshal(x)
		return string(b)
	case []byte:
		b, _ := json.Marshal(string(x))
		return string(b)
	default:
		b, _ := json.Marshal(fmt.Sprintf("%v", x))
		return string(b)
	}
}

// callJSONKeys returns an array of keys from a JSON object.
func callJSONKeys(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	var cPath *C.char
	if len(args) >= 2 {
		if path, ok := toStringArg(args, 1); ok && path != "" && path != "$" {
			cPath = goStringToC(path)
			defer freeCString(cPath)
		}
	}

	result := C.svdb_json_keys(cJSON, cPath)
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONRemove removes one or more paths from a JSON value.
func callJSONRemove(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	paths := make([]*C.char, len(args)-1)
	for i, pathArg := range args[1:] {
		if path, ok := toStringArg([]interface{}{pathArg}, 0); ok {
			paths[i] = goStringToC(path)
		}
	}
	defer func() {
		for _, cstr := range paths {
			if cstr != nil {
				freeCString(cstr)
			}
		}
	}()

	result := C.svdb_json_remove(cJSON, &paths[0], C.int(len(paths)))
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONSet sets values at paths (create or replace).
func callJSONSet(args []interface{}) interface{} {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	nPairs := (len(args) - 1) / 2
	pathValuePairs := make([]*C.char, nPairs*2)

	for i := 0; i < nPairs; i++ {
		path, _ := toStringArg(args, 1+i*2)
		value := sqlValueToJSON(args[2+i*2])
		pathValuePairs[i*2] = goStringToC(path)
		pathValuePairs[i*2+1] = goStringToC(value)
	}
	defer func() {
		for _, cstr := range pathValuePairs {
			freeCString(cstr)
		}
	}()

	result := C.svdb_json_set(cJSON, &pathValuePairs[0], C.int(nPairs))
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONReplace replaces values at paths (only existing).
func callJSONReplace(args []interface{}) interface{} {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	nPairs := (len(args) - 1) / 2
	pathValuePairs := make([]*C.char, nPairs*2)

	for i := 0; i < nPairs; i++ {
		path, _ := toStringArg(args, 1+i*2)
		value := sqlValueToJSON(args[2+i*2])
		pathValuePairs[i*2] = goStringToC(path)
		pathValuePairs[i*2+1] = goStringToC(value)
	}
	defer func() {
		for _, cstr := range pathValuePairs {
			freeCString(cstr)
		}
	}()

	result := C.svdb_json_replace(cJSON, &pathValuePairs[0], C.int(nPairs))
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	jsonStr := cStringToGo(result)
	if jsonStr == "" {
		return nil
	}
	return jsonStr
}

// callJSONUpdate is an alias for json_set.
func callJSONUpdate(args []interface{}) interface{} {
	return callJSONSet(args)
}

// callJSONType returns the JSON type of a value.
func callJSONType(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cJSON := goStringToC(s)
	defer freeCString(cJSON)

	// Default to root path "$" if no path provided
	path := "$"
	if len(args) >= 2 {
		if p, ok := toStringArg(args, 1); ok && p != "" {
			path = p
		}
	}
	cPath := goStringToC(path)
	defer freeCString(cPath)

	result := C.svdb_json_type(cJSON, cPath)
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONPretty returns pretty-printed JSON.
func callJSONPretty(args []interface{}) interface{} {
	s, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}

	cstr := goStringToC(s)
	defer freeCString(cstr)

	result := C.svdb_json_pretty(cstr)
	if result == nil {
		return nil
	}
	defer C.svdb_json_free(result)

	return cStringToGo(result)
}

// callJSONPatch applies an RFC 7396 MergePatch.
func callJSONPatch(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	target, ok1 := toStringArg(args, 0)
	patch, ok2 := toStringArg(args, 1)
	if !ok1 || !ok2 {
		return nil
	}

	cTarget := goStringToC(target)
	cPatch := goStringToC(patch)
	defer freeCString(cTarget)
	defer freeCString(cPatch)

	dest := make([]byte, 65536)
	cDest := (*C.char)(unsafe.Pointer(&dest[0]))

	ret := C.svdb_json_patch(cDest, C.size_t(len(dest)), cTarget, cPatch)
	if ret != 0 {
		return nil
	}

	result := cStringToGo(cDest)
	return result
}

// callJSONArrayInsert inserts a value into a JSON array.
func callJSONArrayInsert(args []interface{}) interface{} {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil
	}
	
	// First arg is JSON array
	jsonStr, ok := toStringArg(args, 0)
	if !ok {
		return nil
	}
	
	// Parse JSON
	var arr []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
		return nil
	}
	
	// Process path-value pairs
	for i := 1; i+1 < len(args); i += 2 {
		path, ok := toStringArg(args, i)
		if !ok {
			continue
		}
		value := sqlValueToGoValue(args[i+1])
		
		// Parse path to get index
		if strings.HasPrefix(path, "$[") && strings.HasSuffix(path, "]") {
			idxStr := path[2 : len(path)-1]
			idx, err := strconv.Atoi(idxStr)
			if err == nil {
				if idx < 0 {
					idx = len(arr) + 1 + idx
				}
				if idx < 0 {
					idx = 0
				}
				if idx >= len(arr) {
					arr = append(arr, value)
				} else {
					// Insert at index
					newArr := make([]interface{}, 0, len(arr)+1)
					newArr = append(newArr, arr[:idx]...)
					newArr = append(newArr, value)
					newArr = append(newArr, arr[idx:]...)
					arr = newArr
				}
			}
		}
	}
	
	result, _ := json.Marshal(arr)
	return string(result)
}

// callJSONGroupArray aggregates values into a JSON array.
func callJSONGroupArray(args []interface{}) interface{} {
	return callJSONArray(args)
}

// callJSONGroupObject aggregates key-value pairs into a JSON object.
func callJSONGroupObject(args []interface{}) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return "{}"
	}
	return callJSONObject(args)
}

// Table function implementations - delegate to pure Go helpers
// These functions are defined in json_pure.go and shared between both builds

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
