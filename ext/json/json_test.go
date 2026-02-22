//go:build SVDB_EXT_JSON

package json_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/ext"
	_ "github.com/cyw0ng95/sqlvibe/ext/json"
)

func callJSON(t *testing.T, fn string, args ...interface{}) interface{} {
	t.Helper()
	result, ok := ext.CallFunc(fn, args)
	if !ok {
		t.Fatalf("function %q not found in ext registry", fn)
	}
	return result
}

func TestJSON_Validate(t *testing.T) {
	tests := []struct {
		input string
		want  interface{}
	}{
		{`{"a":1}`, `{"a":1}`},
		{`[1,2,3]`, `[1,2,3]`},
		{`"hello"`, `"hello"`},
		{`null`, `null`},
		{`invalid`, nil},
		{``, nil},
	}
	for _, tt := range tests {
		got := callJSON(t, "json", tt.input)
		if got != tt.want {
			t.Errorf("json(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestJSON_Array(t *testing.T) {
	got := callJSON(t, "json_array", int64(1), "two", int64(3))
	if got != `[1,"two",3]` {
		t.Errorf("json_array = %v", got)
	}
}

func TestJSON_Array_Empty(t *testing.T) {
	got := callJSON(t, "json_array")
	if got != `[]` {
		t.Errorf("json_array() = %v, want []", got)
	}
}

func TestJSON_Extract_Simple(t *testing.T) {
	got := callJSON(t, "json_extract", `{"a":1,"b":"hello"}`, "$.a")
	if got != int64(1) && got != float64(1) {
		t.Errorf("json_extract $.a = %v (%T), want 1", got, got)
	}
}

func TestJSON_Extract_String(t *testing.T) {
	got := callJSON(t, "json_extract", `{"a":"world"}`, "$.a")
	if got != "world" {
		t.Errorf("json_extract $.a = %v, want world", got)
	}
}

func TestJSON_Extract_Nested(t *testing.T) {
	got := callJSON(t, "json_extract", `{"a":{"b":42}}`, "$.a.b")
	if got != float64(42) && got != int64(42) {
		t.Errorf("json_extract $.a.b = %v", got)
	}
}

func TestJSON_Extract_Array(t *testing.T) {
	got := callJSON(t, "json_extract", `[10,20,30]`, "$[1]")
	if got != float64(20) && got != int64(20) {
		t.Errorf("json_extract $[1] = %v", got)
	}
}

func TestJSON_Extract_Missing(t *testing.T) {
	got := callJSON(t, "json_extract", `{"a":1}`, "$.z")
	if got != nil {
		t.Errorf("json_extract $.z = %v, want nil", got)
	}
}

func TestJSON_Extract_Root(t *testing.T) {
	got := callJSON(t, "json_extract", `{"a":1}`, "$")
	if got != `{"a":1}` {
		t.Errorf("json_extract $ = %v", got)
	}
}

func TestJSON_IsValid(t *testing.T) {
	if callJSON(t, "json_isvalid", `{"a":1}`) != int64(1) {
		t.Error("json_isvalid valid JSON should be 1")
	}
	if callJSON(t, "json_isvalid", `not json`) != int64(0) {
		t.Error("json_isvalid invalid JSON should be 0")
	}
}

func TestJSON_Invalid(t *testing.T) {
	if callJSON(t, "json_invalid", `{"a":1}`) != int64(0) {
		t.Error("json_invalid valid JSON should be 0")
	}
	if callJSON(t, "json_invalid", `not json`) != int64(1) {
		t.Error("json_invalid invalid JSON should be 1")
	}
}

func TestJSON_Length_Object(t *testing.T) {
	got := callJSON(t, "json_length", `{"a":1,"b":2,"c":3}`)
	if got != int64(3) {
		t.Errorf("json_length object = %v, want 3", got)
	}
}

func TestJSON_Length_Array(t *testing.T) {
	got := callJSON(t, "json_length", `[1,2,3,4]`)
	if got != int64(4) {
		t.Errorf("json_length array = %v, want 4", got)
	}
}

func TestJSON_Length_AtPath(t *testing.T) {
	got := callJSON(t, "json_length", `{"a":[1,2,3]}`, "$.a")
	if got != int64(3) {
		t.Errorf("json_length $.a = %v, want 3", got)
	}
}

func TestJSON_Object(t *testing.T) {
	got := callJSON(t, "json_object", "a", int64(1), "b", "hello")
	// JSON object key order may vary; check it's valid JSON
	if s, ok := got.(string); !ok {
		t.Errorf("json_object = %v (%T), want string", got, got)
	} else {
		if callJSON(t, "json_isvalid", s) != int64(1) {
			t.Errorf("json_object result is not valid JSON: %v", s)
		}
	}
}

func TestJSON_Quote(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{int64(42), "42"},
		{"hello", `"hello"`},
		{nil, "null"},
	}
	for _, tt := range tests {
		got := callJSON(t, "json_quote", tt.input)
		if got != tt.want {
			t.Errorf("json_quote(%v) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestJSON_Remove(t *testing.T) {
	got := callJSON(t, "json_remove", `{"a":1,"b":2}`, "$.a")
	if got != `{"b":2}` {
		t.Errorf("json_remove = %v", got)
	}
}

func TestJSON_Set(t *testing.T) {
	got := callJSON(t, "json_set", `{"a":1}`, "$.b", int64(2))
	if s, ok := got.(string); !ok {
		t.Errorf("json_set = %v (%T)", got, got)
	} else {
		// Should contain both keys
		aVal := callJSON(t, "json_extract", s, "$.a")
		bVal := callJSON(t, "json_extract", s, "$.b")
		if aVal == nil || bVal == nil {
			t.Errorf("json_set result missing keys: %v", s)
		}
	}
}

func TestJSON_Replace(t *testing.T) {
	// Replace existing key
	got := callJSON(t, "json_replace", `{"a":1}`, "$.a", int64(99))
	if s, ok := got.(string); !ok {
		t.Errorf("json_replace = %T", got)
	} else {
		aVal := callJSON(t, "json_extract", s, "$.a")
		if aVal != float64(99) && aVal != int64(99) {
			t.Errorf("json_replace $.a = %v, want 99", aVal)
		}
	}
	// Replace non-existent key (should leave unchanged)
	got2 := callJSON(t, "json_replace", `{"a":1}`, "$.z", int64(99))
	if s, ok := got2.(string); ok {
		zVal := callJSON(t, "json_extract", s, "$.z")
		if zVal != nil {
			t.Errorf("json_replace non-existent $.z should not be created, got %v", zVal)
		}
	}
}

func TestJSON_Type(t *testing.T) {
	tests := []struct {
		json string
		want string
	}{
		{`{"a":1}`, "object"},
		{`[1,2]`, "array"},
		{`"hello"`, "text"},
		{`null`, "null"},
	}
	for _, tt := range tests {
		got := callJSON(t, "json_type", tt.json)
		if got != tt.want {
			t.Errorf("json_type(%v) = %v, want %v", tt.json, got, tt.want)
		}
	}
}

func TestJSON_Type_AtPath(t *testing.T) {
	got := callJSON(t, "json_type", `{"a":[1,2]}`, "$.a")
	if got != "array" {
		t.Errorf("json_type $.a = %v, want array", got)
	}
}

func TestJSON_Update(t *testing.T) {
	// json_update is alias for json_set
	got := callJSON(t, "json_update", `{"a":1}`, "$.a", int64(42))
	if s, ok := got.(string); !ok {
		t.Errorf("json_update = %T", got)
	} else {
		aVal := callJSON(t, "json_extract", s, "$.a")
		if aVal != float64(42) && aVal != int64(42) {
			t.Errorf("json_update $.a = %v, want 42", aVal)
		}
	}
}
