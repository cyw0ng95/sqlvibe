//go:build SVDB_EXT_JSON

package F900

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/cyw0ng95/sqlvibe/ext/json"
)

// TestSQL1999_F900_JSONExtensions_L1 tests the JSON extension functions via SQL queries.
func TestSQL1999_F900_JSONExtensions_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		query string
		want  interface{}
	}{
		{
			name:  "json_validate",
			query: `SELECT json('{"a":1}')`,
			want:  `{"a":1}`,
		},
		{
			name:  "json_invalid_returns_nil",
			query: `SELECT json('bad json')`,
			want:  nil,
		},
		{
			name:  "json_array",
			query: `SELECT json_array(1, 'two', 3)`,
			want:  `[1,"two",3]`,
		},
		{
			name:  "json_extract_number",
			query: `SELECT json_extract('{"a":42}', '$.a')`,
			want:  float64(42),
		},
		{
			name:  "json_extract_string",
			query: `SELECT json_extract('{"name":"alice"}', '$.name')`,
			want:  "alice",
		},
		{
			name:  "json_isvalid_true",
			query: `SELECT json_isvalid('{"a":1}')`,
			want:  int64(1),
		},
		{
			name:  "json_isvalid_false",
			query: `SELECT json_isvalid('not json')`,
			want:  int64(0),
		},
		{
			name:  "json_length_array",
			query: `SELECT json_length('[1,2,3]')`,
			want:  int64(3),
		},
		{
			name:  "json_length_object",
			query: `SELECT json_length('{"a":1,"b":2}')`,
			want:  int64(2),
		},
		{
			name:  "json_quote_string",
			query: `SELECT json_quote('hello')`,
			want:  `"hello"`,
		},
		{
			name:  "json_type_object",
			query: `SELECT json_type('{"a":1}')`,
			want:  "object",
		},
		{
			name:  "json_type_array",
			query: `SELECT json_type('[1,2]')`,
			want:  "array",
		},
		{
			name:  "json_type_null",
			query: `SELECT json_type('null')`,
			want:  "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("Query %q: %v", tt.query, err)
			}
			if len(rows.Data) != 1 || len(rows.Data[0]) != 1 {
				t.Fatalf("expected 1 row/col, got %v", rows.Data)
			}
			got := rows.Data[0][0]
			if got != tt.want {
				t.Errorf("query %q: got %v (%T), want %v (%T)", tt.query, got, got, tt.want, tt.want)
			}
		})
	}
}

// TestSQL1999_F900_JSONExtensionsTable_L1 tests the sqlvibe_extensions virtual table
// when the JSON extension is loaded.
func TestSQL1999_F900_JSONExtensionsTable_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name, description FROM sqlvibe_extensions WHERE name = 'json'")
	if err != nil {
		t.Fatalf("Query sqlvibe_extensions: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row for 'json' extension, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != "json" {
		t.Errorf("expected name='json', got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F900_JSONRemoveReplace_L1 tests json_remove and json_replace.
func TestSQL1999_F900_JSONRemoveReplace_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// json_remove
	rows, err := db.Query(`SELECT json_remove('{"a":1,"b":2}', '$.a')`)
	if err != nil {
		t.Fatalf("json_remove: %v", err)
	}
	if rows.Data[0][0] != `{"b":2}` {
		t.Errorf("json_remove = %v, want {\"b\":2}", rows.Data[0][0])
	}

	// json_replace existing key
	rows2, err := db.Query(`SELECT json_replace('{"a":1}', '$.a', 99)`)
	if err != nil {
		t.Fatalf("json_replace: %v", err)
	}
	if s, ok := rows2.Data[0][0].(string); ok {
		rows3, _ := db.Query(`SELECT json_extract('` + s + `', '$.a')`)
		if rows3 != nil && len(rows3.Data) > 0 {
			v := rows3.Data[0][0]
			if v != float64(99) && v != int64(99) {
				t.Errorf("json_replace $.a = %v, want 99", v)
			}
		}
	}
}
