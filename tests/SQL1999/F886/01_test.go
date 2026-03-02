//go:build SVDB_EXT_JSON

package F886

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/cyw0ng95/sqlvibe/ext/json"
)

func openDB(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// TestSQL1999_F886_JSONEach_L1 tests json_each() table function on JSON array.
func TestSQL1999_F886_JSONEach_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT key, value, type FROM json_each('[1,"two",3]') ORDER BY key`)
	if err != nil {
		t.Fatalf("json_each array: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
	// First row: key=0, value=1, type=integer
	if fmt.Sprintf("%v", rows.Data[0][0]) != "0" {
		t.Errorf("row[0].key = %v, want 0", rows.Data[0][0])
	}
}

// TestSQL1999_F886_JSONEachObject_L1 tests json_each() on JSON object.
func TestSQL1999_F886_JSONEachObject_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT key, value FROM json_each('{"a":1,"b":2}') ORDER BY key`)
	if err != nil {
		t.Fatalf("json_each object: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Data))
	}
}

// TestSQL1999_F886_JSONTree_L1 tests json_tree() recursive traversal.
func TestSQL1999_F886_JSONTree_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT fullkey, type FROM json_tree('{"a":{"b":1},"c":[2,3]}') ORDER BY id`)
	if err != nil {
		t.Fatalf("json_tree: %v", err)
	}
	// Root + a + a.b + c + c[0] + c[1] = 6 rows
	if len(rows.Data) < 4 {
		t.Fatalf("expected at least 4 rows, got %d: %v", len(rows.Data), rows.Data)
	}
	// First row is root "$"
	if fmt.Sprintf("%v", rows.Data[0][0]) != "$" {
		t.Errorf("root fullkey = %v, want $", rows.Data[0][0])
	}
}

// TestSQL1999_F886_JSONGroupArray_L1 tests json_group_array() aggregate.
func TestSQL1999_F886_JSONGroupArray_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t(id INT, val TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1,'a'),(1,'b'),(2,'c')`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT id, json_group_array(val) FROM t GROUP BY id ORDER BY id`)
	if err != nil {
		t.Fatalf("json_group_array: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Data))
	}
	// Group id=1 should give ["a","b"]
	got := fmt.Sprintf("%v", rows.Data[0][1])
	if got != `["a","b"]` {
		t.Errorf("json_group_array for id=1 = %v, want [\"a\",\"b\"]", got)
	}
}

// TestSQL1999_F886_JSONGroupObject_L1 tests json_group_object() aggregate.
func TestSQL1999_F886_JSONGroupObject_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE kv(k TEXT, v INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO kv VALUES ('x',1),('y',2)`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT json_group_object(k, v) FROM kv`)
	if err != nil {
		t.Fatalf("json_group_object: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	result := fmt.Sprintf("%v", rows.Data[0][0])
	if result == "" || result == "{}" {
		t.Errorf("json_group_object returned empty: %v", result)
	}
}

// TestSQL1999_F886_JSONB_L1 tests jsonb() function.
func TestSQL1999_F886_JSONB_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT jsonb('{"a":1}')`)
	if err != nil {
		t.Fatalf("jsonb: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] == nil {
		t.Fatalf("jsonb returned nil or no rows")
	}
}

// TestSQL1999_F886_JSONPretty_L1 tests json_pretty() function.
func TestSQL1999_F886_JSONPretty_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT json_pretty('{"a":1,"b":2}')`)
	if err != nil {
		t.Fatalf("json_pretty: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got == `{"a":1,"b":2}` {
		t.Error("json_pretty should produce multi-line output")
	}
	if got == "" {
		t.Error("json_pretty returned empty")
	}
}

// TestSQL1999_F886_JSONPatch_L1 tests json_patch() RFC 7396 MergePatch.
func TestSQL1999_F886_JSONPatch_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT json_patch('{"a":1,"b":2}','{"b":99,"c":3}')`)
	if err != nil {
		t.Fatalf("json_patch: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got == "" {
		t.Error("json_patch returned empty")
	}
}

// TestSQL1999_F886_JSONArrayInsert_L1 tests json_array_insert() function.
func TestSQL1999_F886_JSONArrayInsert_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT json_array_insert('[1,2,3]','$[1]',99)`)
	if err != nil {
		t.Fatalf("json_array_insert: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got != "[1,99,2,3]" {
		t.Errorf("json_array_insert = %v, want [1,99,2,3]", got)
	}
}

// TestSQL1999_F886_JSONArrowOp_L1 tests -> and ->> operators.
func TestSQL1999_F886_JSONArrowOp_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// -> extracts JSON sub-element
	rows, err := db.Query(`SELECT '{"a":1}' -> '$.a'`)
	if err != nil {
		t.Fatalf("-> operator: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("-> returned no rows")
	}

	// ->> extracts as text
	rows2, err := db.Query(`SELECT '{"a":"hello"}' ->> '$.a'`)
	if err != nil {
		t.Fatalf("->> operator: %v", err)
	}
	if len(rows2.Data) == 0 {
		t.Fatal("->> returned no rows")
	}
	if fmt.Sprintf("%v", rows2.Data[0][0]) != "hello" {
		t.Errorf("->> = %v, want hello", rows2.Data[0][0])
	}
}

// TestSQL1999_F886_JSONbEach_L1 tests jsonb_each() table function.
func TestSQL1999_F886_JSONbEach_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT key FROM jsonb_each('[10,20,30]') ORDER BY key`)
	if err != nil {
		t.Fatalf("jsonb_each: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
}

// TestSQL1999_F886_JSONbTree_L1 tests jsonb_tree() table function.
func TestSQL1999_F886_JSONbTree_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT fullkey FROM jsonb_tree('[1,2]') ORDER BY id`)
	if err != nil {
		t.Fatalf("jsonb_tree: %v", err)
	}
	if len(rows.Data) < 3 {
		t.Fatalf("expected at least 3 rows (root + 2 elements), got %d", len(rows.Data))
	}
}
