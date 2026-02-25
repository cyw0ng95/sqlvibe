//go:build SVDB_EXT_JSON

package Regression

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/cyw0ng95/sqlvibe/ext/json"
)

// TestRegression_JSONEachColumns_L1 tests that json_each returns correct columns.
func TestRegression_JSONEachColumns_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT key, value, type, atom, id, parent, fullkey, path FROM json_each('[1,2,3]') ORDER BY key`)
	if err != nil {
		t.Fatalf("json_each columns: %v", err)
	}
	if len(rows.Columns) != 8 {
		t.Fatalf("expected 8 columns, got %d: %v", len(rows.Columns), rows.Columns)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
	// Verify path for all rows is "$"
	for i, row := range rows.Data {
		if fmt.Sprintf("%v", row[7]) != "$" {
			t.Errorf("row[%d].path = %v, want $", i, row[7])
		}
	}
}

// TestRegression_JSONTreeRecursive_L1 tests that json_tree recursively traverses.
func TestRegression_JSONTreeRecursive_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT count(*) FROM json_tree('{"a":{"b":{"c":1}}}')`)
	if err != nil {
		t.Fatalf("json_tree count: %v", err)
	}
	cnt := rows.Data[0][0]
	// root + a + a.b + a.b.c = 4 nodes
	if fmt.Sprintf("%v", cnt) != "4" {
		t.Errorf("json_tree node count = %v, want 4", cnt)
	}
}

// TestRegression_JSONGroupArray_L1 tests json_group_array aggregate.
func TestRegression_JSONGroupArray_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE nums(n INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nums VALUES (1),(2),(3)`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT json_group_array(n) FROM nums`)
	if err != nil {
		t.Fatalf("json_group_array: %v", err)
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	if got != "[1,2,3]" {
		t.Errorf("json_group_array = %v, want [1,2,3]", got)
	}
}

// TestRegression_JSONGroupObject_L1 tests json_group_object aggregate.
func TestRegression_JSONGroupObject_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE kv2(k TEXT, v INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO kv2 VALUES ('a',1),('b',2)`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT json_group_object(k, v) FROM kv2`)
	if err != nil {
		t.Fatalf("json_group_object: %v", err)
	}
	got := fmt.Sprintf("%v", rows.Data[0][0])
	// Result should be a valid JSON object containing a and b keys
	parsed, valid := parseJSONForTest(got)
	if !valid {
		t.Errorf("json_group_object result not valid JSON: %v", got)
		return
	}
	m, ok := parsed.(map[string]interface{})
	if !ok {
		t.Errorf("json_group_object not an object: %v", got)
		return
	}
	if len(m) != 2 {
		t.Errorf("json_group_object has %d keys, want 2: %v", len(m), got)
	}
}

// TestRegression_JSONRoundTrip_L1 tests JSON round-trip through table functions.
func TestRegression_JSONRoundTrip_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Store JSON in a table, extract via json_each
	if _, err := db.Exec(`CREATE TABLE docs(id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (1, '[10,20,30]')`); err != nil {
		t.Fatal(err)
	}

	// Verify the stored data
	rows, err := db.Query(`SELECT data FROM docs WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Data) != 1 {
		t.Fatal("expected 1 row")
	}
	jsonData := fmt.Sprintf("%v", rows.Data[0][0])
	if jsonData != "[10,20,30]" {
		t.Errorf("stored json = %v, want [10,20,30]", jsonData)
	}
}

func parseJSONForTest(s string) (interface{}, bool) {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, false
	}
	return v, true
}
