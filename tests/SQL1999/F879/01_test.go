package F879

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F879_PragmaIndexInfo tests PRAGMA index_info
func TestSQL1999_F879_PragmaIndexInfo_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_a ON t (a)"); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_b ON t (b)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	rows, err := db.Query("PRAGMA index_info('idx_a')")
	if err != nil {
		t.Fatalf("PRAGMA index_info: %v", err)
	}
	expected := []string{"seqno", "cid", "name"}
	for i, col := range rows.Columns {
		if col != expected[i] {
			t.Errorf("col[%d]: got %q, want %q", i, col, expected[i])
		}
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	row := rows.Data[0]
	if row[0].(int64) != 0 {
		t.Errorf("seqno: got %v, want 0", row[0])
	}
	if row[1].(int64) != 1 {
		t.Errorf("cid: got %v, want 1 (column 'a' is at index 1)", row[1])
	}
	if row[2].(string) != "a" {
		t.Errorf("name: got %v, want 'a'", row[2])
	}
}

// TestSQL1999_F879_PragmaForeignKeyList tests PRAGMA foreign_key_list
func TestSQL1999_F879_PragmaForeignKeyList_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE)")

	rows, err := db.Query("PRAGMA foreign_key_list('child')")
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_list: %v", err)
	}

	if len(rows.Data) == 0 {
		t.Fatal("expected at least 1 row from foreign_key_list")
	}

	row := rows.Data[0]
	// id, seq, table, from, to, on_update, on_delete, match
	if row[2].(string) != "parent" {
		t.Errorf("table: got %v, want 'parent'", row[2])
	}
	if row[3].(string) != "parent_id" {
		t.Errorf("from: got %v, want 'parent_id'", row[3])
	}
	if row[4].(string) != "id" {
		t.Errorf("to: got %v, want 'id'", row[4])
	}
	if row[6].(string) != "CASCADE" {
		t.Errorf("on_delete: got %v, want 'CASCADE'", row[6])
	}
}

// TestSQL1999_F879_PragmaForeignKeyListEmpty tests PRAGMA foreign_key_list on table with no FKs
func TestSQL1999_F879_PragmaForeignKeyListEmpty_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE standalone (id INTEGER PRIMARY KEY)")

	rows, err := db.Query("PRAGMA foreign_key_list('standalone')")
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_list: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Errorf("expected empty result, got %d rows", len(rows.Data))
	}
}

// TestSQL1999_F879_PragmaFunctionList tests PRAGMA function_list
func TestSQL1999_F879_PragmaFunctionList_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA function_list")
	if err != nil {
		t.Fatalf("PRAGMA function_list: %v", err)
	}
	if len(rows.Columns) == 0 {
		t.Fatal("expected columns in function_list")
	}
	if rows.Columns[0] != "name" {
		t.Errorf("first column: got %q, want 'name'", rows.Columns[0])
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected at least one function in function_list")
	}

	// Check some expected functions are present
	funcNames := make(map[string]bool)
	for _, row := range rows.Data {
		if name, ok := row[0].(string); ok {
			funcNames[name] = true
		}
	}
	for _, f := range []string{"abs", "substr", "length", "upper", "lower", "coalesce", "typeof"} {
		if !funcNames[f] {
			t.Errorf("expected function %q in function_list", f)
		}
	}
}

// TestSQL1999_F879_InformationSchemaViews tests information_schema.views
func TestSQL1999_F879_InformationSchemaViews_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, salary REAL)")
	db.Exec("CREATE VIEW emp_view AS SELECT id, name FROM emp WHERE salary > 50000")

	rows, err := db.Query("SELECT table_name, view_definition FROM information_schema.views")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected at least 1 view in information_schema.views")
	}

	found := false
	for _, row := range rows.Data {
		if row[0].(string) == "emp_view" {
			found = true
			if row[1] == nil {
				t.Error("view_definition should not be nil")
			}
		}
	}
	if !found {
		t.Error("emp_view not found in information_schema.views")
	}
}

// TestSQL1999_F879_InformationSchemaTableConstraints tests information_schema.table_constraints
func TestSQL1999_F879_InformationSchemaTableConstraints_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	db.Exec("CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES dept(id))")

	rows, err := db.Query("SELECT constraint_type, table_name FROM information_schema.table_constraints ORDER BY constraint_type, table_name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	types := make(map[string]bool)
	for _, row := range rows.Data {
		types[row[0].(string)] = true
	}
	if !types["PRIMARY KEY"] {
		t.Error("expected PRIMARY KEY in table_constraints")
	}
	if !types["FOREIGN KEY"] {
		t.Error("expected FOREIGN KEY in table_constraints")
	}
	if !types["UNIQUE"] {
		t.Error("expected UNIQUE in table_constraints")
	}
}

// TestSQL1999_F879_InformationSchemaReferentialConstraints tests referential_constraints
func TestSQL1999_F879_InformationSchemaReferentialConstraints_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	db.Exec("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id))")

	rows, err := db.Query("SELECT * FROM information_schema.referential_constraints")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected at least 1 referential constraint")
	}
	row := rows.Data[0]
	if row[0] == nil || row[0].(string) == "" {
		t.Error("constraint_name should not be empty")
	}
}

// TestSQL1999_F879_SqliteMasterSQL tests that sqlite_master returns proper SQL
func TestSQL1999_F879_SqliteMasterSQL_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")

	rows, err := db.Query("SELECT sql FROM sqlite_master WHERE type='table' AND name='users'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected row for users table")
	}
	sql := rows.Data[0][0].(string)
	if sql == "" {
		t.Fatal("sql should not be empty")
	}
	// Should contain column definitions
	if !containsAll(sql, "id", "name", "age") {
		t.Errorf("sql should contain column names, got: %q", sql)
	}
}

// TestSQL1999_F879_SubstrNegativeLength tests SUBSTR with negative length
func TestSQL1999_F879_SubstrNegativeLength_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"neg_len_3", "SELECT SUBSTR('hello', 5, -3)", "ell"},
		{"neg_len_1", "SELECT SUBSTR('hello', 3, -1)", "e"},
		{"neg_len_exceed", "SELECT SUBSTR('hello', 3, -10)", "he"},
		{"neg_len_at_start", "SELECT SUBSTR('hello', 1, -1)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query error: %v", err)
			}
			if !rows.Next() {
				t.Fatal("no rows returned")
			}
			var got string
			rows.Scan(&got)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSQL1999_F879_InformationSchemaColumns tests information_schema.columns nullable tracking
func TestSQL1999_F879_InformationSchemaColumnsNullable_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")

	rows, err := db.Query("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name='t' ORDER BY column_name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	colNullable := make(map[string]string)
	for _, row := range rows.Data {
		colNullable[row[0].(string)] = row[1].(string)
	}
	if colNullable["id"] != "NO" {
		t.Errorf("id should be NOT NULL (PK), got %q", colNullable["id"])
	}
	if colNullable["name"] != "NO" {
		t.Errorf("name has NOT NULL constraint, got %q", colNullable["name"])
	}
	if colNullable["age"] != "YES" {
		t.Errorf("age should be nullable, got %q", colNullable["age"])
	}
}

// containsAll checks if s contains all the given substrings.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
