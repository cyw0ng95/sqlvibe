package Regression

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestRegression_PragmaIndexInfoMissingIndex_L1 regression test for PRAGMA index_info
// Bug: PRAGMA index_info on non-existent index panicked instead of returning empty
func TestRegression_PragmaIndexInfoMissingIndex_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA index_info('nonexistent_index')")
	if err != nil {
		t.Fatalf("expected no error for missing index, got: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Errorf("expected empty result for missing index, got %d rows", len(rows.Data))
	}
}

// TestRegression_PragmaForeignKeyListNoFK_L1 regression test for PRAGMA foreign_key_list
// Bug: PRAGMA foreign_key_list on table without FKs returned error instead of empty
func TestRegression_PragmaForeignKeyListNoFK_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	rows, err := db.Query("PRAGMA foreign_key_list('t')")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Errorf("expected empty result, got %d rows", len(rows.Data))
	}
}

// TestRegression_SubstrNegativeLengthPanic_L1 regression test for SUBSTR with negative length
// Bug: SUBSTR('hello', 5, -3) panicked with slice bounds out of range
func TestRegression_SubstrNegativeLengthPanic_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// This used to panic
	rows, err := db.Query("SELECT SUBSTR('hello', 5, -3)")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var result string
	rows.Scan(&result)
	if result != "ell" {
		t.Errorf("SUBSTR('hello', 5, -3): got %q, want %q", result, "ell")
	}
}

// TestRegression_InformationSchemaViewsEmpty_L1 regression test for information_schema.views
// Bug: information_schema.views always returned empty even when views existed
func TestRegression_InformationSchemaViewsEmpty_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	db.Exec("CREATE VIEW v AS SELECT id FROM t WHERE val IS NOT NULL")

	rows, err := db.Query("SELECT table_name FROM information_schema.views")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("information_schema.views returned empty despite having a view")
	}
	found := false
	for _, row := range rows.Data {
		if row[0].(string) == "v" {
			found = true
		}
	}
	if !found {
		t.Error("view 'v' not found in information_schema.views")
	}
}

// TestRegression_InformationSchemaTableConstraintsUNIQUE_L1 regression test
// Bug: information_schema.table_constraints only returned PRIMARY KEY, not UNIQUE or FOREIGN KEY
func TestRegression_InformationSchemaTableConstraintsUNIQUE_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")

	rows, err := db.Query("SELECT constraint_type FROM information_schema.table_constraints WHERE table_name='t'")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	types := make(map[string]bool)
	for _, row := range rows.Data {
		types[row[0].(string)] = true
	}
	if !types["PRIMARY KEY"] {
		t.Error("missing PRIMARY KEY constraint")
	}
	if !types["UNIQUE"] {
		t.Error("missing UNIQUE constraint")
	}
}

// TestRegression_SqliteMasterEmptySQL_L1 regression test
// Bug: sqlite_master SQL column showed "CREATE TABLE name ()" with no column definitions
func TestRegression_SqliteMasterEmptySQL_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)")

	rows, err := db.Query("SELECT sql FROM sqlite_master WHERE name='users'")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("no rows returned from sqlite_master")
	}
	sql := rows.Data[0][0].(string)
	// Must have actual column definitions
	if sql == "CREATE TABLE users ()" {
		t.Fatal("sqlite_master SQL should contain column definitions, not empty parens")
	}
	if sql == "" {
		t.Fatal("sqlite_master SQL must not be empty")
	}
}
