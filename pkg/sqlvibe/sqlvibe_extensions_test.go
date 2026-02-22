package sqlvibe

import (
	"testing"
)

// TestSqlvibeExtensionsTable_NoExtensions tests the virtual table with no extensions loaded.
func TestSqlvibeExtensionsTable_NoExtensions(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM sqlvibe_extensions")
	if err != nil {
		t.Fatalf("Query sqlvibe_extensions: %v", err)
	}
	if rows == nil {
		t.Fatal("expected non-nil rows")
	}
	// Columns should always be name, description, functions
	if len(rows.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d: %v", len(rows.Columns), rows.Columns)
	}
}
