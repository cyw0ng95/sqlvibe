package Regression

import (
	"database/sql"

	_ "github.com/cyw0ng95/sqlvibe/driver"
	"testing"

)

// TestRegression_DropColumnThenInsert_L1 verifies that after DROP COLUMN,
// INSERT succeeds and the dropped column is absent from subsequent SELECT results.
func TestRegression_DropColumnThenInsert_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'alice', 99.5)")

	if _, err := db.Exec("ALTER TABLE t DROP COLUMN score"); err != nil {
		t.Fatalf("DROP COLUMN score: %v", err)
	}

	// Insert without the dropped column
	if _, err := db.Exec("INSERT INTO t (id, name) VALUES (2, 'bob')"); err != nil {
		t.Fatalf("INSERT after DROP COLUMN: %v", err)
	}

	rows := qDB(t, db, "SELECT * FROM t ORDER BY id")
	if len(rows.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(rows.Columns), rows.Columns)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Data))
	}
	for _, col := range rows.Columns {
		if col == "score" {
			t.Fatal("dropped column 'score' still visible")
		}
	}
}

// TestRegression_RenameColumnReflectedInSelect_L1 verifies that after RENAME COLUMN
// the new name is usable in SELECT and returns the correct data.
func TestRegression_RenameColumnReflectedInSelect_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, old_col TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'hello')")

	if _, err := db.Exec("ALTER TABLE t RENAME COLUMN old_col TO new_col"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	rows := qDB(t, db, "SELECT new_col FROM t")
	if len(rows.Data) != 1 || rows.Data[0][0] != "hello" {
		t.Fatalf("unexpected rows: %v", rows.Data)
	}

	// Schema must show new_col, not old_col
	infoRows := qDB(t, db, "PRAGMA table_info(t)")
	colNames := make(map[string]bool)
	for _, row := range infoRows.Data {
		if len(row) > 1 {
			if name, ok := row[1].(string); ok {
				colNames[name] = true
			}
		}
	}
	if !colNames["new_col"] {
		t.Fatal("new_col not found in PRAGMA table_info after RENAME COLUMN")
	}
	if colNames["old_col"] {
		t.Fatal("old_col still present in PRAGMA table_info after RENAME COLUMN")
	}
}

// TestRegression_RenameColumnUpdatesIndex_L1 verifies that an index referencing
// the renamed column is updated so subsequent queries still use it.
func TestRegression_RenameColumnUpdatesIndex_L1(t *testing.T) {
	db, err := sql.Open("sqlvibe", ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	mustExec(t, db, "CREATE INDEX idx_val ON t (val)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 10),(2, 20),(3, 30)")

	if _, err := db.Exec("ALTER TABLE t RENAME COLUMN val TO value"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	// Query should still work via the updated column name
	rows := qDB(t, db, "SELECT id FROM t WHERE value = 20")
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
}

// TestRegression_CSVRoundTrip_L1 - ExportCSV/ImportCSV not available via driver interface.
func TestRegression_CSVRoundTrip_L1(t *testing.T) {
	t.Skip("ExportCSV/ImportCSV not available via database/sql driver interface")
}

// TestRegression_JSONExportNullLiteral_L1 - ExportJSON not available via driver interface.
func TestRegression_JSONExportNullLiteral_L1(t *testing.T) {
	t.Skip("ExportJSON not available via database/sql driver interface")
}
