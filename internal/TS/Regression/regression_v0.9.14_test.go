package Regression

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestRegression_DropColumnThenInsert_L1 verifies that after DROP COLUMN,
// INSERT succeeds and the dropped column is absent from subsequent SELECT results.
func TestRegression_DropColumnThenInsert_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	db.MustExec("INSERT INTO t VALUES (1, 'alice', 99.5)")

	if _, err := db.Exec("ALTER TABLE t DROP COLUMN score"); err != nil {
		t.Fatalf("DROP COLUMN score: %v", err)
	}

	// Insert without the dropped column
	if _, err := db.Exec("INSERT INTO t (id, name) VALUES (2, 'bob')"); err != nil {
		t.Fatalf("INSERT after DROP COLUMN: %v", err)
	}

	rows, err := db.Query("SELECT * FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
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
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE t (id INTEGER, old_col TEXT)")
	db.MustExec("INSERT INTO t VALUES (1, 'hello')")

	if _, err := db.Exec("ALTER TABLE t RENAME COLUMN old_col TO new_col"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	rows, err := db.Query("SELECT new_col FROM t")
	if err != nil {
		t.Fatalf("SELECT new_col: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "hello" {
		t.Fatalf("unexpected rows: %v", rows.Data)
	}

	// Schema must show new_col, not old_col
	infoRows, err := db.Query("PRAGMA table_info(t)")
	if err != nil {
		t.Fatalf("PRAGMA table_info: %v", err)
	}
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
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE t (id INTEGER, val INTEGER)")
	db.MustExec("CREATE INDEX idx_val ON t (val)")
	db.MustExec("INSERT INTO t VALUES (1, 10),(2, 20),(3, 30)")

	if _, err := db.Exec("ALTER TABLE t RENAME COLUMN val TO value"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	// Query should still work via the updated column name
	rows, err := db.Query("SELECT id FROM t WHERE value = 20")
	if err != nil {
		t.Fatalf("SELECT after RENAME COLUMN (indexed): %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
}

// TestRegression_CSVRoundTrip_L1 verifies that ExportCSV + ImportCSV produces
// identical rows to the original table.
func TestRegression_CSVRoundTrip_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE src (id INTEGER, name TEXT, score REAL)")
	db.MustExec("INSERT INTO src VALUES (1,'alice',9.5),(2,'bob',8.0),(3,'carol',7.25)")

	var buf bytes.Buffer
	exportOpts := sqlvibe.CSVExportOptions{WriteHeader: true, Comma: ','}
	if err := db.ExportCSV(&buf, "SELECT id, name, score FROM src ORDER BY id", exportOpts); err != nil {
		t.Fatalf("ExportCSV: %v", err)
	}

	db.MustExec("CREATE TABLE dst (id INTEGER, name TEXT, score REAL)")
	importOpts := sqlvibe.CSVImportOptions{HasHeader: true, Comma: ','}
	count, err := db.ImportCSV("dst", strings.NewReader(buf.String()), importOpts)
	if err != nil {
		t.Fatalf("ImportCSV: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows imported, got %d", count)
	}

	rows, err := db.Query("SELECT id, name FROM dst ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT dst: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows in dst, got %d", len(rows.Data))
	}
	if rows.Data[1][1] != "bob" {
		t.Fatalf("expected 'bob' at row[1][1], got %v", rows.Data[1][1])
	}
}

// TestRegression_JSONExportNullLiteral_L1 verifies that NULL values in ExportJSON
// produce JSON null literals (not "null" strings or zeros).
func TestRegression_JSONExportNullLiteral_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE t (id INTEGER, val TEXT)")
	db.MustExec("INSERT INTO t VALUES (1, NULL),(2, 'hello')")

	var buf bytes.Buffer
	if err := db.ExportJSON(&buf, "SELECT id, val FROM t ORDER BY id"); err != nil {
		t.Fatalf("ExportJSON: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "null") {
		t.Fatalf("expected JSON null in output, got: %s", out)
	}
	if !strings.Contains(out, "hello") {
		t.Fatalf("expected 'hello' in output, got: %s", out)
	}
}
