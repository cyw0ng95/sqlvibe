package F870

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F870_Vacuum_L1 tests that VACUUM executes without error.
func TestSQL1999_F870_Vacuum_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'hello')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec("VACUUM"); err != nil {
		t.Fatalf("VACUUM failed: %v", err)
	}

	// Data should still be intact after VACUUM
	rows, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT after VACUUM: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("Expected 1 row after VACUUM, got %d", len(rows.Data))
	}
}

// TestSQL1999_F870_Analyze_L1 tests that ANALYZE executes without error and populates stats.
func TestSQL1999_F870_Analyze_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE items (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO items VALUES (1, 'a')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec("INSERT INTO items VALUES (2, 'b')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec("ANALYZE"); err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	rows, err := db.Query("SELECT * FROM sqlite_stat1")
	if err != nil {
		t.Fatalf("SELECT sqlite_stat1: %v", err)
	}
	if rows == nil {
		t.Fatal("Expected rows from sqlite_stat1")
	}
	// Should have at least one row for items table
	found := false
	for _, row := range rows.Data {
		if row[0] == "items" {
			found = true
		}
	}
	if !found {
		t.Error("Expected 'items' table in sqlite_stat1 after ANALYZE")
	}
}

// TestSQL1999_F870_AnalyzeTarget_L1 tests ANALYZE with a specific table target.
func TestSQL1999_F870_AnalyzeTarget_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE tgt (x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO tgt VALUES (42)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec("ANALYZE tgt"); err != nil {
		t.Fatalf("ANALYZE tgt: %v", err)
	}
}

// TestSQL1999_F870_View_L1 tests basic CREATE VIEW and SELECT from it.
func TestSQL1999_F870_View_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE base (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO base VALUES (1, 'foo'), (2, 'bar')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec("CREATE VIEW v AS SELECT id, val FROM base WHERE id > 1"); err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	rows, err := db.Query("SELECT * FROM v")
	if err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("Expected 1 row from view, got %d", len(rows.Data))
	}
}

// TestSQL1999_F870_DropView_L1 tests DROP VIEW.
func TestSQL1999_F870_DropView_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE src (n INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("CREATE VIEW vdrop AS SELECT n FROM src"); err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}
	if _, err := db.Exec("DROP VIEW vdrop"); err != nil {
		t.Fatalf("DROP VIEW: %v", err)
	}
	if _, err := db.Exec("DROP VIEW IF EXISTS vdrop"); err != nil {
		t.Fatalf("DROP VIEW IF EXISTS: %v", err)
	}
}
