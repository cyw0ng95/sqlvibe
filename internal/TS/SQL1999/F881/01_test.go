package F881

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F881_ShrinkMemory_L1 tests PRAGMA shrink_memory.
func TestSQL1999_F881_ShrinkMemory_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query("PRAGMA shrink_memory")
	if err != nil {
		t.Fatalf("PRAGMA shrink_memory: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	// Data should still be readable after shrink_memory.
	result, err := db.Query("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatalf("SELECT after shrink_memory: %v", err)
	}
	if result.Data[0][0].(int64) != 10 {
		t.Errorf("row count: got %v, want 10", result.Data[0][0])
	}
}

// TestSQL1999_F881_Optimize_L1 tests PRAGMA optimize.
func TestSQL1999_F881_Optimize_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query("PRAGMA optimize")
	if err != nil {
		t.Fatalf("PRAGMA optimize: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0].(string) != "ok" {
		t.Errorf("optimize result: got %v, want 'ok'", rows.Data[0][0])
	}
}

// TestSQL1999_F881_IntegrityCheckOK_L1 tests PRAGMA integrity_check on a valid database.
func TestSQL1999_F881_IntegrityCheckOK_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE good (id INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0].(string) != "ok" {
		t.Errorf("integrity_check: got %v, want 'ok'", rows.Data[0][0])
	}
}

// TestSQL1999_F881_QuickCheckOK_L1 tests PRAGMA quick_check on a valid in-memory database.
func TestSQL1999_F881_QuickCheckOK_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA quick_check")
	if err != nil {
		t.Fatalf("PRAGMA quick_check: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0].(string) != "ok" {
		t.Errorf("quick_check: got %v, want 'ok'", rows.Data[0][0])
	}
}

// TestSQL1999_F881_JournalSizeLimitRead_L1 tests reading PRAGMA journal_size_limit.
func TestSQL1999_F881_JournalSizeLimitRead_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA journal_size_limit")
	if err != nil {
		t.Fatalf("PRAGMA journal_size_limit: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	// Default is -1 (unlimited).
	if rows.Data[0][0].(int64) != -1 {
		t.Errorf("journal_size_limit default: got %v, want -1", rows.Data[0][0])
	}
}

// TestSQL1999_F881_JournalSizeLimitSet_L1 tests setting PRAGMA journal_size_limit.
func TestSQL1999_F881_JournalSizeLimitSet_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA journal_size_limit = 1048576"); err != nil {
		t.Fatalf("set journal_size_limit: %v", err)
	}

	rows, err := db.Query("PRAGMA journal_size_limit")
	if err != nil {
		t.Fatalf("read journal_size_limit: %v", err)
	}
	if rows.Data[0][0].(int64) != 1048576 {
		t.Errorf("journal_size_limit: got %v, want 1048576", rows.Data[0][0])
	}
}

// TestSQL1999_F881_CacheGrind_L1 tests PRAGMA cache_grind returns expected columns.
func TestSQL1999_F881_CacheGrind_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA cache_grind")
	if err != nil {
		t.Fatalf("PRAGMA cache_grind: %v", err)
	}
	expected := []string{"pages_cached", "pages_free", "hits", "misses"}
	if len(rows.Columns) != len(expected) {
		t.Fatalf("columns: got %v, want %v", rows.Columns, expected)
	}
	for i, col := range rows.Columns {
		if col != expected[i] {
			t.Errorf("col[%d]: got %q, want %q", i, col, expected[i])
		}
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
}
