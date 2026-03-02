package F871

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	return db
}

// TestSQL1999_F871_PageSize_L1 tests PRAGMA page_size get and set.
func TestSQL1999_F871_PageSize_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("PRAGMA page_size")
	if err != nil {
		t.Fatalf("PRAGMA page_size: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("Expected row from PRAGMA page_size")
	}

	if _, err := db.Exec("PRAGMA page_size = 8192"); err != nil {
		t.Fatalf("PRAGMA page_size = 8192: %v", err)
	}

	rows, err = db.Query("PRAGMA page_size")
	if err != nil {
		t.Fatalf("PRAGMA page_size after set: %v", err)
	}
	if rows.Data[0][0] != int64(8192) {
		t.Errorf("Expected page_size 8192, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_MmapSize_L1 tests PRAGMA mmap_size get and set.
func TestSQL1999_F871_MmapSize_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("PRAGMA mmap_size = 1048576"); err != nil {
		t.Fatalf("PRAGMA mmap_size = 1048576: %v", err)
	}

	rows, err := db.Query("PRAGMA mmap_size")
	if err != nil {
		t.Fatalf("PRAGMA mmap_size: %v", err)
	}
	if rows.Data[0][0] != int64(1048576) {
		t.Errorf("Expected mmap_size 1048576, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_LockingMode_L1 tests PRAGMA locking_mode.
func TestSQL1999_F871_LockingMode_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("PRAGMA locking_mode")
	if err != nil {
		t.Fatalf("PRAGMA locking_mode: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("Expected row")
	}

	if _, err := db.Exec("PRAGMA locking_mode = EXCLUSIVE"); err != nil {
		t.Fatalf("PRAGMA locking_mode=EXCLUSIVE: %v", err)
	}

	rows, err = db.Query("PRAGMA locking_mode")
	if err != nil {
		t.Fatalf("PRAGMA locking_mode after set: %v", err)
	}
	if rows.Data[0][0] != "exclusive" {
		t.Errorf("Expected exclusive, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_Synchronous_L1 tests PRAGMA synchronous.
func TestSQL1999_F871_Synchronous_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("PRAGMA synchronous")
	if err != nil {
		t.Fatalf("PRAGMA synchronous: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("Expected row")
	}
	// Default is 2
	if rows.Data[0][0] != int64(2) {
		t.Errorf("Expected default synchronous=2, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_AutoVacuum_L1 tests PRAGMA auto_vacuum.
func TestSQL1999_F871_AutoVacuum_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("PRAGMA auto_vacuum = 1"); err != nil {
		t.Fatalf("PRAGMA auto_vacuum=1: %v", err)
	}
	rows, err := db.Query("PRAGMA auto_vacuum")
	if err != nil {
		t.Fatalf("PRAGMA auto_vacuum: %v", err)
	}
	if rows.Data[0][0] != int64(1) {
		t.Errorf("Expected 1, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_QueryOnly_L1 tests PRAGMA query_only.
func TestSQL1999_F871_QueryOnly_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("PRAGMA query_only = ON"); err != nil {
		t.Fatalf("PRAGMA query_only=ON: %v", err)
	}
	rows, err := db.Query("PRAGMA query_only")
	if err != nil {
		t.Fatalf("PRAGMA query_only: %v", err)
	}
	if rows.Data[0][0] != int64(1) {
		t.Errorf("Expected 1, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_TempStore_L1 tests PRAGMA temp_store.
func TestSQL1999_F871_TempStore_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("PRAGMA temp_store = 2"); err != nil {
		t.Fatalf("PRAGMA temp_store=2: %v", err)
	}
	rows, err := db.Query("PRAGMA temp_store")
	if err != nil {
		t.Fatalf("PRAGMA temp_store: %v", err)
	}
	if rows.Data[0][0] != int64(2) {
		t.Errorf("Expected 2, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_ReadUncommitted_L1 tests PRAGMA read_uncommitted.
func TestSQL1999_F871_ReadUncommitted_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("PRAGMA read_uncommitted = 1"); err != nil {
		t.Fatalf("PRAGMA read_uncommitted=1: %v", err)
	}
	rows, err := db.Query("PRAGMA read_uncommitted")
	if err != nil {
		t.Fatalf("PRAGMA read_uncommitted: %v", err)
	}
	if rows.Data[0][0] != int64(1) {
		t.Errorf("Expected 1, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F871_CacheSpill_L1 tests PRAGMA cache_spill.
func TestSQL1999_F871_CacheSpill_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("PRAGMA cache_spill")
	if err != nil {
		t.Fatalf("PRAGMA cache_spill: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("Expected row")
	}
	// Default is 1
	if rows.Data[0][0] != int64(1) {
		t.Errorf("Expected default cache_spill=1, got %v", rows.Data[0][0])
	}

	if _, err := db.Exec("PRAGMA cache_spill = 0"); err != nil {
		t.Fatalf("PRAGMA cache_spill=0: %v", err)
	}
	rows, err = db.Query("PRAGMA cache_spill")
	if err != nil {
		t.Fatalf("PRAGMA cache_spill after set: %v", err)
	}
	if rows.Data[0][0] != int64(0) {
		t.Errorf("Expected 0, got %v", rows.Data[0][0])
	}
}
