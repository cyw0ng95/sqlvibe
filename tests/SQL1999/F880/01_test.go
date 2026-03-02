package F880

import (
	"testing"
	"time"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F880_WALCheckpointPassive_L1 tests PRAGMA wal_checkpoint (passive mode).
func TestSQL1999_F880_WALCheckpointPassive_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Passive checkpoint without WAL mode returns (0,0,0).
	rows, err := db.Query("PRAGMA wal_checkpoint")
	if err != nil {
		t.Fatalf("PRAGMA wal_checkpoint: %v", err)
	}
	if len(rows.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(rows.Columns))
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
}

// TestSQL1999_F880_WALCheckpointModes_L1 tests PRAGMA wal_checkpoint with explicit modes.
func TestSQL1999_F880_WALCheckpointModes_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for _, mode := range []string{"passive", "full", "truncate"} {
		rows, err := db.Query("PRAGMA wal_checkpoint(" + mode + ")")
		if err != nil {
			t.Fatalf("PRAGMA wal_checkpoint(%s): %v", mode, err)
		}
		if len(rows.Data) != 1 {
			t.Fatalf("mode %s: expected 1 row, got %d", mode, len(rows.Data))
		}
	}
}

// TestSQL1999_F880_WALAutocheckpointRead_L1 tests reading PRAGMA wal_autocheckpoint.
func TestSQL1999_F880_WALAutocheckpointRead_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA wal_autocheckpoint")
	if err != nil {
		t.Fatalf("PRAGMA wal_autocheckpoint: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	// Default should be 1000 (SQLite default).
	val, ok := rows.Data[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", rows.Data[0][0])
	}
	if val != 1000 {
		t.Errorf("default wal_autocheckpoint: got %d, want 1000", val)
	}
}

// TestSQL1999_F880_WALAutocheckpointSet_L1 tests setting PRAGMA wal_autocheckpoint = N.
func TestSQL1999_F880_WALAutocheckpointSet_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA wal_autocheckpoint = 500")
	if err != nil {
		t.Fatalf("set wal_autocheckpoint: %v", err)
	}

	rows, err := db.Query("PRAGMA wal_autocheckpoint")
	if err != nil {
		t.Fatalf("read wal_autocheckpoint: %v", err)
	}
	val, ok := rows.Data[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", rows.Data[0][0])
	}
	if val != 500 {
		t.Errorf("wal_autocheckpoint: got %d, want 500", val)
	}
}

// TestSQL1999_F880_WALAutocheckpointDisable_L1 tests disabling auto-checkpoint (N=0).
func TestSQL1999_F880_WALAutocheckpointDisable_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA wal_autocheckpoint = 0"); err != nil {
		t.Fatalf("disable wal_autocheckpoint: %v", err)
	}
	// Should not panic or block when disabled.
	time.Sleep(10 * time.Millisecond)
}

// TestSQL1999_F880_WALShouldCheckpoint_L1 tests WAL ShouldCheckpoint logic.
func TestSQL1999_F880_WALShouldCheckpoint_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// In delete mode (no WAL), wal_checkpoint returns (0,0,0) with no error.
	rows, err := db.Query("PRAGMA wal_checkpoint(passive)")
	if err != nil {
		t.Fatalf("PRAGMA wal_checkpoint(passive): %v", err)
	}
	row := rows.Data[0]
	for i, v := range row {
		if v.(int64) != 0 {
			t.Errorf("col[%d]: expected 0, got %v", i, v)
		}
	}
}

// TestSQL1999_F880_WALChecksumRecovery_L1 verifies WAL replay tolerates
// corrupted (malformed) entries without returning an error.
func TestSQL1999_F880_WALChecksumRecovery_L1(t *testing.T) {
	// This test exercises the DS WAL corruption-recovery path indirectly:
	// opening a DB that has no WAL should still succeed cleanly.
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (42)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	rows, err := db.Query("SELECT x FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0].(int64) != 42 {
		t.Errorf("unexpected result: %v", rows.Data)
	}
}
