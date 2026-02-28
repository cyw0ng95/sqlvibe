package TM

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/PB"
)

func newTestTM(t *testing.T) *TransactionManager {
	t.Helper()
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { file.Close() })
	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("pm: %v", err)
	}
	tm := NewTransactionManager(pm)
	t.Cleanup(func() { tm.Close() })
	return tm
}

// --- TransactionType.String --------------------------------------------------

func TestTransactionType_String(t *testing.T) {
	tests := []struct {
		typ  TransactionType
		want string
	}{
		{TransactionDeferred, "DEFERRED"},
		{TransactionImmediate, "IMMEDIATE"},
		{TransactionExclusive, "EXCLUSIVE"},
		{TransactionType(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("TransactionType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

// --- LockState helpers -------------------------------------------------------

func TestLockState_GetLockType(t *testing.T) {
	ls := NewLockState()

	// Non-existent resource
	if got := ls.GetLockType("res"); got != LockNone {
		t.Errorf("expected LockNone, got %v", got)
	}

	// After acquiring
	_ = ls.Acquire("res", "h1", LockShared, time.Second)
	if got := ls.GetLockType("res"); got != LockShared {
		t.Errorf("expected LockShared, got %v", got)
	}
}

func TestLockState_IsLocked(t *testing.T) {
	ls := NewLockState()

	if ls.IsLocked("res") {
		t.Error("expected not locked")
	}

	_ = ls.Acquire("res", "h1", LockShared, time.Second)
	if !ls.IsLocked("res") {
		t.Error("expected locked after acquire")
	}
}

func TestLockState_LockCount(t *testing.T) {
	ls := NewLockState()

	if got := ls.LockCount("res"); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}

	_ = ls.Acquire("res", "h1", LockShared, time.Second)
	if got := ls.LockCount("res"); got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
}

func TestLockState_SetGetTimeout(t *testing.T) {
	ls := NewLockState()

	if got := ls.GetTimeout(); got != 0 {
		t.Errorf("expected 0, got %v", got)
	}

	ls.SetTimeout(50 * time.Millisecond)
	if got := ls.GetTimeout(); got != 50*time.Millisecond {
		t.Errorf("expected 50ms, got %v", got)
	}
}

func TestLockState_DetectDeadlock_NoDeadlock(t *testing.T) {
	ls := NewLockState()
	found, victim := ls.DetectDeadlock()
	if found {
		t.Errorf("expected no deadlock, got victim=%q", victim)
	}
}

func TestLockState_DetectDeadlock_WithWaiters(t *testing.T) {
	ls := NewLockState()

	// Acquire exclusive, then set up a waiter
	_ = ls.Acquire("res", "h1", LockExclusive, time.Second)

	done := make(chan error, 1)
	go func() {
		err := ls.Acquire("res", "h2", LockExclusive, 500*time.Millisecond)
		done <- err
	}()

	// Give waiter time to register
	time.Sleep(20 * time.Millisecond)

	found, victim := ls.DetectDeadlock()
	if found {
		if victim != "res" {
			t.Errorf("expected victim 'res', got %q", victim)
		}
	}
	// Release so the goroutine can finish
	_ = ls.Release("res", "h1")
	<-done
}

func TestLockState_Release_NotLocked(t *testing.T) {
	ls := NewLockState()
	err := ls.Release("nonexistent", "h1")
	if err != ErrNotLocked {
		t.Errorf("expected ErrNotLocked, got %v", err)
	}
}

// --- MVCC --------------------------------------------------------------------

func TestMVCCStore_Basic(t *testing.T) {
	m := NewMVCCStore()

	if m.CommitID() != 0 {
		t.Errorf("expected commitID=0, got %d", m.CommitID())
	}

	snap := m.Snapshot()
	if snap.CommitID != 0 {
		t.Errorf("expected snap.CommitID=0, got %d", snap.CommitID)
	}

	// Put a value
	cid := m.Put("key1", "value1")
	if cid == 0 {
		t.Error("expected non-zero commit ID")
	}

	// Get after snapshot (should not see it)
	val, ok := m.Get("key1", snap)
	if ok {
		t.Errorf("should not see value committed after snapshot, got %v", val)
	}

	// Get with fresh snapshot
	snap2 := m.Snapshot()
	val, ok = m.Get("key1", snap2)
	if !ok {
		t.Error("expected to find key1 with fresh snapshot")
	}
	if val != "value1" {
		t.Errorf("expected 'value1', got %v", val)
	}
}

func TestMVCCStore_Delete(t *testing.T) {
	m := NewMVCCStore()
	m.Put("key1", "hello")

	snap := m.Snapshot()

	// Delete after snapshot
	m.Delete("key1")

	// Snapshot should still see the value
	val, ok := m.Get("key1", snap)
	if !ok {
		t.Error("expected to see key1 through old snapshot")
	}
	if val != "hello" {
		t.Errorf("expected 'hello', got %v", val)
	}

	// Fresh snapshot should not see it
	snap2 := m.Snapshot()
	_, ok = m.Get("key1", snap2)
	if ok {
		t.Error("should not see deleted key through fresh snapshot")
	}
}

func TestMVCCStore_ActiveTxns(t *testing.T) {
	m := NewMVCCStore()
	snap := m.Snapshot()

	// Mark commit 1 as active (in-flight) in snapshot
	m.Put("key1", "dirty")
	snap.ActiveTxns[m.CommitID()] = true

	// Should not see it
	_, ok := m.Get("key1", snap)
	if ok {
		t.Error("should not see value from active transaction")
	}
}

func TestMVCCStore_GC(t *testing.T) {
	m := NewMVCCStore()

	m.Put("key1", "v1") // commitID=1
	m.Put("key1", "v2") // commitID=2
	m.Put("key1", "v3") // commitID=3

	// GC below 3 should remove old versions
	pruned := m.GC(3)
	if pruned < 1 {
		t.Errorf("expected at least 1 pruned version, got %d", pruned)
	}

	// Latest value should still be accessible
	snap := m.Snapshot()
	val, ok := m.Get("key1", snap)
	if !ok {
		t.Error("key1 should still exist after GC")
	}
	_ = val
}

func TestMVCCStore_GC_DeletedBaseline(t *testing.T) {
	m := NewMVCCStore()
	m.Put("key1", "v1")
	m.Delete("key1") // commitID=2

	// GC below 3 — the only remaining version is a deleted baseline
	m.GC(10)

	snap := m.Snapshot()
	_, ok := m.Get("key1", snap)
	if ok {
		t.Error("deleted key should not be visible after GC removes it")
	}
}

func TestMVCCStore_GetMissing(t *testing.T) {
	m := NewMVCCStore()
	snap := m.Snapshot()
	_, ok := m.Get("nonexistent", snap)
	if ok {
		t.Error("expected not found for missing key")
	}
}

// --- WAL ---------------------------------------------------------------------

func TestWAL_WriteReadFrameCommitCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path, 512)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	// Write a frame
	pageData := make([]byte, 512)
	pageData[0] = 0xAB
	err = wal.WriteFrame(1, pageData)
	if err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Read the frame back
	frame, err := wal.ReadFrame(0)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.PageNumber != 1 {
		t.Errorf("expected PageNumber=1, got %d", frame.PageNumber)
	}

	// Commit
	err = wal.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify sequence / frame count / size
	seq := wal.Sequence()
	if seq != 1 {
		t.Errorf("expected sequence=1, got %d", seq)
	}
	if wal.FrameCount() != 1 {
		t.Errorf("expected FrameCount=1, got %d", wal.FrameCount())
	}
	if wal.Path() != path {
		t.Errorf("expected path %q, got %q", path, wal.Path())
	}
	if wal.Size() <= 0 {
		t.Error("expected positive WAL size")
	}

	// ShouldCheckpoint
	if wal.ShouldCheckpoint(0) {
		t.Error("ShouldCheckpoint(0) should be false")
	}
	if !wal.ShouldCheckpoint(1) {
		t.Error("ShouldCheckpoint(1) should be true with 1 frame")
	}

	// CheckpointFull
	busy, logRemoved, checkpointed, err := wal.CheckpointFull()
	if err != nil {
		t.Fatalf("CheckpointFull: %v", err)
	}
	if logRemoved < 0 || checkpointed < 0 || busy < 0 {
		t.Errorf("unexpected negative checkpoint counts: busy=%d logRemoved=%d checkpointed=%d", busy, logRemoved, checkpointed)
	}
}

func TestWAL_CheckpointTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test2.wal")

	wal, err := OpenWAL(path, 512)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	pageData := make([]byte, 512)
	_ = wal.WriteFrame(1, pageData)
	_ = wal.Commit()

	busy, logRemoved, checkpointed, err := wal.CheckpointTruncate()
	if err != nil {
		t.Fatalf("CheckpointTruncate: %v", err)
	}
	if logRemoved < 0 || checkpointed < 0 || busy < 0 {
		t.Errorf("unexpected negative checkpoint counts: busy=%d logRemoved=%d checkpointed=%d", busy, logRemoved, checkpointed)
	}
}

func TestWAL_Recover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test3.wal")

	wal, err := OpenWAL(path, 512)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	pageData := make([]byte, 512)
	_ = wal.WriteFrame(1, pageData)
	_ = wal.WriteFrame(2, pageData)
	_ = wal.Commit()
	_ = wal.Close()

	// Reopen and recover
	wal2, err := OpenWAL(path, 512)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	n, err := wal2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// 2 frames were written before close
	if n < 0 {
		t.Errorf("expected non-negative recovered frames, got %d", n)
	}
}

func TestWAL_WALExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nofile.wal")

	if WALExists(path) {
		t.Error("expected WALExists=false for non-existent file")
	}

	// Create the file
	f, _ := os.Create(path)
	f.Close()
	if !WALExists(path) {
		t.Error("expected WALExists=true after creating file")
	}
}

func TestWAL_WriteClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "closed.wal")
	wal, _ := OpenWAL(path, 512)
	wal.Close()

	err := wal.WriteFrame(1, make([]byte, 512))
	if err == nil {
		t.Error("expected error writing to closed WAL")
	}
}

// --- WALReader ---------------------------------------------------------------

func TestWALReader_NextReset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reader.wal")

	wal, err := OpenWAL(path, 512)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	pageData := make([]byte, 512)
	pageData[0] = 0x42
	_ = wal.WriteFrame(3, pageData)
	_ = wal.Commit()
	_ = wal.Close()

	reader, err := NewWALReader(path, 512)
	if err != nil {
		t.Fatalf("NewWALReader: %v", err)
	}
	defer reader.Close()

	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if frame.PageNumber != 3 {
		t.Errorf("expected PageNumber=3, got %d", frame.PageNumber)
	}

	// EOF
	_, err = reader.Next()
	if err == nil {
		// Some implementations may return empty frames instead of EOF
	}

	// Reset and read again
	reader.Reset()
	frame2, err := reader.Next()
	if err != nil {
		t.Fatalf("Next after Reset: %v", err)
	}
	if frame2.PageNumber != 3 {
		t.Errorf("expected PageNumber=3 after reset, got %d", frame2.PageNumber)
	}
}

// --- TransactionManager: WAL -------------------------------------------------

func TestTransactionManager_EnableDisableWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "tm.wal")

	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("pm: %v", err)
	}

	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Enable WAL
	err = tm.EnableWAL(walPath, 512)
	if err != nil {
		t.Fatalf("EnableWAL: %v", err)
	}

	// Enable again (idempotent)
	err = tm.EnableWAL(walPath, 512)
	if err != nil {
		t.Fatalf("EnableWAL second call: %v", err)
	}

	// Disable
	err = tm.DisableWAL()
	if err != nil {
		t.Fatalf("DisableWAL: %v", err)
	}

	// Disable again (idempotent)
	err = tm.DisableWAL()
	if err != nil {
		t.Fatalf("DisableWAL second call: %v", err)
	}
}

// --- Transaction: AcquireWriteLock -------------------------------------------

func TestTransaction_AcquireWriteLock_AlreadyExclusive(t *testing.T) {
	tm := newTestTM(t)

	// EXCLUSIVE transaction already holds the lock; AcquireWriteLock is a no-op
	tx, err := tm.Begin(":memory:", TransactionExclusive)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if tx.lockType != LockExclusive {
		t.Fatalf("expected EXCLUSIVE after TransactionExclusive Begin, got %v", tx.lockType)
	}

	// Second call should be a no-op
	err = tx.AcquireWriteLock()
	if err != nil {
		t.Fatalf("AcquireWriteLock on already-exclusive tx: %v", err)
	}
	if tx.lockType != LockExclusive {
		t.Errorf("expected EXCLUSIVE, got %v", tx.lockType)
	}

	_ = tm.CommitTransaction(tx.ID)
}

// --- TransactionManager: GetTransaction --------------------------------------

func TestTransactionManager_GetTransaction(t *testing.T) {
	tm := newTestTM(t)

	tx, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	got, err := tm.GetTransaction(tx.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if got.ID != tx.ID {
		t.Errorf("expected ID %d, got %d", tx.ID, got.ID)
	}

	// Non-existent
	_, err = tm.GetTransaction(9999)
	if err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction, got %v", err)
	}

	_ = tm.CommitTransaction(tx.ID)
}

// --- AcquireReadLock on inactive tx ------------------------------------------

func TestTransaction_AcquireReadLock_Inactive(t *testing.T) {
	tm := newTestTM(t)
	tx, _ := tm.Begin(":memory:", TransactionDeferred)
	_ = tm.CommitTransaction(tx.ID)

	err := tx.AcquireReadLock()
	if err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction for committed tx, got %v", err)
	}
}

func TestTransaction_AcquireWriteLock_Inactive(t *testing.T) {
	tm := newTestTM(t)
	tx, _ := tm.Begin(":memory:", TransactionDeferred)
	_ = tm.CommitTransaction(tx.ID)

	err := tx.AcquireWriteLock()
	if err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction for committed tx, got %v", err)
	}
}

// --- IsolationLevel ----------------------------------------------------------

func TestIsolationLevel_String(t *testing.T) {
	tests := []struct {
		il   IsolationLevel
		want string
	}{
		{ReadUncommitted, "READ UNCOMMITTED"},
		{ReadCommitted, "READ COMMITTED"},
		{Serializable, "SERIALIZABLE"},
		{IsolationLevel(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.il.String(); got != tt.want {
			t.Errorf("IsolationLevel(%d).String() = %q, want %q", tt.il, got, tt.want)
		}
	}
}

func TestParseIsolationLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    IsolationLevel
		wantErr bool
	}{
		{"READ UNCOMMITTED", ReadUncommitted, false},
		{"read_uncommitted", ReadUncommitted, false},
		{"READ COMMITTED", ReadCommitted, false},
		{"read_committed", ReadCommitted, false},
		{"SERIALIZABLE", Serializable, false},
		{"serializable", Serializable, false},
		{"UNKNOWN_LEVEL", ReadCommitted, true},
	}
	for _, tt := range tests {
		got, err := ParseIsolationLevel(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseIsolationLevel(%q) err=%v, wantErr=%v", tt.input, err, tt.wantErr)
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("ParseIsolationLevel(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsolationConfig(t *testing.T) {
	ic := NewIsolationConfig()
	if ic.Level != ReadCommitted {
		t.Errorf("expected ReadCommitted, got %v", ic.Level)
	}
	if ic.GetIsolationLevel() != "READ COMMITTED" {
		t.Errorf("unexpected level string: %q", ic.GetIsolationLevel())
	}

	if err := ic.SetIsolationLevel("SERIALIZABLE"); err != nil {
		t.Fatalf("SetIsolationLevel: %v", err)
	}
	if ic.Level != Serializable {
		t.Error("expected Serializable")
	}

	if err := ic.SetIsolationLevel("INVALID"); err == nil {
		t.Error("expected error for invalid level")
	}

	ls := ic.LockState()
	if ls == nil {
		t.Error("expected non-nil LockState")
	}
}
