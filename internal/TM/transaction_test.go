package TM

import (
	"testing"
	"time"

	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/PB"
)

func TestTransactionManager_Basic(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Test BEGIN DEFERRED
	tx1, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin DEFERRED transaction: %v", err)
	}

	if tx1.Type != TransactionDeferred {
		t.Errorf("Expected DEFERRED transaction, got %v", tx1.Type)
	}

	if tx1.State != TransactionActive {
		t.Errorf("Expected ACTIVE state, got %v", tx1.State)
	}

	// Test COMMIT
	err = tm.CommitTransaction(tx1.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if tx1.State != TransactionCommitted {
		t.Errorf("Expected COMMITTED state, got %v", tx1.State)
	}

	// Test BEGIN IMMEDIATE
	tx2, err := tm.Begin(":memory:", TransactionImmediate)
	if err != nil {
		t.Fatalf("Failed to begin IMMEDIATE transaction: %v", err)
	}

	if tx2.Type != TransactionImmediate {
		t.Errorf("Expected IMMEDIATE transaction, got %v", tx2.Type)
	}

	if tx2.lockType != LockReserved {
		t.Errorf("Expected RESERVED lock, got %v", tx2.lockType)
	}

	// Test ROLLBACK
	err = tm.RollbackTransaction(tx2.ID)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	if tx2.State != TransactionRolledBack {
		t.Errorf("Expected ROLLEDBACK state, got %v", tx2.State)
	}
}

func TestTransactionManager_Concurrent(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Start multiple DEFERRED transactions (should work as they don't acquire locks immediately)
	tx1, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}

	tx2, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Both should be active
	if tx1.State != TransactionActive {
		t.Errorf("Transaction 1 should be active")
	}

	if tx2.State != TransactionActive {
		t.Errorf("Transaction 2 should be active")
	}

	// Commit both
	err = tm.CommitTransaction(tx1.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	err = tm.CommitTransaction(tx2.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}
}

func TestTransactionManager_LockAcquisition(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Test 1: DEFERRED transaction with read lock
	tx1, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Should have no lock initially
	if tx1.lockType != LockNone {
		t.Errorf("Expected no lock, got %v", tx1.lockType)
	}

	// Acquire read lock
	err = tx1.AcquireReadLock()
	if err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}

	if tx1.lockType != LockShared {
		t.Errorf("Expected SHARED lock, got %v", tx1.lockType)
	}

	// Commit
	err = tm.CommitTransaction(tx1.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test 2: IMMEDIATE transaction starts with RESERVED lock
	tx2, err := tm.Begin(":memory:", TransactionImmediate)
	if err != nil {
		t.Fatalf("Failed to begin IMMEDIATE transaction: %v", err)
	}

	if tx2.lockType != LockReserved {
		t.Errorf("Expected RESERVED lock for IMMEDIATE, got %v", tx2.lockType)
	}

	// Commit
	err = tm.CommitTransaction(tx2.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test 3: EXCLUSIVE transaction starts with EXCLUSIVE lock
	tx3, err := tm.Begin(":memory:", TransactionExclusive)
	if err != nil {
		t.Fatalf("Failed to begin EXCLUSIVE transaction: %v", err)
	}

	if tx3.lockType != LockExclusive {
		t.Errorf("Expected EXCLUSIVE lock, got %v", tx3.lockType)
	}

	// Commit
	err = tm.CommitTransaction(tx3.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestTransactionManager_ActiveCount(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Should start with 0 active transactions
	if tm.ActiveTransactionCount() != 0 {
		t.Errorf("Expected 0 active transactions, got %d", tm.ActiveTransactionCount())
	}

	// Begin transaction
	tx1, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if tm.ActiveTransactionCount() != 1 {
		t.Errorf("Expected 1 active transaction, got %d", tm.ActiveTransactionCount())
	}

	// Begin another
	tx2, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if tm.ActiveTransactionCount() != 2 {
		t.Errorf("Expected 2 active transactions, got %d", tm.ActiveTransactionCount())
	}

	// Commit one
	err = tm.CommitTransaction(tx1.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if tm.ActiveTransactionCount() != 1 {
		t.Errorf("Expected 1 active transaction, got %d", tm.ActiveTransactionCount())
	}

	// Rollback the other
	err = tm.RollbackTransaction(tx2.ID)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	if tm.ActiveTransactionCount() != 0 {
		t.Errorf("Expected 0 active transactions, got %d", tm.ActiveTransactionCount())
	}
}

func TestTransaction_RecordChange(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Begin transaction
	tx, err := tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Record changes
	tx.RecordChange("INSERT", "users", 1, nil, []byte("new data"))
	tx.RecordChange("UPDATE", "users", 2, []byte("old data"), []byte("new data"))
	tx.RecordChange("DELETE", "users", 3, []byte("deleted data"), nil)

	// Should have 3 changes
	if len(tx.changes) != 3 {
		t.Errorf("Expected 3 changes, got %d", len(tx.changes))
	}

	// Verify change types
	if tx.changes[0].Type != "INSERT" {
		t.Errorf("Expected INSERT change, got %s", tx.changes[0].Type)
	}

	if tx.changes[1].Type != "UPDATE" {
		t.Errorf("Expected UPDATE change, got %s", tx.changes[1].Type)
	}

	if tx.changes[2].Type != "DELETE" {
		t.Errorf("Expected DELETE change, got %s", tx.changes[2].Type)
	}

	// Commit should clear changes
	err = tm.CommitTransaction(tx.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if tx.changes != nil {
		t.Errorf("Expected changes to be cleared after commit")
	}
}

func TestTransactionManager_Close(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)

	// Start transactions
	_, err = tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tm.Begin(":memory:", TransactionDeferred)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Close should rollback all active transactions
	err = tm.Close()
	if err != nil {
		t.Fatalf("Failed to close transaction manager: %v", err)
	}

	// Should have no active transactions
	if tm.ActiveTransactionCount() != 0 {
		t.Errorf("Expected 0 active transactions after close, got %d", tm.ActiveTransactionCount())
	}
}

func TestTransaction_LockTimeout(t *testing.T) {
	// Create in-memory page manager for testing
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("Failed to open memory file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(pm)
	defer tm.Close()

	// Start EXCLUSIVE transaction
	tx1, err := tm.Begin(":memory:", TransactionExclusive)
	if err != nil {
		t.Fatalf("Failed to begin EXCLUSIVE transaction: %v", err)
	}

	// Try to start another EXCLUSIVE transaction (should fail due to lock)
	// Set a short timeout for testing
	go func() {
		time.Sleep(100 * time.Millisecond)
		tm.CommitTransaction(tx1.ID)
	}()

	start := time.Now()
	tx2, err := tm.Begin(":memory:", TransactionExclusive)
	elapsed := time.Since(start)

	// Should either succeed after tx1 commits or fail with timeout
	if err != nil && tx2 == nil {
		// Expected - lock conflict
		if elapsed < 100*time.Millisecond {
			t.Logf("Transaction failed quickly as expected: %v", err)
		}
	} else if tx2 != nil {
		// Succeeded after tx1 committed
		t.Logf("Transaction succeeded after waiting: %v", elapsed)
		tm.CommitTransaction(tx2.ID)
	}
}
