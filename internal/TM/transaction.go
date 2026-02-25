package TM

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

var (
	ErrTransactionActive   = errors.New("transaction already active")
	ErrNoTransaction       = errors.New("no transaction active")
	ErrTransactionAborted  = errors.New("transaction aborted")
	ErrTransactionReadOnly = errors.New("transaction is read-only")
)

// TransactionType defines the type of transaction
type TransactionType int

const (
	// DEFERRED is the default - no lock until first read/write
	TransactionDeferred TransactionType = iota
	// IMMEDIATE acquires RESERVED lock at BEGIN
	TransactionImmediate
	// EXCLUSIVE acquires EXCLUSIVE lock at BEGIN
	TransactionExclusive
)

func (t TransactionType) String() string {
	switch t {
	case TransactionDeferred:
		return "DEFERRED"
	case TransactionImmediate:
		return "IMMEDIATE"
	case TransactionExclusive:
		return "EXCLUSIVE"
	default:
		return "UNKNOWN"
	}
}

// TransactionState represents the state of a transaction
type TransactionState int

const (
	TransactionNone TransactionState = iota
	TransactionActive
	TransactionCommitted
	TransactionRolledBack
)

// Transaction represents an active database transaction
type Transaction struct {
	ID        uint64
	Type      TransactionType
	State     TransactionState
	StartTime time.Time
	lockMgr   *LockState
	dbPath    string
	lockType  LockType
	changes   []Change
	wal       *WAL
	pm        *DS.PageManager
}

// Change represents a change made during a transaction
type Change struct {
	Type      string // "INSERT", "UPDATE", "DELETE"
	TableName string
	RowID     uint64
	OldData   []byte
	NewData   []byte
}

// TransactionManager manages database transactions
type TransactionManager struct {
	mu           sync.RWMutex
	lockMgr      *LockState
	nextID       uint64
	transactions map[uint64]*Transaction
	wal          *WAL
	walEnabled   bool
	pm           *DS.PageManager
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(pm *DS.PageManager) *TransactionManager {
	util.AssertNotNil(pm, "PageManager")

	return &TransactionManager{
		lockMgr:      NewLockState(),
		nextID:       1,
		transactions: make(map[uint64]*Transaction),
		wal:          nil,
		walEnabled:   false,
		pm:           pm,
	}
}

// EnableWAL enables Write-Ahead Logging mode
func (tm *TransactionManager) EnableWAL(walPath string, pageSize int) error {
	util.Assert(walPath != "", "WAL path cannot be empty")
	util.Assert(pageSize > 0, "page size must be positive: %d", pageSize)
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.walEnabled && tm.wal != nil {
		return nil
	}

	wal, err := OpenWAL(walPath, pageSize)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %w", err)
	}

	tm.wal = wal
	tm.walEnabled = true
	return nil
}

// DisableWAL disables Write-Ahead Logging mode
func (tm *TransactionManager) DisableWAL() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.walEnabled || tm.wal == nil {
		return nil
	}

	if err := tm.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	tm.wal = nil
	tm.walEnabled = false
	return nil
}

// Begin starts a new transaction
func (tm *TransactionManager) Begin(dbPath string, txType TransactionType) (*Transaction, error) {
	util.Assert(dbPath != "", "database path cannot be empty")
	util.Assert(txType >= TransactionDeferred && txType <= TransactionExclusive, "invalid transaction type: %d", txType)
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Generate transaction ID
	txID := tm.nextID
	tm.nextID++

	// Create transaction
	tx := &Transaction{
		ID:        txID,
		Type:      txType,
		State:     TransactionActive,
		StartTime: time.Now(),
		lockMgr:   tm.lockMgr,
		dbPath:    dbPath,
		lockType:  LockNone,
		changes:   make([]Change, 0),
		wal:       tm.wal,
		pm:        tm.pm,
	}

	// Acquire locks based on transaction type
	holderID := fmt.Sprintf("tx-%d", txID)
	timeout := 5 * time.Second

	switch txType {
	case TransactionDeferred:
		// No lock acquired at BEGIN - will acquire on first operation
		tx.lockType = LockNone

	case TransactionImmediate:
		// Acquire RESERVED lock at BEGIN
		if err := tm.lockMgr.Acquire(dbPath, holderID, LockReserved, timeout); err != nil {
			return nil, fmt.Errorf("failed to acquire RESERVED lock: %w", err)
		}
		tx.lockType = LockReserved

	case TransactionExclusive:
		// Acquire EXCLUSIVE lock at BEGIN
		if err := tm.lockMgr.Acquire(dbPath, holderID, LockExclusive, timeout); err != nil {
			return nil, fmt.Errorf("failed to acquire EXCLUSIVE lock: %w", err)
		}
		tx.lockType = LockExclusive
	}

	// Register transaction
	tm.transactions[txID] = tx

	return tx, nil
}

// AcquireReadLock acquires a shared lock for reading
func (tx *Transaction) AcquireReadLock() error {
	if tx.State != TransactionActive {
		return ErrNoTransaction
	}

	// If we already have a lock, no need to acquire
	if tx.lockType >= LockShared {
		return nil
	}

	holderID := fmt.Sprintf("tx-%d", tx.ID)
	timeout := 5 * time.Second

	if err := tx.lockMgr.Acquire(tx.dbPath, holderID, LockShared, timeout); err != nil {
		return fmt.Errorf("failed to acquire SHARED lock: %w", err)
	}

	tx.lockType = LockShared
	return nil
}

// AcquireWriteLock acquires an exclusive lock for writing
func (tx *Transaction) AcquireWriteLock() error {
	if tx.State != TransactionActive {
		return ErrNoTransaction
	}

	// If we already have exclusive lock, no need to acquire
	if tx.lockType == LockExclusive {
		return nil
	}

	holderID := fmt.Sprintf("tx-%d", tx.ID)
	timeout := 5 * time.Second

	// First acquire RESERVED if we don't have any lock
	if tx.lockType == LockNone || tx.lockType == LockShared {
		if err := tx.lockMgr.Acquire(tx.dbPath, holderID, LockReserved, timeout); err != nil {
			return fmt.Errorf("failed to acquire RESERVED lock: %w", err)
		}
		tx.lockType = LockReserved
	}

	// Then upgrade to EXCLUSIVE
	if err := tx.lockMgr.Acquire(tx.dbPath, holderID, LockExclusive, timeout); err != nil {
		return fmt.Errorf("failed to acquire EXCLUSIVE lock: %w", err)
	}

	tx.lockType = LockExclusive
	return nil
}

// RecordChange records a change made during the transaction
func (tx *Transaction) RecordChange(changeType, tableName string, rowID uint64, oldData, newData []byte) {
	if tx.State != TransactionActive {
		return
	}

	tx.changes = append(tx.changes, Change{
		Type:      changeType,
		TableName: tableName,
		RowID:     rowID,
		OldData:   oldData,
		NewData:   newData,
	})
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	if tx.State != TransactionActive {
		return ErrNoTransaction
	}

	// If WAL is enabled, commit to WAL
	if tx.wal != nil {
		if err := tx.wal.Commit(); err != nil {
			// Attempt rollback on commit failure
			_ = tx.Rollback()
			return fmt.Errorf("failed to commit WAL: %w", err)
		}
	}

	// Release locks
	holderID := fmt.Sprintf("tx-%d", tx.ID)
	if tx.lockType != LockNone {
		if err := tx.lockMgr.Release(tx.dbPath, holderID); err != nil {
			return fmt.Errorf("failed to release lock: %w", err)
		}
	}

	// Update state
	tx.State = TransactionCommitted
	tx.lockType = LockNone
	tx.changes = nil

	return nil
}

// Rollback rolls back the transaction
func (tx *Transaction) Rollback() error {
	if tx.State != TransactionActive {
		return ErrNoTransaction
	}

	// Undo changes in reverse order
	for i := len(tx.changes) - 1; i >= 0; i-- {
		change := tx.changes[i]
		// TODO: Implement actual rollback logic
		// This requires integration with DS layer to restore old data
		_ = change
	}

	// Release locks
	holderID := fmt.Sprintf("tx-%d", tx.ID)
	if tx.lockType != LockNone {
		if err := tx.lockMgr.Release(tx.dbPath, holderID); err != nil {
			return fmt.Errorf("failed to release lock: %w", err)
		}
	}

	// Update state
	tx.State = TransactionRolledBack
	tx.lockType = LockNone
	tx.changes = nil

	return nil
}

// GetTransaction returns an active transaction by ID
func (tm *TransactionManager) GetTransaction(txID uint64) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return nil, ErrNoTransaction
	}

	return tx, nil
}

// CommitTransaction commits a transaction by ID
func (tm *TransactionManager) CommitTransaction(txID uint64) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return ErrNoTransaction
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Remove from active transactions
	delete(tm.transactions, txID)
	return nil
}

// RollbackTransaction rolls back a transaction by ID
func (tm *TransactionManager) RollbackTransaction(txID uint64) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return ErrNoTransaction
	}

	if err := tx.Rollback(); err != nil {
		return err
	}

	// Remove from active transactions
	delete(tm.transactions, txID)
	return nil
}

// ActiveTransactionCount returns the number of active transactions
func (tm *TransactionManager) ActiveTransactionCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.transactions)
}

// Close closes the transaction manager and releases all resources
func (tm *TransactionManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Rollback all active transactions
	for txID := range tm.transactions {
		tx := tm.transactions[txID]
		_ = tx.Rollback()
	}

	// Close WAL if enabled
	if tm.walEnabled && tm.wal != nil {
		if err := tm.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
	}

	tm.transactions = make(map[uint64]*Transaction)
	return nil
}
