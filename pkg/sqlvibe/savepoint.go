package sqlvibe

import "fmt"

// handleSavepoint creates a named savepoint within the current transaction.
// If no explicit transaction is active, an implicit one is started.
func (db *Database) handleSavepoint(name string) (Result, error) {
	if db.activeTx == nil && db.txSnapshot == nil {
		// Implicit transaction: take a baseline snapshot
		db.txSnapshot = db.captureSnapshot()
	}
	// Push a new savepoint snapshot
	db.savepointStack = append(db.savepointStack, savepointEntry{
		name:     name,
		snapshot: db.captureSnapshot(),
	})
	return Result{}, nil
}

// handleReleaseSavepoint releases (removes) the named savepoint and all
// savepoints that were set after it. The data changes are kept.
func (db *Database) handleReleaseSavepoint(name string) (Result, error) {
	idx := db.findSavepoint(name)
	if idx < 0 {
		return Result{}, fmt.Errorf("no such savepoint: %s", name)
	}
	db.savepointStack = db.savepointStack[:idx]
	return Result{}, nil
}

// handleRollbackToSavepoint reverts to the named savepoint but keeps it on
// the stack so it can be rolled back to again.
func (db *Database) handleRollbackToSavepoint(name string) (Result, error) {
	idx := db.findSavepoint(name)
	if idx < 0 {
		return Result{}, fmt.Errorf("no such savepoint: %s", name)
	}
	snap := db.savepointStack[idx].snapshot
	// Restore to that savepoint's state
	db.restoreSnapshot(snap)
	// Remove all savepoints added AFTER this one (they are no longer valid),
	// but keep this savepoint itself with a fresh copy of the snapshot.
	db.savepointStack = db.savepointStack[:idx+1]
	db.savepointStack[idx].snapshot = db.captureSnapshot()
	return Result{}, nil
}

// findSavepoint returns the index of the most recent savepoint with the given
// name, or -1 if not found.
func (db *Database) findSavepoint(name string) int {
	for i := len(db.savepointStack) - 1; i >= 0; i-- {
		if db.savepointStack[i].name == name {
			return i
		}
	}
	return -1
}
