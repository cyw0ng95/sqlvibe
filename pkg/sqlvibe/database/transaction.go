package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Txn provides transaction management helpers.
type Txn struct {
	db *sqlvibe.Database
}

// Begin starts an explicit transaction.
func (t *Txn) Begin() (*sqlvibe.Transaction, error) {
	return t.db.Begin()
}

// Exec executes a transaction-related statement (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE).
func (t *Txn) Exec(sql string) (sqlvibe.Result, error) {
	return t.db.Exec(sql)
}

// Savepoint creates a named savepoint.
func (t *Txn) Savepoint(name string) (sqlvibe.Result, error) {
	return t.db.Exec("SAVEPOINT " + sanitizeIdent(name))
}

// ReleaseSavepoint releases a named savepoint.
func (t *Txn) ReleaseSavepoint(name string) (sqlvibe.Result, error) {
	return t.db.Exec("RELEASE SAVEPOINT " + sanitizeIdent(name))
}

// RollbackToSavepoint rolls back to a named savepoint.
func (t *Txn) RollbackToSavepoint(name string) (sqlvibe.Result, error) {
	return t.db.Exec("ROLLBACK TO SAVEPOINT " + sanitizeIdent(name))
}
