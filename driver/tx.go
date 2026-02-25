package driver

import (
	"database/sql/driver"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Tx implements driver.Tx.
// It uses explicit BEGIN/COMMIT/ROLLBACK SQL statements so that the snapshot-
// based rollback mechanism inside sqlvibe is properly engaged.
type Tx struct {
	db *sqlvibe.Database
}

// Commit commits the transaction.
func (t *Tx) Commit() error {
	_, err := t.db.Exec("COMMIT")
	return err
}

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error {
	_, err := t.db.Exec("ROLLBACK")
	return err
}

// Ensure Tx implements driver.Tx.
var _ driver.Tx = &Tx{}
