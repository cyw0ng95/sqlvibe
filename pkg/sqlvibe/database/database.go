// Package database provides higher-level access helpers and sub-operation
// interfaces built on top of [sqlvibe.Database].  It organises database
// operations into DDL, DML, query, transaction, prepare, meta and constraint
// concerns, making each area independently testable.
package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// DB is a thin wrapper around [sqlvibe.Database] that exposes operation-
// specific sub-interfaces.  All methods delegate to the underlying database.
type DB struct {
	db *sqlvibe.Database
}

// New creates a DB wrapper around an existing [sqlvibe.Database].
func New(db *sqlvibe.Database) *DB {
	return &DB{db: db}
}

// Open is a convenience function that opens a new database at the given path
// and wraps it in a DB.
func Open(path string) (*DB, error) {
	db, err := sqlvibe.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

// Unwrap returns the underlying [sqlvibe.Database].
func (d *DB) Unwrap() *sqlvibe.Database {
	return d.db
}

// Close closes the underlying database.
func (d *DB) Close() error {
	return d.db.Close()
}

// DDL returns a DDL helper for this database.
func (d *DB) DDL() *DDL { return &DDL{db: d.db} }

// DML returns a DML helper for this database.
func (d *DB) DML() *DML { return &DML{db: d.db} }

// Query returns a Query helper for this database.
func (d *DB) Query() *Query { return &Query{db: d.db} }

// Txn returns a Txn helper for this database.
func (d *DB) Txn() *Txn { return &Txn{db: d.db} }

// Prepare returns a Prepare helper for this database.
func (d *DB) Prepare() *Prepare { return &Prepare{db: d.db} }

// Meta returns a Meta helper for this database.
func (d *DB) Meta() *Meta { return &Meta{db: d.db} }

// Constraint returns a Constraint helper for this database.
func (d *DB) Constraint() *Constraint { return &Constraint{db: d.db} }
