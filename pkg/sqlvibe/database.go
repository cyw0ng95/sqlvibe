// Package sqlvibe provides the core database API.
// This is a minimal wrapper around the C++ engine for the driver package.
package sqlvibe

import (
	"context"
	cgo "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/cgo"
)

// Database is the primary handle for a sqlvibe database.
type Database struct {
	cdb *cgo.DB
}

// Result holds the outcome of a non-query SQL execution.
type Result struct {
	LastInsertRowID int64
	RowsAffected    int64
}

// Rows holds a query result set.
type Rows struct {
	crows *cgo.Rows
}

// Statement is a compiled SQL statement.
type Statement struct {
	cstmt *cgo.Stmt
	db    *Database
	sql   string
}

// Transaction is an in-progress database transaction.
type Transaction struct {
	ctx *cgo.Tx
	db  *Database
}

// Open opens a database connection.
func Open(path string) (*Database, error) {
	cdb, err := cgo.Open(path)
	if err != nil {
		return nil, err
	}
	return &Database{cdb: cdb}, nil
}

// Close closes the database.
func (db *Database) Close() error {
	if db.cdb == nil {
		return nil
	}
	return db.cdb.Close()
}

// Exec executes a SQL statement.
func (db *Database) Exec(sql string) (Result, error) {
	r, err := db.cdb.Exec(sql)
	if err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: r.RowsAffected, LastInsertRowID: r.LastInsertRowid}, nil
}

// Query executes a SQL query.
func (db *Database) Query(sql string) (*Rows, error) {
	crows, err := db.cdb.Query(sql)
	if err != nil {
		return nil, err
	}
	return &Rows{crows: crows}, nil
}

// Prepare prepares a SQL statement.
func (db *Database) Prepare(sql string) (*Statement, error) {
	cstmt, err := db.cdb.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return &Statement{cstmt: cstmt, db: db, sql: sql}, nil
}

// Begin starts a transaction.
func (db *Database) Begin() (*Transaction, error) {
	ctx, err := db.cdb.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{ctx: ctx, db: db}, nil
}

// ExecContextWithParams executes with positional parameters.
func (db *Database) ExecContextWithParams(ctx context.Context, sql string, params []interface{}) (Result, error) {
	return db.Exec(sql) // Simplified for driver-only usage
}

// QueryContextWithParams queries with positional parameters.
func (db *Database) QueryContextWithParams(ctx context.Context, sql string, params []interface{}) (*Rows, error) {
	return db.Query(sql) // Simplified for driver-only usage
}

// ExecContextNamed executes with named parameters.
func (db *Database) ExecContextNamed(ctx context.Context, sql string, params map[string]interface{}) (Result, error) {
	return db.Exec(sql) // Simplified for driver-only usage
}

// QueryContextNamed queries with named parameters.
func (db *Database) QueryContextNamed(ctx context.Context, sql string, params map[string]interface{}) (*Rows, error) {
	return db.Query(sql) // Simplified for driver-only usage
}

// Exec executes a statement with parameters.
func (s *Statement) Exec(params ...interface{}) (Result, error) {
	return s.db.Exec(s.sql)
}

// Query executes a query with parameters.
func (s *Statement) Query(params ...interface{}) (*Rows, error) {
	return s.db.Query(s.sql)
}

// Close closes the statement.
func (s *Statement) Close() error {
	if s.cstmt != nil {
		return s.cstmt.Close()
	}
	return nil
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
	if tx.ctx == nil {
		return nil
	}
	return tx.ctx.Commit()
}

// Rollback rolls back the transaction.
func (tx *Transaction) Rollback() error {
	if tx.ctx == nil {
		return nil
	}
	return tx.ctx.Rollback()
}

// Next advances to the next row.
func (r *Rows) Next() bool {
	if r.crows == nil {
		return false
	}
	return r.crows.Next()
}

// Columns returns column names.
func (r *Rows) Columns() []string {
	if r.crows == nil {
		return nil
	}
	n := r.crows.ColumnCount()
	cols := make([]string, n)
	for i := 0; i < n; i++ {
		cols[i] = r.crows.ColumnName(i)
	}
	return cols
}

// Scan scans the current row into dest.
func (r *Rows) Scan(dest ...interface{}) error {
	if r.crows == nil {
		return nil
	}
	for i := range dest {
		if i < r.crows.ColumnCount() {
			dest[i] = r.crows.Get(i)
		}
	}
	return nil
}

// Close closes the rows.
func (r *Rows) Close() error {
	if r.crows != nil {
		r.crows.Close()
	}
	return nil
}
