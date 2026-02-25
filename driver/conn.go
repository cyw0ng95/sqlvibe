package driver

import (
	"context"
	"database/sql/driver"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Conn implements driver.Conn, driver.ConnBeginTx, driver.ExecerContext,
// and driver.QueryerContext.
type Conn struct {
	db *sqlvibe.Database
}

// Prepare returns a prepared statement.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt: stmt, query: query, conn: c}, nil
}

// Close closes the underlying database connection.
func (c *Conn) Close() error {
	return c.db.Close()
}

// Begin starts a new transaction using the SQL BEGIN statement.
func (c *Conn) Begin() (driver.Tx, error) {
	if _, err := c.db.Exec("BEGIN"); err != nil {
		return nil, err
	}
	return &Tx{db: c.db}, nil
}

// BeginTx starts a new transaction with context and options.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, err := c.db.Exec("BEGIN")
		ch <- result{err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return &Tx{db: c.db}, nil
	}
}

// ExecContext executes a non-query statement with context support.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	pos, named := fromNamedValues(args)
	var res sqlvibe.Result
	var err error
	if named != nil {
		res, err = c.db.ExecContextNamed(ctx, query, named)
	} else {
		res, err = c.db.ExecContextWithParams(ctx, query, pos)
	}
	if err != nil {
		return nil, err
	}
	return Result{lastInsertID: res.LastInsertRowID, rowsAffected: res.RowsAffected}, nil
}

// QueryContext executes a query statement with context support.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	pos, named := fromNamedValues(args)
	var rows *sqlvibe.Rows
	var err error
	if named != nil {
		rows, err = c.db.QueryContextNamed(ctx, query, named)
	} else {
		rows, err = c.db.QueryContextWithParams(ctx, query, pos)
	}
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}

// Ensure Conn implements required interfaces.
var _ driver.Conn = &Conn{}
var _ driver.ConnBeginTx = &Conn{}
var _ driver.ExecerContext = &Conn{}
var _ driver.QueryerContext = &Conn{}
