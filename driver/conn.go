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
	type result struct {
		res sqlvibe.Result
		err error
	}
	ch := make(chan result, 1)
	pos, named := fromNamedValues(args)
	go func() {
		var res sqlvibe.Result
		var err error
		if named != nil {
			res, err = c.db.ExecNamed(query, named)
		} else {
			res, err = c.db.ExecWithParams(query, pos)
		}
		ch <- result{res, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return Result{lastInsertID: r.res.LastInsertRowID, rowsAffected: r.res.RowsAffected}, nil
	}
}

// QueryContext executes a query statement with context support.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	type result struct {
		rows *sqlvibe.Rows
		err  error
	}
	ch := make(chan result, 1)
	pos, named := fromNamedValues(args)
	go func() {
		var rows *sqlvibe.Rows
		var err error
		if named != nil {
			rows, err = c.db.QueryNamed(query, named)
		} else {
			rows, err = c.db.QueryWithParams(query, pos)
		}
		ch <- result{rows, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return &Rows{rows: r.rows}, nil
	}
}

// Ensure Conn implements required interfaces.
var _ driver.Conn = &Conn{}
var _ driver.ConnBeginTx = &Conn{}
var _ driver.ExecerContext = &Conn{}
var _ driver.QueryerContext = &Conn{}
