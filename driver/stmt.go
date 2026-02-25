package driver

import (
	"context"
	"database/sql/driver"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Stmt implements driver.Stmt, driver.StmtExecContext, and driver.StmtQueryContext.
type Stmt struct {
	stmt  *sqlvibe.Statement
	query string
	conn  *Conn
	closed bool
}

// Close closes the prepared statement.
func (s *Stmt) Close() error {
	if s.closed {
		return driver.ErrBadConn
	}
	s.closed = true
	return s.stmt.Close()
}

// NumInput returns -1 so the database/sql package validates args dynamically.
func (s *Stmt) NumInput() int {
	return -1
}

// Exec executes a non-query statement.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.closed {
		return nil, driver.ErrBadConn
	}
	params := make([]interface{}, len(args))
	for i, a := range args {
		params[i] = a
	}
	res, err := s.stmt.Exec(params...)
	if err != nil {
		return nil, err
	}
	return Result{lastInsertID: res.LastInsertRowID, rowsAffected: res.RowsAffected}, nil
}

// Query executes a query statement.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.closed {
		return nil, driver.ErrBadConn
	}
	params := make([]interface{}, len(args))
	for i, a := range args {
		params[i] = a
	}
	rows, err := s.stmt.Query(params...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}

// ExecContext executes a non-query statement with context support.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, driver.ErrBadConn
	}
	type result struct {
		res sqlvibe.Result
		err error
	}
	ch := make(chan result, 1)
	pos, _ := fromNamedValues(args)
	go func() {
		res, err := s.stmt.Exec(pos...)
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
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, driver.ErrBadConn
	}
	type result struct {
		rows *sqlvibe.Rows
		err  error
	}
	ch := make(chan result, 1)
	pos, _ := fromNamedValues(args)
	go func() {
		rows, err := s.stmt.Query(pos...)
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

// Ensure Stmt implements required interfaces.
var _ driver.Stmt = &Stmt{}
var _ driver.StmtExecContext = &Stmt{}
var _ driver.StmtQueryContext = &Stmt{}
