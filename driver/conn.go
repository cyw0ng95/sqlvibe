package driver

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// isSQLWordChar reports whether r is a word character in SQL identifiers.
func isSQLWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}

// splitSQLStatements splits SQL on top-level semicolons, respecting
// string literals, parentheses, and BEGIN…END blocks (used in triggers).
func splitSQLStatements(sql string) []string {
	var stmts []string
	var curStmt strings.Builder
	var wordBuf strings.Builder
	parenDepth := 0
	blockDepth := 0 // tracks BEGIN…END nesting (trigger bodies)
	inString := false

	// Called whenever a word boundary is reached; updates blockDepth.
	checkWord := func(w string) {
		upper := strings.ToUpper(w)
		switch upper {
		case "BEGIN":
			blockDepth++
		case "TRANSACTION", "DEFERRED", "IMMEDIATE", "EXCLUSIVE":
			// These follow "BEGIN" for transaction syntax (not a block body).
			if blockDepth > 0 {
				blockDepth--
			}
		case "END":
			if blockDepth > 0 {
				blockDepth--
			}
		}
	}

	for _, ch := range sql {
		if ch == '\'' {
			// Flush word buffer before toggling string mode.
			if wordBuf.Len() > 0 {
				checkWord(wordBuf.String())
				wordBuf.Reset()
			}
			inString = !inString
			curStmt.WriteRune(ch)
			continue
		}
		if inString {
			curStmt.WriteRune(ch)
			continue
		}
		if isSQLWordChar(ch) {
			wordBuf.WriteRune(ch)
			curStmt.WriteRune(ch)
			continue
		}
		// Non-word character: flush the word buffer.
		if wordBuf.Len() > 0 {
			checkWord(wordBuf.String())
			wordBuf.Reset()
		}
		if ch == '(' {
			parenDepth++
			curStmt.WriteRune(ch)
		} else if ch == ')' {
			parenDepth--
			curStmt.WriteRune(ch)
		} else if ch == ';' && parenDepth == 0 && blockDepth == 0 {
			stmt := strings.TrimSpace(curStmt.String())
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			curStmt.Reset()
		} else {
			curStmt.WriteRune(ch)
		}
	}
	// Flush any trailing word.
	if wordBuf.Len() > 0 {
		checkWord(wordBuf.String())
	}
	lastStmt := strings.TrimSpace(curStmt.String())
	if lastStmt != "" {
		stmts = append(stmts, lastStmt)
	}
	return stmts
}

// Conn implements driver.Conn, driver.ConnBeginTx, driver.ConnPrepareContext,
// driver.ExecerContext, and driver.QueryerContext.
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

	/* Split on semicolons and execute each statement */
	stmts := splitSQLStatements(query)
	if len(stmts) == 0 {
		return Result{}, nil
	}

	var lastRes sqlvibe.Result
	var lastErr error

	for _, stmt := range stmts {
		if named != nil {
			lastRes, lastErr = c.db.ExecContextNamed(ctx, stmt, named)
		} else {
			lastRes, lastErr = c.db.ExecContextWithParams(ctx, stmt, pos)
		}
		if lastErr != nil {
			return nil, lastErr
		}
	}

	return Result{lastInsertID: lastRes.LastInsertRowID, rowsAffected: lastRes.RowsAffected}, nil
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
