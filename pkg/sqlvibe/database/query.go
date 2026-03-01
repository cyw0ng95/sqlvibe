package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Query provides SELECT query helpers.
type Query struct {
	db *sqlvibe.Database
}

// Select executes a SELECT statement.
func (q *Query) Select(sql string) (*sqlvibe.Rows, error) {
	return q.db.Query(sql)
}

// SelectWithParams executes a parameterised SELECT statement.
func (q *Query) SelectWithParams(sql string, params []interface{}) (*sqlvibe.Rows, error) {
	return q.db.QueryWithParams(sql, params)
}

// SelectNamed executes a named-parameter SELECT statement.
func (q *Query) SelectNamed(sql string, params map[string]interface{}) (*sqlvibe.Rows, error) {
	return q.db.QueryNamed(sql, params)
}

// Pragma executes a PRAGMA query and returns the result rows.
func (q *Query) Pragma(sql string) (*sqlvibe.Rows, error) {
	return q.db.Query(sql)
}
