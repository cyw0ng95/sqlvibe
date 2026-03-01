package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Prepare provides statement preparation helpers.
type Prepare struct {
	db *sqlvibe.Database
}

// Statement prepares a SQL statement for repeated execution.
func (p *Prepare) Statement(sql string) (*sqlvibe.Statement, error) {
	return p.db.Prepare(sql)
}
