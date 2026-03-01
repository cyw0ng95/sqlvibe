package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Constraint provides constraint checking helpers.
type Constraint struct {
	db *sqlvibe.Database
}

// ForeignKeyCheck runs PRAGMA foreign_key_check and returns any violations.
func (c *Constraint) ForeignKeyCheck() (*sqlvibe.Rows, error) {
	return c.db.Query("PRAGMA foreign_key_check")
}

// ForeignKeyCheckTable runs PRAGMA foreign_key_check(table) for a specific table.
func (c *Constraint) ForeignKeyCheckTable(table string) (*sqlvibe.Rows, error) {
	return c.db.Query("PRAGMA foreign_key_check(" + quoteIdent(table) + ")")
}

// QuickCheck runs PRAGMA quick_check and returns any integrity issues.
func (c *Constraint) QuickCheck() (*sqlvibe.Rows, error) {
	return c.db.Query("PRAGMA quick_check")
}

// IntegrityCheck runs PRAGMA integrity_check and returns any integrity issues.
func (c *Constraint) IntegrityCheck() (*sqlvibe.Rows, error) {
	return c.db.Query("PRAGMA integrity_check")
}
