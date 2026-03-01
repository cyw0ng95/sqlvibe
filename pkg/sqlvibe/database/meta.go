package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Meta provides metadata and schema inspection helpers.
type Meta struct {
	db *sqlvibe.Database
}

// TableInfo returns PRAGMA table_info rows for a table.
func (m *Meta) TableInfo(table string) (*sqlvibe.Rows, error) {
	return m.db.Query("PRAGMA table_info(" + quoteIdent(table) + ")")
}

// TableList returns PRAGMA table_list rows listing all tables and views.
func (m *Meta) TableList() (*sqlvibe.Rows, error) {
	return m.db.Query("PRAGMA table_list")
}

// IndexList returns PRAGMA index_list rows for a table.
func (m *Meta) IndexList(table string) (*sqlvibe.Rows, error) {
	return m.db.Query("PRAGMA index_list(" + quoteIdent(table) + ")")
}

// IndexInfo returns PRAGMA index_info rows for an index.
func (m *Meta) IndexInfo(index string) (*sqlvibe.Rows, error) {
	return m.db.Query("PRAGMA index_info(" + quoteIdent(index) + ")")
}

// IndexXInfo returns PRAGMA index_xinfo rows for an index.
func (m *Meta) IndexXInfo(index string) (*sqlvibe.Rows, error) {
	return m.db.Query("PRAGMA index_xinfo(" + quoteIdent(index) + ")")
}

// ForeignKeyList returns PRAGMA foreign_key_list rows for a table.
func (m *Meta) ForeignKeyList(table string) (*sqlvibe.Rows, error) {
	return m.db.Query("PRAGMA foreign_key_list(" + quoteIdent(table) + ")")
}

// Schema returns the DDL schema string for a table.
func (m *Meta) Schema(table string) (string, error) {
	return m.db.Schema(table)
}
