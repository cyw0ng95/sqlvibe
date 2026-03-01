package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// DDL provides CREATE / DROP / ALTER TABLE helpers.
type DDL struct {
	db *sqlvibe.Database
}

// CreateTable executes a CREATE TABLE statement.
func (d *DDL) CreateTable(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// DropTable executes a DROP TABLE [IF EXISTS] statement.
func (d *DDL) DropTable(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// AlterTable executes an ALTER TABLE statement.
func (d *DDL) AlterTable(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// CreateIndex executes a CREATE INDEX statement.
func (d *DDL) CreateIndex(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// DropIndex executes a DROP INDEX [IF EXISTS] statement.
func (d *DDL) DropIndex(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// CreateView executes a CREATE VIEW statement.
func (d *DDL) CreateView(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// DropView executes a DROP VIEW [IF EXISTS] statement.
func (d *DDL) DropView(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}
