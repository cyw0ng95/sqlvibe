package database

import (
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// DML provides INSERT / UPDATE / DELETE helpers.
type DML struct {
	db *sqlvibe.Database
}

// Insert executes an INSERT statement.
func (d *DML) Insert(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// InsertWithParams executes a parameterised INSERT statement.
func (d *DML) InsertWithParams(sql string, params []interface{}) (sqlvibe.Result, error) {
	return d.db.ExecWithParams(sql, params)
}

// Update executes an UPDATE statement.
func (d *DML) Update(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// UpdateWithParams executes a parameterised UPDATE statement.
func (d *DML) UpdateWithParams(sql string, params []interface{}) (sqlvibe.Result, error) {
	return d.db.ExecWithParams(sql, params)
}

// Delete executes a DELETE statement.
func (d *DML) Delete(sql string) (sqlvibe.Result, error) {
	return d.db.Exec(sql)
}

// DeleteWithParams executes a parameterised DELETE statement.
func (d *DML) DeleteWithParams(sql string, params []interface{}) (sqlvibe.Result, error) {
	return d.db.ExecWithParams(sql, params)
}
