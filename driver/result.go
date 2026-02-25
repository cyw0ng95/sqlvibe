package driver

import "database/sql/driver"

// Result implements driver.Result.
type Result struct {
	lastInsertID int64
	rowsAffected int64
}

func (r Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Ensure Result implements driver.Result.
var _ driver.Result = Result{}
