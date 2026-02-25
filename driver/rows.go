package driver

import (
	"database/sql/driver"
	"io"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Rows implements driver.Rows.
type Rows struct {
	rows *sqlvibe.Rows
}

// Columns returns the names of the columns.
func (r *Rows) Columns() []string {
	if r.rows == nil {
		return nil
	}
	return r.rows.Columns
}

// Close closes the rows iterator. No-op since sqlvibe Rows are in-memory.
func (r *Rows) Close() error {
	return nil
}

// Next populates dest with the values of the next row.
// Returns io.EOF when there are no more rows.
func (r *Rows) Next(dest []driver.Value) error {
	if r.rows == nil || !r.rows.Next() {
		return io.EOF
	}
	// Read current row from r.rows.Data via position
	ifaces := make([]interface{}, len(dest))
	ptrs := make([]interface{}, len(dest))
	for i := range ifaces {
		ptrs[i] = &ifaces[i]
	}
	if err := r.rows.Scan(ptrs...); err != nil {
		return err
	}
	for i, v := range ifaces {
		dv, err := toDriverValue(v)
		if err != nil {
			return err
		}
		dest[i] = dv
	}
	return nil
}

// Ensure Rows implements driver.Rows.
var _ driver.Rows = &Rows{}
