// Package driver registers sqlvibe as a Go database/sql driver under the
// name "sqlvibe".
//
// To use sqlvibe through the standard database/sql interface, simply import
// this package for its side effects and open a connection with sql.Open:
//
//	import _ "github.com/cyw0ng95/sqlvibe/driver"
//
//	db, err := sql.Open("sqlvibe", ":memory:")
package driver

import (
	"database/sql"
	gosqldriver "database/sql/driver"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// DriverName is the name used to register the sqlvibe driver with database/sql.
const DriverName = "sqlvibe"

func init() {
	sql.Register(DriverName, &Driver{})
}

// Driver implements database/sql/driver.Driver.
type Driver struct{}

// Open opens a new database connection. The name parameter is the path to the
// database file, or ":memory:" for an in-memory database.
func (d *Driver) Open(name string) (gosqldriver.Conn, error) {
	db, err := sqlvibe.Open(name)
	if err != nil {
		return nil, err
	}
	return &Conn{db: db}, nil
}

// Ensure Driver implements driver.Driver.
var _ gosqldriver.Driver = &Driver{}
