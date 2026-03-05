package cgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/svdb
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Tables returns a Rows result with all table names in the database.
func (db *DB) Tables() (*Rows, error) {
	var h *C.svdb_rows_t
	code := C.svdb_tables(db.h, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Rows{h: h}, nil
}

// Columns returns a Rows result with column info for the given table.
func (db *DB) Columns(table string) (*Rows, error) {
	cs := C.CString(table)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_rows_t
	code := C.svdb_columns(db.h, cs, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Rows{h: h}, nil
}

// Indexes returns a Rows result with index info for the given table.
func (db *DB) Indexes(table string) (*Rows, error) {
	cs := C.CString(table)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_rows_t
	code := C.svdb_indexes(db.h, cs, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Rows{h: h}, nil
}
