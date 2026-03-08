package cgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/SC
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Result holds the outcome of an Exec call.
type Result struct {
	RowsAffected    int64
	LastInsertRowid int64
}

// Exec executes a non-query SQL statement and returns the result.
func (db *DB) Exec(sql string) (Result, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var res C.svdb_result_t
	code := C.svdb_exec(db.h, cs, &res)
	if code != C.SVDB_OK {
		return Result{}, svdbErr(db, code)
	}
	return Result{
		RowsAffected:    int64(res.rows_affected),
		LastInsertRowid: int64(res.last_insert_rowid),
	}, nil
}
