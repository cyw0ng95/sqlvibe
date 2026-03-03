package svdbcgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../src/core/svdb
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Stmt wraps a svdb_stmt_t prepared statement handle.
type Stmt struct{ h *C.svdb_stmt_t }

// Prepare compiles an SQL statement for repeated execution.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_stmt_t
	code := C.svdb_prepare(db.h, cs, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Stmt{h: h}, nil
}

// BindInt binds an integer value to parameter idx (1-based).
func (s *Stmt) BindInt(idx int, val int64) error {
	return svdbErr(nil, C.svdb_stmt_bind_int(s.h, C.int(idx), C.int64_t(val)))
}

// BindReal binds a float64 value to parameter idx (1-based).
func (s *Stmt) BindReal(idx int, val float64) error {
	return svdbErr(nil, C.svdb_stmt_bind_real(s.h, C.int(idx), C.double(val)))
}

// BindText binds a string value to parameter idx (1-based).
func (s *Stmt) BindText(idx int, val string) error {
	cs := C.CString(val)
	defer C.free(unsafe.Pointer(cs))
	return svdbErr(nil, C.svdb_stmt_bind_text(s.h, C.int(idx), cs, C.size_t(len(val))))
}

// BindNull binds NULL to parameter idx (1-based).
func (s *Stmt) BindNull(idx int) error {
	return svdbErr(nil, C.svdb_stmt_bind_null(s.h, C.int(idx)))
}

// Exec executes a non-query prepared statement.
func (s *Stmt) Exec() (Result, error) {
	var res C.svdb_result_t
	code := C.svdb_stmt_exec(s.h, &res)
	if code != C.SVDB_OK {
		return Result{}, svdbErr(nil, code)
	}
	return Result{
		RowsAffected:    int64(res.rows_affected),
		LastInsertRowid: int64(res.last_insert_rowid),
	}, nil
}

// Query executes a prepared SELECT statement.
func (s *Stmt) Query() (*Rows, error) {
	var h *C.svdb_rows_t
	code := C.svdb_stmt_query(s.h, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(nil, code)
	}
	return &Rows{h: h}, nil
}

// Reset clears all bound parameters.
func (s *Stmt) Reset() error {
	return svdbErr(nil, C.svdb_stmt_reset(s.h))
}

// Close frees the prepared statement.
func (s *Stmt) Close() error {
	if s.h == nil {
		return nil
	}
	code := C.svdb_stmt_close(s.h)
	s.h = nil
	return svdbErr(nil, code)
}
