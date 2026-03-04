package cgo

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/svdb
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// DB wraps a svdb_db_t handle.
type DB struct{ h *C.svdb_db_t }

// Open opens a database.
func Open(path string) (*DB, error) {
	cs := C.CString(path)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_db_t
	if code := C.svdb_open(cs, &h); code != C.SVDB_OK {
		return nil, svdbErr(nil, code)
	}
	return &DB{h: h}, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.h == nil {
		return nil
	}
	code := C.svdb_close(db.h)
	db.h = nil
	if code != C.SVDB_OK {
		return svdbErr(nil, code)
	}
	return nil
}

// Exec executes SQL.
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

// Query queries SQL.
func (db *DB) Query(sql string) (*Rows, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_rows_t
	code := C.svdb_query(db.h, cs, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Rows{h: h}, nil
}

// Prepare prepares SQL.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_stmt_t
	code := C.svdb_prepare(db.h, cs, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Stmt{h: h, db: db, sql: sql}, nil
}

// Begin starts a transaction.
func (db *DB) Begin() (*Tx, error) {
	var h *C.svdb_tx_t
	code := C.svdb_begin(db.h, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Tx{h: h, db: db}, nil
}
