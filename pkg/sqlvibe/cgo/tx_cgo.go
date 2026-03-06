package cgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/SC
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Tx wraps a svdb_tx_t transaction handle.
type Tx struct{ h *C.svdb_tx_t }

// Begin starts a new transaction.
func (db *DB) Begin() (*Tx, error) {
	var h *C.svdb_tx_t
	code := C.svdb_begin(db.h, &h)
	if code != C.SVDB_OK {
		return nil, svdbErr(db, code)
	}
	return &Tx{h: h}, nil
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	code := C.svdb_commit(tx.h)
	tx.h = nil
	return svdbErr(nil, code)
}

// Rollback rolls back the transaction.
func (tx *Tx) Rollback() error {
	code := C.svdb_rollback(tx.h)
	tx.h = nil
	return svdbErr(nil, code)
}

// Savepoint creates a savepoint with the given name.
func (tx *Tx) Savepoint(name string) error {
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	return svdbErr(nil, C.svdb_savepoint(tx.h, cs))
}

// Release releases a savepoint.
func (tx *Tx) Release(name string) error {
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	return svdbErr(nil, C.svdb_release(tx.h, cs))
}

// RollbackTo rolls back to a savepoint.
func (tx *Tx) RollbackTo(name string) error {
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	return svdbErr(nil, C.svdb_rollback_to(tx.h, cs))
}
