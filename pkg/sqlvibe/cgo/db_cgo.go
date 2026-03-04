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

// Open opens (or creates) a database at the given path.
// Use ":memory:" for an in-memory database.
func Open(path string) (*DB, error) {
	cs := C.CString(path)
	defer C.free(unsafe.Pointer(cs))
	var h *C.svdb_db_t
	if code := C.svdb_open(cs, &h); code != C.SVDB_OK {
		return nil, svdbErr(nil, code)
	}
	return &DB{h: h}, nil
}

// Close closes the database and frees all resources.
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

// Errmsg returns the last error message for the database.
func (db *DB) Errmsg() string {
	if db.h == nil {
		return ""
	}
	return C.GoString(C.svdb_errmsg(db.h))
}

// Version returns the svdb version string.
func Version() string { return C.GoString(C.svdb_version()) }

// VersionNumber returns the svdb version as an integer (e.g. 112 for 0.11.2).
func VersionNumber() int { return int(C.svdb_version_number()) }
