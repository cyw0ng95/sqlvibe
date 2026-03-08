package cgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/SC
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Backup copies the database to destPath.
func (db *DB) Backup(destPath string) error {
	cs := C.CString(destPath)
	defer C.free(unsafe.Pointer(cs))
	return svdbErr(db, C.svdb_backup(db.h, cs))
}
