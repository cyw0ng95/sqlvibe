package cgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/SC
#include "svdb.h"
*/
import "C"
import "fmt"

// svdbErr converts an svdb_code_t to a Go error.
func svdbErr(db *DB, code C.svdb_code_t) error {
	if code == C.SVDB_OK {
		return nil
	}
	var msg string
	if db != nil && db.h != nil {
		msg = db.Errmsg()
	}
	if msg == "" {
		switch code {
		case C.SVDB_ERR:
			msg = "generic error"
		case C.SVDB_NOTFOUND:
			msg = "not found"
		case C.SVDB_BUSY:
			msg = "database busy"
		case C.SVDB_READONLY:
			msg = "database read-only"
		case C.SVDB_CORRUPT:
			msg = "database corrupt"
		case C.SVDB_NOMEM:
			msg = "out of memory"
		case C.SVDB_DONE:
			msg = "done"
		default:
			msg = fmt.Sprintf("svdb error code %d", int(code))
		}
	}
	return fmt.Errorf("svdb: %s", msg)
}
