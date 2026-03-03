package svdbcgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../src/core/svdb
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Rows wraps a svdb_rows_t handle for iterating a result set.
type Rows struct {
	h *C.svdb_rows_t
}

// ColumnCount returns the number of columns in the result set.
func (r *Rows) ColumnCount() int {
	if r.h == nil {
		return 0
	}
	return int(C.svdb_rows_column_count(r.h))
}

// ColumnName returns the name of column col (0-based).
func (r *Rows) ColumnName(col int) string {
	if r.h == nil {
		return ""
	}
	return C.GoString(C.svdb_rows_column_name(r.h, C.int(col)))
}

// Next advances to the next row. Returns true if a row is available.
func (r *Rows) Next() bool {
	if r.h == nil {
		return false
	}
	return C.svdb_rows_next(r.h) != 0
}

// Get returns the value at column col in the current row.
func (r *Rows) Get(col int) interface{} {
	if r.h == nil {
		return nil
	}
	v := C.svdb_rows_get(r.h, C.int(col))
	// v._type: CGO renames C struct field "type" to "_type" because "type" is a Go keyword.
	switch v._type {
	case C.SVDB_TYPE_INT:
		return int64(v.ival)
	case C.SVDB_TYPE_REAL:
		return float64(v.rval)
	case C.SVDB_TYPE_TEXT:
		if v.sval == nil {
			return ""
		}
		return C.GoString(v.sval)
	case C.SVDB_TYPE_BLOB:
		if v.sval == nil {
			return []byte(nil)
		}
		return C.GoBytes(unsafe.Pointer(v.sval), C.int(v.slen))
	default:
		return nil
	}
}

// Close frees the result set resources.
func (r *Rows) Close() {
	if r.h != nil {
		C.svdb_rows_close(r.h)
		r.h = nil
	}
}

// Query executes a SELECT SQL statement and returns a Rows iterator.
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
