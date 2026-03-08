package cgo

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/SC
#include "svdb.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)

// Default batch size for row fetching (reduces CGO calls 256x)
const defaultBatchSize = 256

// Rows wraps a svdb_rows_t handle for iterating a result set.
type Rows struct {
	h         *C.svdb_rows_t
	batch     C.svdb_row_batch_t // current batch buffer (embedded struct)
	batchRows int                // rows in current batch
	batchIdx  int                // current row index within batch (0 to batchRows-1)
	colCount  int                // cached column count
	exhausted bool               // true when no more rows to fetch
	hasBatch  bool               // true if batch contains valid data
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

	// Check if we have more rows in the current batch
	if r.hasBatch && r.batchIdx < r.batchRows-1 {
		r.batchIdx++
		return true
	}

	// Need to fetch a new batch
	if r.exhausted {
		return false
	}

	// Free previous batch if any
	if r.hasBatch {
		C.svdb_row_batch_free(&r.batch)
		r.hasBatch = false
	}

	// Fetch next batch
	fetchCount := C.svdb_rows_fetch_batch(r.h, &r.batch, defaultBatchSize)
	if fetchCount == 0 {
		r.batchRows = 0
		r.batchIdx = 0
		r.exhausted = true
		return false
	}

	r.batchRows = int(fetchCount)
	r.batchIdx = 0
	r.hasBatch = true
	if r.colCount == 0 {
		r.colCount = int(C.svdb_batch_col_count(&r.batch))
	}
	return true
}

// Get returns the value at column col in the current row.
func (r *Rows) Get(col int) interface{} {
	if r.h == nil || !r.hasBatch {
		return nil
	}
	if col < 0 || col >= r.colCount || r.batchIdx >= r.batchRows {
		return nil
	}

	rowIdx := C.int(r.batchIdx)
	colIdx := C.int(col)

	// Check for NULL
	if C.svdb_batch_is_null(&r.batch, colIdx, rowIdx) != 0 {
		return nil
	}

	// Get per-row type
	colType := C.svdb_batch_get_row_type(&r.batch, colIdx, rowIdx)

	switch colType {
	case C.SVDB_TYPE_INT:
		return int64(C.svdb_batch_get_int(&r.batch, colIdx, rowIdx))
	case C.SVDB_TYPE_REAL:
		return float64(C.svdb_batch_get_real(&r.batch, colIdx, rowIdx))
	case C.SVDB_TYPE_TEXT:
		var slen C.size_t
		sval := C.svdb_batch_get_text(&r.batch, colIdx, rowIdx, &slen)
		if sval == nil {
			return ""
		}
		return C.GoStringN(sval, C.int(slen))
	case C.SVDB_TYPE_BLOB:
		var slen C.size_t
		sval := C.svdb_batch_get_blob(&r.batch, colIdx, rowIdx, &slen)
		if sval == nil {
			return []byte(nil)
		}
		return C.GoBytes(unsafe.Pointer(sval), C.int(slen))
	default:
		return nil
	}
}

// Close frees the result set resources.
func (r *Rows) Close() {
	if r.hasBatch {
		C.svdb_row_batch_free(&r.batch)
		r.hasBatch = false
	}
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
