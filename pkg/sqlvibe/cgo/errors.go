package cgo

/*
#include "svdb.h"
*/
import "C"
import "fmt"

// Result holds execution result.
type Result struct {
	RowsAffected    int64
	LastInsertRowid int64
}

// Rows holds query results.
type Rows struct {
	h *C.svdb_rows_t
}

// Stmt is a prepared statement.
type Stmt struct {
	h    *C.svdb_stmt_t
	db   *DB
	sql  string
}

// Tx is a transaction.
type Tx struct {
	h  *C.svdb_tx_t
	db *DB
}

func svdbErr(db *DB, code C.svdb_code_t) error {
	if db != nil && db.h != nil {
		return fmt.Errorf("svdb error %d: %s", code, C.GoString(C.svdb_errmsg(db.h)))
	}
	return fmt.Errorf("svdb error %d", code)
}

// ColumnCount returns column count.
func (r *Rows) ColumnCount() int {
	if r.h == nil {
		return 0
	}
	return int(C.svdb_rows_column_count(r.h))
}

// ColumnName returns column name.
func (r *Rows) ColumnName(col int) string {
	if r.h == nil {
		return ""
	}
	return C.GoString(C.svdb_rows_column_name(r.h, C.int(col)))
}

// Next advances to next row.
func (r *Rows) Next() bool {
	if r.h == nil {
		return false
	}
	return C.svdb_rows_next(r.h) != 0
}

// Get returns column value.
func (r *Rows) Get(col int) interface{} {
	if r.h == nil {
		return nil
	}
	v := C.svdb_rows_get(r.h, C.int(col))
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
	default:
		return nil
	}
}

// Close closes rows.
func (r *Rows) Close() {
	if r.h != nil {
		C.svdb_rows_close(r.h)
		r.h = nil
	}
}

// Close closes statement.
func (s *Stmt) Close() error {
	if s.h != nil {
		return svdbErr(s.db, C.svdb_stmt_close(s.h))
	}
	return nil
}

// Commit commits transaction.
func (t *Tx) Commit() error {
	if t.h != nil {
		return svdbErr(t.db, C.svdb_commit(t.h))
	}
	return nil
}

// Rollback rolls back transaction.
func (t *Tx) Rollback() error {
	if t.h != nil {
		return svdbErr(t.db, C.svdb_rollback(t.h))
	}
	return nil
}
