package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/CG
#include "direct_compiler.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// cDirectIsSimpleSelect returns true if sql is a simple SELECT that qualifies
// for the direct (fast-path) compilation route. Delegates to C++.
func cDirectIsSimpleSelect(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_direct_is_simple_select(cs, C.size_t(len(sql))) != 0
}

// cDirectExtractTableName extracts the first table name from the FROM clause.
// Returns an empty string if not found.
func cDirectExtractTableName(sql string) string {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var buf [256]C.char
	n := C.svdb_direct_extract_table_name(cs, C.size_t(len(sql)), &buf[0], C.int(len(buf)))
	if n <= 0 {
		return ""
	}
	return C.GoStringN(&buf[0], n)
}

// cDirectExtractLimit returns the LIMIT value from sql, or -1 if absent.
func cDirectExtractLimit(sql string) int64 {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return int64(C.svdb_direct_extract_limit(cs, C.size_t(len(sql))))
}

// cDirectExtractOffset returns the OFFSET value from sql, or 0 if absent.
func cDirectExtractOffset(sql string) int64 {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return int64(C.svdb_direct_extract_offset(cs, C.size_t(len(sql))))
}

// cDirectHasWhere returns true if sql contains a WHERE clause.
func cDirectHasWhere(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_direct_has_where(cs, C.size_t(len(sql))) != 0
}

// cDirectHasOrderBy returns true if sql contains ORDER BY.
func cDirectHasOrderBy(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_direct_has_order_by(cs, C.size_t(len(sql))) != 0
}
