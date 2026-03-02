package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/CG
#include "bytecode_compiler.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// cBcCanUseFastPath returns true if sql is a simple SELECT that can use the
// fast bytecode compilation path (no JOIN, CTE, WINDOW, set ops).
func cBcCanUseFastPath(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_bc_can_use_fast_path(cs, C.size_t(len(sql))) != 0
}

// cBcHasAggregates returns true if sql contains aggregate function keywords.
func cBcHasAggregates(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_bc_has_aggregates(cs, C.size_t(len(sql))) != 0
}

// cBcNeedsSort returns true if sql contains ORDER BY.
func cBcNeedsSort(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_bc_needs_sort(cs, C.size_t(len(sql))) != 0
}

// cBcHasLimit returns true if sql contains a LIMIT clause.
func cBcHasLimit(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_bc_has_limit(cs, C.size_t(len(sql))) != 0
}

// cBcHasGroupBy returns true if sql contains GROUP BY.
func cBcHasGroupBy(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_bc_has_group_by(cs, C.size_t(len(sql))) != 0
}

// cBcHasWindowFunc returns true if sql contains window function calls (OVER).
func cBcHasWindowFunc(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_bc_has_window_func(cs, C.size_t(len(sql))) != 0
}

// cBcEstimateRegCount estimates the number of VM registers needed for a query.
func cBcEstimateRegCount(numColumns, hasWhere, hasAgg int) int {
	return int(C.svdb_bc_estimate_reg_count(C.int(numColumns), C.int(hasWhere), C.int(hasAgg)))
}
