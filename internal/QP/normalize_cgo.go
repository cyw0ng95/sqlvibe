package QP

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/QP
#include "normalize.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// CNormalizeQuery normalizes sql for cache-key generation:
// lowercase, trim whitespace, replace string/numeric literals with ?.
func CNormalizeQuery(sql string) string {
	if len(sql) == 0 {
		return ""
	}
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	sqlLen := C.size_t(len(sql))

	size := C.svdb_normalize_get_required_size(cs, sqlLen)
	if size == 0 {
		return ""
	}

	buf := make([]C.char, size+1)
	n := C.svdb_normalize_query(cs, sqlLen, &buf[0], C.size_t(size+1))
	if n < 0 || int(n) > int(size)+1 {
		return ""
	}
	return C.GoStringN(&buf[0], n)
}
