package QP

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/cgo/../../../src/core/QP
#include "tokenizer.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// FastTokenCount returns the exact token count by running the C++ fast tokenizer.
// This is used to pre-allocate the token slice in Tokenize(), avoiding
// incremental slice growth.
func FastTokenCount(sql string) int {
	if len(sql) == 0 {
		return 1 // just EOF
	}
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	n := int(C.svdb_token_count(cs, C.size_t(len(sql))))
	if n < 1 {
		n = 1
	}
	return n
}
