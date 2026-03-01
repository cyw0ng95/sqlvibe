package QP

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/QP
#include "tokenizer.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// CToken is a token returned by CTokenizeSQL.
type CToken struct {
	Type  int
	Start int
	End   int
}

// CTokenCount returns the number of tokens in sql (including EOF).
func CTokenCount(sql string) int {
	if len(sql) == 0 {
		return 0
	}
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return int(C.svdb_token_count(cs, C.size_t(len(sql))))
}

// CTokenizeSQL tokenizes sql and returns a slice of CToken.
func CTokenizeSQL(sql string) []CToken {
	if len(sql) == 0 {
		return nil
	}
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	sqlLen := C.size_t(len(sql))
	count := C.svdb_token_count(cs, sqlLen)
	if count == 0 {
		return nil
	}
	ctoks := make([]C.svdb_token_t, count)
	n := C.svdb_tokenize(cs, sqlLen, &ctoks[0], count)
	result := make([]CToken, int(n))
	for i := 0; i < int(n); i++ {
		result[i] = CToken{
			Type:  int(ctoks[i]._type),
			Start: int(ctoks[i].start),
			End:   int(ctoks[i].end),
		}
	}
	return result
}
