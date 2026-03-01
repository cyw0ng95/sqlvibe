package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "string_pool.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// stringPoolIntern returns the canonical pooled copy of s.
// This uses the C++ StringPool implementation for memory-efficient string interning.
func stringPoolIntern(s string) string {
	if len(s) == 0 {
		return ""
	}
	cstr := C.CString(s)
	defer C.free(unsafe.Pointer(cstr))
	
	interned := C.svdb_string_intern(cstr, C.size_t(len(s)))
	if interned == nil {
		return s
	}
	return C.GoStringN(interned, C.int(len(s)))
}

// InternString returns the canonical pooled copy of s.
// Identical strings share a single backing allocation.
func InternString(s string) string {
	return stringPoolIntern(s)
}

// IsInterned checks if a string is already in the pool.
func IsInterned(s string) bool {
	if len(s) == 0 {
		return false
	}
	cstr := C.CString(s)
	defer C.free(unsafe.Pointer(cstr))
	return C.svdb_string_is_interned(cstr, C.size_t(len(s))) != 0
}

// StringPoolSize returns the number of interned strings.
func StringPoolSize() int {
	return int(C.svdb_string_pool_size())
}

// ClearStringPool clears the string pool (for testing).
func ClearStringPool() {
	C.svdb_string_pool_clear()
}
