package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/CG
#include "plan_cache.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// CPlanCache is a CGO wrapper around the C++ plan cache.
// It stores compiled plan JSON strings keyed by SQL text.
type CPlanCache struct {
	ptr *C.svdb_cg_cache_t
}

// NewCPlanCache creates a new C++ plan cache.
func NewCPlanCache() *CPlanCache {
	c := &CPlanCache{ptr: C.svdb_cg_cache_create()}
	runtime.SetFinalizer(c, func(x *CPlanCache) {
		if x.ptr != nil {
			C.svdb_cg_cache_free(x.ptr)
			x.ptr = nil
		}
	})
	return c
}

// PutJSON stores a JSON-encoded plan under sql.
func (c *CPlanCache) PutJSON(sql, jsonData string) {
	if c.ptr == nil {
		return
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	cjson := C.CString(jsonData)
	defer C.free(unsafe.Pointer(cjson))
	C.svdb_cg_cache_put_json(c.ptr, csql, cjson, C.size_t(len(jsonData)))
}

// GetJSON retrieves a JSON-encoded plan for sql.
// Returns ("", false) on cache miss.
func (c *CPlanCache) GetJSON(sql string) (string, bool) {
	if c.ptr == nil {
		return "", false
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var outLen C.size_t
	p := C.svdb_cg_cache_get_json(c.ptr, csql, &outLen)
	if p == nil {
		return "", false
	}
	return C.GoStringN(p, C.int(outLen)), true
}

// Erase removes all entries from the C++ cache.
func (c *CPlanCache) Erase() {
	if c.ptr != nil {
		C.svdb_cg_cache_erase(c.ptr)
	}
}

// Count returns the number of entries in the C++ cache.
func (c *CPlanCache) Count() int {
	if c.ptr == nil {
		return 0
	}
	return int(C.svdb_cg_cache_count(c.ptr))
}
