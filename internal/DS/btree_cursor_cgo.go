package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include <stdint.h>
#include <stddef.h>

void* SVDB_DS_BTreeCursor_Create();
void SVDB_DS_BTreeCursor_Destroy(void* cursor);
void SVDB_DS_BTreeCursor_Reset(void* cursor);
int SVDB_DS_BTreeCursor_First(void* cursor);
int SVDB_DS_BTreeCursor_Last(void* cursor);
int SVDB_DS_BTreeCursor_Next(void* cursor);
int SVDB_DS_BTreeCursor_Prev(void* cursor);
int SVDB_DS_BTreeCursor_Seek(void* cursor, const uint8_t* key, size_t key_len);
int SVDB_DS_BTreeCursor_IsValid(void* cursor);
const uint8_t* SVDB_DS_BTreeCursor_GetKey(void* cursor, size_t* out_len);
const uint8_t* SVDB_DS_BTreeCursor_GetValue(void* cursor, size_t* out_len);

void* SVDB_DS_PageCache_Create(size_t max_pages);
void SVDB_DS_PageCache_Destroy(void* cache);
void SVDB_DS_PageCache_Clear(void* cache);
size_t SVDB_DS_PageCache_GetSize(void* cache);
size_t SVDB_DS_PageCache_GetHits(void* cache);
size_t SVDB_DS_PageCache_GetMisses(void* cache);
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// CBTreeCursor is a CGO wrapper around the C++ BTreeCursor class.
type CBTreeCursor struct {
	ptr unsafe.Pointer
}

// NewCBTreeCursor creates a new C++ BTreeCursor and registers a finalizer.
func NewCBTreeCursor() *CBTreeCursor {
	c := &CBTreeCursor{ptr: unsafe.Pointer(C.SVDB_DS_BTreeCursor_Create())}
	runtime.SetFinalizer(c, func(x *CBTreeCursor) {
		if x.ptr != nil {
			C.SVDB_DS_BTreeCursor_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return c
}

func (c *CBTreeCursor) Reset() { C.SVDB_DS_BTreeCursor_Reset(c.ptr) }

func (c *CBTreeCursor) First() bool { return C.SVDB_DS_BTreeCursor_First(c.ptr) != 0 }

func (c *CBTreeCursor) Last() bool { return C.SVDB_DS_BTreeCursor_Last(c.ptr) != 0 }

func (c *CBTreeCursor) Next() bool { return C.SVDB_DS_BTreeCursor_Next(c.ptr) != 0 }

func (c *CBTreeCursor) Prev() bool { return C.SVDB_DS_BTreeCursor_Prev(c.ptr) != 0 }

func (c *CBTreeCursor) Seek(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	return C.SVDB_DS_BTreeCursor_Seek(c.ptr, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.size_t(len(key))) != 0
}

func (c *CBTreeCursor) IsValid() bool { return C.SVDB_DS_BTreeCursor_IsValid(c.ptr) != 0 }

func (c *CBTreeCursor) Key() []byte {
	var sz C.size_t
	p := C.SVDB_DS_BTreeCursor_GetKey(c.ptr, &sz)
	if p == nil || sz == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(p), C.int(sz))
}

func (c *CBTreeCursor) Value() []byte {
	var sz C.size_t
	p := C.SVDB_DS_BTreeCursor_GetValue(c.ptr, &sz)
	if p == nil || sz == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(p), C.int(sz))
}

// CPageCache is a CGO wrapper around the C++ PageCache class.
type CPageCache struct {
	ptr unsafe.Pointer
}

// NewCPageCache creates a new C++ PageCache with the given maximum page count.
func NewCPageCache(maxPages int) *CPageCache {
	pc := &CPageCache{ptr: unsafe.Pointer(C.SVDB_DS_PageCache_Create(C.size_t(maxPages)))}
	runtime.SetFinalizer(pc, func(x *CPageCache) {
		if x.ptr != nil {
			C.SVDB_DS_PageCache_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return pc
}

func (pc *CPageCache) Clear()   { C.SVDB_DS_PageCache_Clear(pc.ptr) }
func (pc *CPageCache) Size() int { return int(C.SVDB_DS_PageCache_GetSize(pc.ptr)) }
func (pc *CPageCache) Hits() int { return int(C.SVDB_DS_PageCache_GetHits(pc.ptr)) }
func (pc *CPageCache) Misses() int { return int(C.SVDB_DS_PageCache_GetMisses(pc.ptr)) }
