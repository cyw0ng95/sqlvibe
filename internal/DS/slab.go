package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "slab.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// SlabStats holds allocation statistics for the slab allocator.
type SlabStats struct {
	TotalAllocs    int64
	SlabHits       int64
	PoolHits       int64
	Misses         int64
	BytesAllocated int64
}

// SlabAllocator wraps the C++ slab allocator for reduced GC pressure.
type SlabAllocator struct {
	ptr   *C.svdb_slab_alloc_t
	Stats SlabStats
}

// NewSlabAllocator creates a new SlabAllocator backed by the C++ implementation.
func NewSlabAllocator() *SlabAllocator {
	sa := &SlabAllocator{
		ptr: C.svdb_slab_alloc_create(),
	}
	runtime.SetFinalizer(sa, func(s *SlabAllocator) {
		if s.ptr != nil {
			C.svdb_slab_alloc_destroy(s.ptr)
			s.ptr = nil
		}
	})
	return sa
}

// Alloc allocates size bytes and returns a Go-owned []byte copy.
func (sa *SlabAllocator) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	ptr := C.svdb_slab_alloc(sa.ptr, C.size_t(size))
	if ptr == nil {
		return make([]byte, size)
	}
	buf := C.GoBytes(ptr, C.int(size))
	sa.syncStats()
	return buf
}

// AllocIntSlice allocates a slice of n ints, returning a Go-owned []int.
func (sa *SlabAllocator) AllocIntSlice(n int) []int {
	if n <= 0 {
		return nil
	}
	ptr := C.svdb_slab_alloc_int64_array(sa.ptr, C.size_t(n))
	if ptr == nil {
		return make([]int, n)
	}
	// 1<<28 is a safe upper bound for a CGO slice header (2^28 × 8 bytes = 2 GiB,
	// well within the addressable range on 64-bit systems and matching Go's max-slice convention).
	src := (*[1 << 28]C.int64_t)(unsafe.Pointer(ptr))[:n:n]
	dst := make([]int, n)
	for i := range dst {
		dst[i] = int(src[i])
	}
	sa.syncStats()
	return dst
}

// Reset resets the allocator state (keeps slabs for reuse). Stats are preserved.
func (sa *SlabAllocator) Reset() {
	if sa.ptr != nil {
		C.svdb_slab_alloc_reset(sa.ptr)
		sa.syncStats()
	}
}

// syncStats fetches stats from C++ and updates the Go Stats field.
func (sa *SlabAllocator) syncStats() {
	if sa.ptr == nil {
		return
	}
	cs := C.svdb_slab_alloc_stats(sa.ptr)
	sa.Stats.TotalAllocs = int64(cs.total_allocs)
	sa.Stats.SlabHits = int64(cs.slab_hits)
	sa.Stats.PoolHits = int64(cs.pool_hits)
	sa.Stats.Misses = int64(cs.misses)
	sa.Stats.BytesAllocated = int64(cs.bytes_allocated)
}
