package DS

import (
	"sync"
	"unsafe"
)

const (
	slabSize  = 64 * 1024
	maxSlabs  = 16
	smallSlab = 4 * 1024
)

// SlabStats tracks allocator usage statistics.
type SlabStats struct {
	TotalAllocs    int64
	SlabHits       int64
	PoolHits       int64
	Misses         int64
	BytesAllocated int64
}

// SlabAllocator manages memory in fixed-size chunks to reduce GC pressure.
type SlabAllocator struct {
	slabs     [][]byte
	current   []byte
	offset    int
	smallPool sync.Pool
	Stats     SlabStats
}

// NewSlabAllocator creates a new slab allocator with one pre-allocated slab.
func NewSlabAllocator() *SlabAllocator {
	sa := &SlabAllocator{
		slabs:   make([][]byte, 0, maxSlabs),
		current: make([]byte, slabSize),
	}
	sa.smallPool = sync.Pool{
		New: func() interface{} { return make([]byte, smallSlab) },
	}
	return sa
}

// Alloc returns a byte slice of exactly size bytes from the slab.
func (sa *SlabAllocator) Alloc(size int) []byte {
	sa.Stats.TotalAllocs++
	sa.Stats.BytesAllocated += int64(size)

	if size <= smallSlab/4 {
		buf := sa.smallPool.Get().([]byte)
		sa.Stats.PoolHits++
		return buf[:size]
	}

	if sa.offset+size <= len(sa.current) {
		buf := sa.current[sa.offset : sa.offset+size]
		sa.offset += size
		sa.Stats.SlabHits++
		return buf
	}

	if size > slabSize {
		sa.Stats.Misses++
		return make([]byte, size)
	}

	if len(sa.slabs) >= maxSlabs-1 {
		sa.Reset()
		sa.Stats.Misses++
		return make([]byte, size)
	}

	newSlab := make([]byte, slabSize)
	sa.slabs = append(sa.slabs, sa.current)
	sa.current = newSlab
	sa.offset = size
	sa.Stats.SlabHits++
	return newSlab[:size]
}

// Reset resets the bump pointer so slabs can be reused without freeing.
func (sa *SlabAllocator) Reset() {
	sa.offset = 0
	sa.slabs = sa.slabs[:0]
}

// Release returns a pooled small buffer.
func (sa *SlabAllocator) Release(buf []byte) {
	if cap(buf) == smallSlab {
		sa.smallPool.Put(buf[:smallSlab])
	}
}

// AllocIntSlice allocates a []int64 from the slab.
func (sa *SlabAllocator) AllocIntSlice(n int) []int64 {
	if n == 0 {
		return nil
	}
	buf := sa.Alloc(n * 8)
	return unsafe.Slice((*int64)(unsafe.Pointer(&buf[0])), n)
}

// AllocFloatSlice allocates a []float64 from the slab.
func (sa *SlabAllocator) AllocFloatSlice(n int) []float64 {
	if n == 0 {
		return nil
	}
	buf := sa.Alloc(n * 8)
	return unsafe.Slice((*float64)(unsafe.Pointer(&buf[0])), n)
}

// AllocStringSlice allocates a []string from the slab.
func (sa *SlabAllocator) AllocStringSlice(n int) []string {
	if n == 0 {
		return nil
	}
	buf := sa.Alloc(int(unsafe.Sizeof("")) * n)
	return unsafe.Slice((*string)(unsafe.Pointer(&buf[0])), n)
}

// AllocInterfaceSlice allocates a []interface{} from the slab.
func (sa *SlabAllocator) AllocInterfaceSlice(n int) []interface{} {
	if n == 0 {
		return nil
	}
	buf := sa.Alloc(int(unsafe.Sizeof((*interface{})(nil))*2) * n)
	return unsafe.Slice((*interface{})(unsafe.Pointer(&buf[0])), n)
}
