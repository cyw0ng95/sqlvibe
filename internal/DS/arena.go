package DS

import "sync"

// arenaChunkSize is the default size for new arena chunks.
// Increased from 64 KB to 256 KB to reduce chunk-allocation frequency in
// hot query paths and lower GC pressure.
const arenaChunkSize = 256 * 1024 // 256 KB default chunk

// arenaPool recycles large byte slices to avoid repeated GC-visible allocations
// when many Arena instances are created and reset during query execution.
var arenaPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, arenaChunkSize)
		return &buf
	},
}

// Arena is a simple bump-pointer allocator that reduces GC pressure by grouping
// many small allocations into large contiguous chunks. Call Reset() to free all
// allocations at once (e.g. at the end of a query).
type Arena struct {
	chunks    [][]byte
	current   []byte
	offset    int
	used      int
	totalUsed int // cumulative bytes allocated since creation (not reset by Reset)
	numAllocs int // total number of Alloc calls (not reset by Reset)
}

// NewArena creates a new Arena with the given initial capacity.
// If initialSize <= 0 the default arenaChunkSize is used.
func NewArena(initialSize int) *Arena {
	if initialSize <= 0 {
		initialSize = arenaChunkSize
	}
	var buf []byte
	if initialSize == arenaChunkSize {
		// Try to reuse a pooled chunk to avoid a new allocation.
		if p, ok := arenaPool.Get().(*[]byte); ok && p != nil {
			buf = *p
		}
	}
	if buf == nil {
		buf = make([]byte, initialSize)
	}
	return &Arena{
		chunks:  [][]byte{buf},
		current: buf,
		offset:  0,
	}
}

// Alloc returns a slice of size bytes from the arena.
func (a *Arena) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	if a.offset+size > len(a.current) {
		// Grow: allocate a new chunk large enough.
		chunkSize := arenaChunkSize
		if size > chunkSize {
			chunkSize = size
		}
		var chunk []byte
		if chunkSize == arenaChunkSize {
			if p, ok := arenaPool.Get().(*[]byte); ok && p != nil {
				chunk = *p
			}
		}
		if chunk == nil {
			chunk = make([]byte, chunkSize)
		}
		a.chunks = append(a.chunks, chunk)
		a.current = chunk
		a.offset = 0
	}
	buf := a.current[a.offset : a.offset+size]
	a.offset += size
	a.used += size
	a.totalUsed += size
	a.numAllocs++
	return buf
}

// AllocSlice allocates a slice of n interface{} values from the arena.
// Note: the backing array lives on the Go heap; Arena tracks the usage count only.
func (a *Arena) AllocSlice(n int) []interface{} {
	approx := n * 16
	a.used += approx
	a.totalUsed += approx
	a.numAllocs++
	return make([]interface{}, n)
}

// Reset resets the arena to its initial state, releasing extra chunks back to
// the pool to reduce GC pressure.
func (a *Arena) Reset() {
	// Return extra chunks to the pool.
	for i := 1; i < len(a.chunks); i++ {
		c := a.chunks[i]
		if len(c) == arenaChunkSize {
			arenaPool.Put(&c)
		}
		a.chunks[i] = nil // clear reference to help GC
	}
	// Keep only the first chunk.
	if len(a.chunks) > 0 {
		a.current = a.chunks[0]
		a.chunks = a.chunks[:1]
	}
	a.offset = 0
	a.used = 0
}

// BytesUsed returns an approximation of the bytes currently allocated since
// the last Reset.
func (a *Arena) BytesUsed() int { return a.used }

// TotalBytesAllocated returns the cumulative bytes allocated across all
// Reset cycles since this Arena was created.
func (a *Arena) TotalBytesAllocated() int { return a.totalUsed }

// NumAllocs returns the total number of Alloc/AllocSlice calls since creation.
func (a *Arena) NumAllocs() int { return a.numAllocs }
