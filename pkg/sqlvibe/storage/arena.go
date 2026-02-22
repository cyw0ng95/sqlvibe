package storage

const arenaChunkSize = 64 * 1024 // 64 KB default chunk

// Arena is a simple bump-pointer allocator that reduces GC pressure by grouping
// many small allocations into large contiguous chunks. Call Reset() to free all
// allocations at once (e.g. at the end of a query).
type Arena struct {
	chunks  [][]byte
	current []byte
	offset  int
	used    int
}

// NewArena creates a new Arena with the given initial capacity.
func NewArena(initialSize int) *Arena {
	if initialSize <= 0 {
		initialSize = arenaChunkSize
	}
	buf := make([]byte, initialSize)
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
		// Grow: allocate a new chunk large enough
		chunkSize := arenaChunkSize
		if size > chunkSize {
			chunkSize = size
		}
		chunk := make([]byte, chunkSize)
		a.chunks = append(a.chunks, chunk)
		a.current = chunk
		a.offset = 0
	}
	buf := a.current[a.offset : a.offset+size]
	a.offset += size
	a.used += size
	return buf
}

// AllocSlice allocates a slice of n interface{} values from the arena.
// Note: the backing array lives on the Go heap; Arena tracks the usage count only.
func (a *Arena) AllocSlice(n int) []interface{} {
	a.used += n * 16 // approximate size
	return make([]interface{}, n)
}

// Reset resets the arena to its initial state, releasing all allocations.
func (a *Arena) Reset() {
	// Keep the first chunk, discard the rest
	if len(a.chunks) > 0 {
		a.current = a.chunks[0]
		a.chunks = a.chunks[:1]
	}
	a.offset = 0
	a.used = 0
}

// BytesUsed returns an approximation of the bytes currently allocated.
func (a *Arena) BytesUsed() int { return a.used }
