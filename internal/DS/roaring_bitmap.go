package DS

import "sort"

const (
	arrayContainerMaxSize = 4096 // switch to bitmap container above this
	bitmapContainerWords  = 1024 // 1024 * 64 = 65536 bits
)

// containerKind differentiates array vs bitmap containers.
type containerKind int

const (
	kindArray containerKind = iota
	kindBitmap
)

// rbContainer holds either an array or a bitmap for one 16-bit key bucket.
type rbContainer struct {
	key    uint16
	kind   containerKind
	array  []uint16 // sorted, used when kind == kindArray
	bitmap []uint64 // 1024 words (65536 bits), used when kind == kindBitmap
	count  int      // cardinality
}

// RoaringBitmap is a pure-Go roaring bitmap implementation without external dependencies.
type RoaringBitmap struct {
	containers []*rbContainer // sorted by key
}

// NewRoaringBitmap creates an empty RoaringBitmap.
func NewRoaringBitmap() *RoaringBitmap {
	return &RoaringBitmap{}
}

// IsEmpty returns true when the bitmap has no set bits.
func (rb *RoaringBitmap) IsEmpty() bool { return rb.Cardinality() == 0 }

// Cardinality returns the number of set bits.
func (rb *RoaringBitmap) Cardinality() int {
	total := 0
	for _, c := range rb.containers {
		total += c.count
	}
	return total
}

func (rb *RoaringBitmap) findContainer(key uint16) (int, bool) {
	lo, hi := 0, len(rb.containers)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		k := rb.containers[mid].key
		if k == key {
			return mid, true
		} else if k < key {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return lo, false
}

func (rb *RoaringBitmap) getOrCreate(key uint16) *rbContainer {
	idx, found := rb.findContainer(key)
	if found {
		return rb.containers[idx]
	}
	c := &rbContainer{key: key, kind: kindArray}
	// Insert at idx to maintain sorted order
	rb.containers = append(rb.containers, nil)
	copy(rb.containers[idx+1:], rb.containers[idx:])
	rb.containers[idx] = c
	return c
}

// Add adds x to the bitmap.
func (rb *RoaringBitmap) Add(x uint32) {
	hi := uint16(x >> 16)
	lo := uint16(x & 0xFFFF)
	c := rb.getOrCreate(hi)
	containerAdd(c, lo)
}

// Remove removes x from the bitmap.
func (rb *RoaringBitmap) Remove(x uint32) {
	hi := uint16(x >> 16)
	lo := uint16(x & 0xFFFF)
	idx, found := rb.findContainer(hi)
	if !found {
		return
	}
	containerRemove(rb.containers[idx], lo)
	if rb.containers[idx].count == 0 {
		rb.containers = append(rb.containers[:idx], rb.containers[idx+1:]...)
	}
}

// Contains returns true if x is in the bitmap.
func (rb *RoaringBitmap) Contains(x uint32) bool {
	hi := uint16(x >> 16)
	lo := uint16(x & 0xFFFF)
	idx, found := rb.findContainer(hi)
	if !found {
		return false
	}
	return containerContains(rb.containers[idx], lo)
}

// ToSlice returns all elements in sorted order.
func (rb *RoaringBitmap) ToSlice() []uint32 {
	out := make([]uint32, 0, rb.Cardinality())
	for _, c := range rb.containers {
		base := uint32(c.key) << 16
		if c.kind == kindArray {
			for _, v := range c.array {
				out = append(out, base|uint32(v))
			}
		} else {
			for wi, w := range c.bitmap {
				for b := 0; b < 64; b++ {
					if w>>uint(b)&1 == 1 {
						out = append(out, base|uint32(wi*64+b))
					}
				}
			}
		}
	}
	return out
}

// Clone returns a deep copy.
func (rb *RoaringBitmap) Clone() *RoaringBitmap {
	out := &RoaringBitmap{containers: make([]*rbContainer, len(rb.containers))}
	for i, c := range rb.containers {
		out.containers[i] = cloneContainer(c)
	}
	return out
}

// And returns the intersection of rb and other.
func (rb *RoaringBitmap) And(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()
	i, j := 0, 0
	for i < len(rb.containers) && j < len(other.containers) {
		a, b := rb.containers[i], other.containers[j]
		if a.key == b.key {
			c := containerAnd(a, b)
			if c.count > 0 {
				result.containers = append(result.containers, c)
			}
			i++
			j++
		} else if a.key < b.key {
			i++
		} else {
			j++
		}
	}
	return result
}

// Or returns the union of rb and other.
func (rb *RoaringBitmap) Or(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()
	i, j := 0, 0
	for i < len(rb.containers) && j < len(other.containers) {
		a, b := rb.containers[i], other.containers[j]
		if a.key == b.key {
			result.containers = append(result.containers, containerOr(a, b))
			i++
			j++
		} else if a.key < b.key {
			result.containers = append(result.containers, cloneContainer(a))
			i++
		} else {
			result.containers = append(result.containers, cloneContainer(b))
			j++
		}
	}
	for ; i < len(rb.containers); i++ {
		result.containers = append(result.containers, cloneContainer(rb.containers[i]))
	}
	for ; j < len(other.containers); j++ {
		result.containers = append(result.containers, cloneContainer(other.containers[j]))
	}
	return result
}

// AndNot returns rb minus other.
func (rb *RoaringBitmap) AndNot(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()
	i, j := 0, 0
	for i < len(rb.containers) {
		if j >= len(other.containers) || rb.containers[i].key < other.containers[j].key {
			result.containers = append(result.containers, cloneContainer(rb.containers[i]))
			i++
		} else if rb.containers[i].key == other.containers[j].key {
			c := containerAndNot(rb.containers[i], other.containers[j])
			if c.count > 0 {
				result.containers = append(result.containers, c)
			}
			i++
			j++
		} else {
			j++
		}
	}
	return result
}

// ---- container helpers ----

func containerAdd(c *rbContainer, lo uint16) {
	if c.kind == kindArray {
		idx := sort.Search(len(c.array), func(i int) bool { return c.array[i] >= lo })
		if idx < len(c.array) && c.array[idx] == lo {
			return // already present
		}
		c.array = append(c.array, 0)
		copy(c.array[idx+1:], c.array[idx:])
		c.array[idx] = lo
		c.count++
		if c.count > arrayContainerMaxSize {
			convertToBitmap(c)
		}
	} else {
		wi, bit := int(lo)/64, uint(lo)%64
		if c.bitmap[wi]>>bit&1 == 0 {
			c.bitmap[wi] |= 1 << bit
			c.count++
		}
	}
}

func containerRemove(c *rbContainer, lo uint16) {
	if c.kind == kindArray {
		idx := sort.Search(len(c.array), func(i int) bool { return c.array[i] >= lo })
		if idx >= len(c.array) || c.array[idx] != lo {
			return
		}
		c.array = append(c.array[:idx], c.array[idx+1:]...)
		c.count--
	} else {
		wi, bit := int(lo)/64, uint(lo)%64
		if c.bitmap[wi]>>bit&1 == 1 {
			c.bitmap[wi] &^= 1 << bit
			c.count--
		}
		if c.count <= arrayContainerMaxSize/2 {
			convertToArray(c)
		}
	}
}

func containerContains(c *rbContainer, lo uint16) bool {
	if c.kind == kindArray {
		idx := sort.Search(len(c.array), func(i int) bool { return c.array[i] >= lo })
		return idx < len(c.array) && c.array[idx] == lo
	}
	wi, bit := int(lo)/64, uint(lo)%64
	return c.bitmap[wi]>>bit&1 == 1
}

func convertToBitmap(c *rbContainer) {
	bm := make([]uint64, bitmapContainerWords)
	for _, v := range c.array {
		wi, bit := int(v)/64, uint(v)%64
		bm[wi] |= 1 << bit
	}
	c.bitmap = bm
	c.array = nil
	c.kind = kindBitmap
}

func convertToArray(c *rbContainer) {
	arr := make([]uint16, 0, c.count)
	for wi, w := range c.bitmap {
		for b := 0; b < 64; b++ {
			if w>>uint(b)&1 == 1 {
				arr = append(arr, uint16(wi*64+b))
			}
		}
	}
	c.array = arr
	c.bitmap = nil
	c.kind = kindArray
}

func cloneContainer(c *rbContainer) *rbContainer {
	out := &rbContainer{key: c.key, kind: c.kind, count: c.count}
	if c.kind == kindArray {
		out.array = make([]uint16, len(c.array))
		copy(out.array, c.array)
	} else {
		out.bitmap = make([]uint64, len(c.bitmap))
		copy(out.bitmap, c.bitmap)
	}
	return out
}

func containerAnd(a, b *rbContainer) *rbContainer {
	c := &rbContainer{key: a.key, kind: kindArray}
	iterateContainer(a, func(v uint16) {
		if containerContains(b, v) {
			c.array = append(c.array, v)
			c.count++
		}
	})
	if c.count > arrayContainerMaxSize {
		convertToBitmap(c)
	}
	return c
}

func containerOr(a, b *rbContainer) *rbContainer {
	out := cloneContainer(a)
	iterateContainer(b, func(v uint16) {
		containerAdd(out, v)
	})
	return out
}

func containerAndNot(a, b *rbContainer) *rbContainer {
	c := &rbContainer{key: a.key, kind: kindArray}
	iterateContainer(a, func(v uint16) {
		if !containerContains(b, v) {
			c.array = append(c.array, v)
			c.count++
		}
	})
	if c.count > arrayContainerMaxSize {
		convertToBitmap(c)
	}
	return c
}

func iterateContainer(c *rbContainer, fn func(uint16)) {
	if c.kind == kindArray {
		for _, v := range c.array {
			fn(v)
		}
	} else {
		for wi, w := range c.bitmap {
			for b := 0; b < 64; b++ {
				if w>>uint(b)&1 == 1 {
					fn(uint16(wi*64 + b))
				}
			}
		}
	}
}

// IntersectWith replaces the receiver with its intersection with other.
func (rb *RoaringBitmap) IntersectWith(other *RoaringBitmap) {
	result := rb.And(other)
	rb.containers = result.containers
}

// UnionInPlace merges other into the receiver.
func (rb *RoaringBitmap) UnionInPlace(other *RoaringBitmap) {
	result := rb.Or(other)
	rb.containers = result.containers
}
