package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "roaring.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)

// RoaringBitmap is a wrapper around the C++ roaring bitmap implementation.
type RoaringBitmap struct {
	ptr *C.svdb_roaring_bitmap_t
}

// NewRoaringBitmap creates an empty RoaringBitmap.
func NewRoaringBitmap() *RoaringBitmap {
	return &RoaringBitmap{
		ptr: C.svdb_roaring_create(),
	}
}

// Free releases the C++ bitmap memory.
func (rb *RoaringBitmap) Free() {
	if rb.ptr != nil {
		C.svdb_roaring_free(rb.ptr)
		rb.ptr = nil
	}
}

// IsEmpty returns true when the bitmap has no set bits.
func (rb *RoaringBitmap) IsEmpty() bool {
	return C.svdb_roaring_is_empty(rb.ptr) != 0
}

// Cardinality returns the number of set bits.
func (rb *RoaringBitmap) Cardinality() int {
	return int(C.svdb_roaring_cardinality(rb.ptr))
}

// Add adds x to the bitmap.
func (rb *RoaringBitmap) Add(x uint32) {
	C.svdb_roaring_add(rb.ptr, C.uint32_t(x))
}

// Remove removes x from the bitmap.
func (rb *RoaringBitmap) Remove(x uint32) {
	C.svdb_roaring_remove(rb.ptr, C.uint32_t(x))
}

// Contains checks if x is in the bitmap.
func (rb *RoaringBitmap) Contains(x uint32) bool {
	return C.svdb_roaring_contains(rb.ptr, C.uint32_t(x)) != 0
}

// Min returns the minimum value in the bitmap (or UINT32_MAX if empty).
func (rb *RoaringBitmap) Min() uint32 {
	return uint32(C.svdb_roaring_min(rb.ptr))
}

// Max returns the maximum value in the bitmap (or 0 if empty).
func (rb *RoaringBitmap) Max() uint32 {
	return uint32(C.svdb_roaring_max(rb.ptr))
}

// Rank counts values <= x.
func (rb *RoaringBitmap) Rank(x uint32) int {
	return int(C.svdb_roaring_rank(rb.ptr, C.uint32_t(x)))
}

// Select finds the n-th smallest value (0-indexed).
// Returns UINT32_MAX if n >= cardinality.
func (rb *RoaringBitmap) Select(n int) uint32 {
	return uint32(C.svdb_roaring_select(rb.ptr, C.size_t(n)))
}

// And returns a new bitmap with intersection: result = rb & other.
func (rb *RoaringBitmap) And(other *RoaringBitmap) *RoaringBitmap {
	result := rb.Clone()
	C.svdb_roaring_and(result.ptr, other.ptr)
	return result
}

// Or returns a new bitmap with union: result = rb | other.
func (rb *RoaringBitmap) Or(other *RoaringBitmap) *RoaringBitmap {
	result := rb.Clone()
	C.svdb_roaring_or(result.ptr, other.ptr)
	return result
}

// Xor returns a new bitmap with symmetric difference: result = rb ^ other.
func (rb *RoaringBitmap) Xor(other *RoaringBitmap) *RoaringBitmap {
	result := rb.Clone()
	C.svdb_roaring_xor(result.ptr, other.ptr)
	return result
}

// AndNot returns a new bitmap with difference: result = rb & ~other.
func (rb *RoaringBitmap) AndNot(other *RoaringBitmap) *RoaringBitmap {
	result := rb.Clone()
	C.svdb_roaring_andnot(result.ptr, other.ptr)
	return result
}

// AndInPlace performs in-place intersection: rb = rb & other.
func (rb *RoaringBitmap) AndInPlace(other *RoaringBitmap) {
	C.svdb_roaring_and(rb.ptr, other.ptr)
}

// OrInPlace performs in-place union: rb = rb | other.
func (rb *RoaringBitmap) OrInPlace(other *RoaringBitmap) {
	C.svdb_roaring_or(rb.ptr, other.ptr)
}

// XorInPlace performs in-place symmetric difference: rb = rb ^ other.
func (rb *RoaringBitmap) XorInPlace(other *RoaringBitmap) {
	C.svdb_roaring_xor(rb.ptr, other.ptr)
}

// AndNotInPlace performs in-place difference: rb = rb & ~other.
func (rb *RoaringBitmap) AndNotInPlace(other *RoaringBitmap) {
	C.svdb_roaring_andnot(rb.ptr, other.ptr)
}

// ToArray returns all values as a sorted slice.
func (rb *RoaringBitmap) ToArray() []uint32 {
	var count C.size_t
	cArray := C.svdb_roaring_to_array(rb.ptr, &count)
	if cArray == nil || count == 0 {
		return nil
	}
	defer C.free(unsafe.Pointer(cArray))

	result := make([]uint32, count)
	copy(result, unsafe.Slice((*uint32)(cArray), count))
	return result
}

// ToSlice is an alias for ToArray (for API compatibility).
func (rb *RoaringBitmap) ToSlice() []uint32 {
	return rb.ToArray()
}

// Clone creates a copy of the bitmap.
func (rb *RoaringBitmap) Clone() *RoaringBitmap {
	if rb.ptr == nil {
		return NewRoaringBitmap()
	}
	// Create from array representation
	arr := rb.ToArray()
	return FromArray(arr)
}

// UnionInPlace performs in-place union: rb = rb | other (alias for OrInPlace).
func (rb *RoaringBitmap) UnionInPlace(other *RoaringBitmap) {
	rb.OrInPlace(other)
}

// IntersectWith performs in-place intersection: rb = rb & other (alias for AndInPlace).
func (rb *RoaringBitmap) IntersectWith(other *RoaringBitmap) {
	rb.AndInPlace(other)
}

// FromArray creates a bitmap from a sorted slice.
func FromArray(values []uint32) *RoaringBitmap {
	if len(values) == 0 {
		return NewRoaringBitmap()
	}
	rb := &RoaringBitmap{
		ptr: C.svdb_roaring_from_array((*C.uint32_t)(unsafe.Pointer(&values[0])), C.size_t(len(values))),
	}
	return rb
}
