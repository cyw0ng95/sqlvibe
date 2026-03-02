package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "skip_list.h"
#include <stdlib.h>
*/
import "C"
import (
"math"
"runtime"
"sync"
"unsafe"
)

// skipListFindBufSize is the maximum number of row indices returned per C++ query.
// Sized generously to avoid silent truncation in typical OLAP workloads.
const skipListFindBufSize = 65536

// nullKeyInt64 is the sentinel int64 used to represent TypeNull keys.
// No legitimate integer key may equal math.MinInt64.
const nullKeyInt64 = math.MinInt64

// SkipList provides O(log n) ordered key → row index mapping backed by C++.
// goKeys tracks all unique keys (in insertion order) for Pairs(), Min(), Max(),
// and Range() on non-integer key types.
type SkipList struct {
mu        sync.Mutex
cSkipList C.svdb_skiplist_t
goKeys    []Value // unique keys, deduped
}

// NewSkipList creates an empty SkipList.
func NewSkipList() *SkipList {
sl := &SkipList{
cSkipList: C.svdb_skiplist_create(),
goKeys:    make([]Value, 0),
}
runtime.SetFinalizer(sl, func(s *SkipList) {
if s.cSkipList != nil {
C.svdb_skiplist_destroy(s.cSkipList)
s.cSkipList = nil
}
})
return sl
}

// Len returns the number of unique keys.
func (sl *SkipList) Len() int {
sl.mu.Lock()
defer sl.mu.Unlock()
return int(C.svdb_skiplist_len(sl.cSkipList))
}

// Insert adds the key → rowIdx mapping.
func (sl *SkipList) Insert(key Value, rowIdx uint32) {
sl.mu.Lock()
defer sl.mu.Unlock()
sl.cInsert(key, int64(rowIdx))
// Track unique keys for iteration.
for _, k := range sl.goKeys {
if Compare(k, key) == 0 {
return
}
}
sl.goKeys = append(sl.goKeys, key)
}

// Delete removes the key → rowIdx pair.
func (sl *SkipList) Delete(key Value, rowIdx uint32) {
sl.mu.Lock()
defer sl.mu.Unlock()
sl.cDelete(key, int64(rowIdx))
// Remove key from goKeys if it has no remaining entries.
if sl.cFindCount(key) == 0 {
newKeys := sl.goKeys[:0]
for _, k := range sl.goKeys {
if Compare(k, key) != 0 {
newKeys = append(newKeys, k)
}
}
sl.goKeys = newKeys
}
}

// Find returns all row indices for key.
func (sl *SkipList) Find(key Value) []uint32 {
sl.mu.Lock()
defer sl.mu.Unlock()
return sl.cFind(key)
}

// Range returns all row indices for keys in [lo, hi] (or (lo, hi) if !inclusive).
// For int/float/bool key types the C++ range query is used.
// For string/bytes key types, goKeys is scanned for matching keys.
func (sl *SkipList) Range(lo, hi Value, inclusive bool) []uint32 {
sl.mu.Lock()
defer sl.mu.Unlock()

if isNumericType(lo.Type) && isNumericType(hi.Type) {
loInt := valueToInt64Key(lo)
hiInt := valueToInt64Key(hi)
inc := C.int(0)
if inclusive {
inc = 1
}
var buf [skipListFindBufSize]C.int64_t
n := int(C.svdb_skiplist_range_int(sl.cSkipList, C.int64_t(loInt), C.int64_t(hiInt), inc, &buf[0], skipListFindBufSize))
return int64SliceToUint32(buf[:n])
}

// For string/bytes types: iterate goKeys and Find each in-range key.
var out []uint32
for _, k := range sl.goKeys {
cmpLo := Compare(k, lo)
cmpHi := Compare(k, hi)
var inRange bool
if inclusive {
inRange = cmpLo >= 0 && cmpHi <= 0
} else {
inRange = cmpLo > 0 && cmpHi < 0
}
if inRange {
out = append(out, sl.cFind(k)...)
}
}
return out
}

// Min returns the smallest key. ok is false if empty.
// O(n) over goKeys; acceptable since skip lists index single columns.
func (sl *SkipList) Min() (Value, bool) {
sl.mu.Lock()
defer sl.mu.Unlock()
if len(sl.goKeys) == 0 {
return NullValue(), false
}
min := sl.goKeys[0]
for _, k := range sl.goKeys[1:] {
if Compare(k, min) < 0 {
min = k
}
}
return min, true
}

// Max returns the largest key. ok is false if empty.
// O(n) over goKeys; acceptable since skip lists index single columns.
func (sl *SkipList) Max() (Value, bool) {
sl.mu.Lock()
defer sl.mu.Unlock()
if len(sl.goKeys) == 0 {
return NullValue(), false
}
max := sl.goKeys[0]
for _, k := range sl.goKeys[1:] {
if Compare(k, max) > 0 {
max = k
}
}
return max, true
}

// SkipPair holds a single (key, rowIdx) association from a SkipList.
type SkipPair struct {
Key    Value
RowIdx uint32
}

// Pairs returns all (key, rowIdx) pairs. Each unique key may appear multiple times.
func (sl *SkipList) Pairs() []SkipPair {
sl.mu.Lock()
defer sl.mu.Unlock()
var out []SkipPair
for _, k := range sl.goKeys {
for _, idx := range sl.cFind(k) {
out = append(out, SkipPair{Key: k, RowIdx: idx})
}
}
return out
}

// --- internal helpers (must be called with mu held) ---

func isNumericType(t ValueType) bool {
return t == TypeInt || t == TypeFloat || t == TypeBool
}

// valueToInt64Key encodes a Value as an int64 for the C++ int-key API.
// TypeFloat uses a sort-preserving encoding: positive floats keep their bit
// pattern with the sign bit forced to 1 (so they sort after all negatives);
// negative floats have all bits flipped (so they sort before positives).
// This is the standard IEEE 754 total-order encoding used by database engines.
// NaN/Inf are not supported as skip-list keys.
// TypeNull maps to math.MinInt64 (nullKeyInt64 sentinel).
func valueToInt64Key(v Value) int64 {
	switch v.Type {
	case TypeInt, TypeBool:
		return v.Int
	case TypeFloat:
		bits := math.Float64bits(v.Float)
		if bits>>63 == 0 {
			// positive: set MSB so all positives sort after all negatives
			return int64(bits | (1 << 63))
		}
		// negative: flip all bits so negatives sort in ascending numeric order
		return int64(^bits)
	default:
		return nullKeyInt64
	}
}

func int64SliceToUint32(s []C.int64_t) []uint32 {
if len(s) == 0 {
return nil
}
out := make([]uint32, len(s))
for i, v := range s {
out[i] = uint32(v)
}
return out
}

func (sl *SkipList) cInsert(key Value, rowIdx int64) {
	switch key.Type {
	case TypeInt, TypeBool:
		C.svdb_skiplist_insert_int(sl.cSkipList, C.int64_t(key.Int), C.int64_t(rowIdx))
	case TypeFloat:
		C.svdb_skiplist_insert_int(sl.cSkipList, C.int64_t(valueToInt64Key(key)), C.int64_t(rowIdx))
	case TypeNull:
		C.svdb_skiplist_insert_int(sl.cSkipList, C.int64_t(nullKeyInt64), C.int64_t(rowIdx))
	case TypeString:
		// C.CString allocates; free after the call.
		cs := C.CString(key.Str)
		C.svdb_skiplist_insert_str(sl.cSkipList, (*C.uint8_t)(unsafe.Pointer(cs)), C.size_t(len(key.Str)), C.int64_t(rowIdx))
		C.free(unsafe.Pointer(cs))
	case TypeBytes:
		// TypeBytes: point directly into the Go slice — no C allocation.
		if len(key.Bytes) == 0 {
			C.svdb_skiplist_insert_str(sl.cSkipList, nil, 0, C.int64_t(rowIdx))
		} else {
			C.svdb_skiplist_insert_str(sl.cSkipList, (*C.uint8_t)(unsafe.Pointer(&key.Bytes[0])), C.size_t(len(key.Bytes)), C.int64_t(rowIdx))
		}
	}
}

func (sl *SkipList) cDelete(key Value, rowIdx int64) {
	switch key.Type {
	case TypeInt, TypeBool:
		C.svdb_skiplist_delete_int(sl.cSkipList, C.int64_t(key.Int), C.int64_t(rowIdx))
	case TypeFloat:
		C.svdb_skiplist_delete_int(sl.cSkipList, C.int64_t(valueToInt64Key(key)), C.int64_t(rowIdx))
	case TypeNull:
		C.svdb_skiplist_delete_int(sl.cSkipList, C.int64_t(nullKeyInt64), C.int64_t(rowIdx))
	case TypeString:
		cs := C.CString(key.Str)
		C.svdb_skiplist_delete_str(sl.cSkipList, (*C.uint8_t)(unsafe.Pointer(cs)), C.size_t(len(key.Str)), C.int64_t(rowIdx))
		C.free(unsafe.Pointer(cs))
	case TypeBytes:
		if len(key.Bytes) == 0 {
			C.svdb_skiplist_delete_str(sl.cSkipList, nil, 0, C.int64_t(rowIdx))
		} else {
			C.svdb_skiplist_delete_str(sl.cSkipList, (*C.uint8_t)(unsafe.Pointer(&key.Bytes[0])), C.size_t(len(key.Bytes)), C.int64_t(rowIdx))
		}
	}
}

func (sl *SkipList) cFind(key Value) []uint32 {
	var buf [skipListFindBufSize]C.int64_t
	var n int
	switch key.Type {
	case TypeInt, TypeBool:
		n = int(C.svdb_skiplist_find_int(sl.cSkipList, C.int64_t(key.Int), &buf[0], skipListFindBufSize))
	case TypeFloat:
		n = int(C.svdb_skiplist_find_int(sl.cSkipList, C.int64_t(valueToInt64Key(key)), &buf[0], skipListFindBufSize))
	case TypeNull:
		n = int(C.svdb_skiplist_find_int(sl.cSkipList, C.int64_t(nullKeyInt64), &buf[0], skipListFindBufSize))
	case TypeString:
		cs := C.CString(key.Str)
		n = int(C.svdb_skiplist_find_str(sl.cSkipList, (*C.uint8_t)(unsafe.Pointer(cs)), C.size_t(len(key.Str)), &buf[0], skipListFindBufSize))
		C.free(unsafe.Pointer(cs))
	case TypeBytes:
		if len(key.Bytes) == 0 {
			n = int(C.svdb_skiplist_find_str(sl.cSkipList, nil, 0, &buf[0], skipListFindBufSize))
		} else {
			n = int(C.svdb_skiplist_find_str(sl.cSkipList, (*C.uint8_t)(unsafe.Pointer(&key.Bytes[0])), C.size_t(len(key.Bytes)), &buf[0], skipListFindBufSize))
		}
	}
	return int64SliceToUint32(buf[:n])
}

func (sl *SkipList) cFindCount(key Value) int {
return len(sl.cFind(key))
}
