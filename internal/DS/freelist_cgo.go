package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "freelist.h"
*/
import "C"
import "unsafe"

// CFreelistMaxEntries returns the maximum number of leaf entries that fit in a trunk page.
func CFreelistMaxEntries(pageSize int) uint32 {
	return uint32(C.svdb_freelist_max_entries(C.size_t(pageSize)))
}

// CFreelistParseTrunk parses the trunk page header, returning nextTrunk, count, and ok.
func CFreelistParseTrunk(data []byte) (nextTrunk uint32, count uint32, ok bool) {
	if len(data) == 0 {
		return 0, 0, false
	}
	var cNext, cCount C.uint32_t
	rc := C.svdb_freelist_parse_trunk((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), &cNext, &cCount)
	if rc == 0 {
		return 0, 0, false
	}
	return uint32(cNext), uint32(cCount), true
}

// CFreelistWriteTrunk writes the trunk page header.
func CFreelistWriteTrunk(data []byte, nextTrunk uint32, count uint32) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_freelist_write_trunk((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint32_t(nextTrunk), C.uint32_t(count)) == 1
}

// CFreelistGetEntry returns the leaf page number at slot idx, or 0 on error.
func CFreelistGetEntry(data []byte, idx uint32) uint32 {
	if len(data) == 0 {
		return 0
	}
	return uint32(C.svdb_freelist_get_entry((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint32_t(idx)))
}

// CFreelistSetEntry sets the leaf page number at slot idx.
func CFreelistSetEntry(data []byte, idx uint32, pageNum uint32) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_freelist_set_entry((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint32_t(idx), C.uint32_t(pageNum)) == 1
}

// CFreelistAddEntry appends pageNum to the entry list.
func CFreelistAddEntry(data []byte, pageNum uint32) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_freelist_add_entry((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint32_t(pageNum)) == 1
}

// CFreelistRemoveEntry removes the entry at slot idx.
func CFreelistRemoveEntry(data []byte, idx uint32) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_freelist_remove_entry((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint32_t(idx)) == 1
}
