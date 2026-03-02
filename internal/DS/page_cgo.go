package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "page.h"
*/
import "C"
import "unsafe"

// CPageInit initializes data as an empty page of the given type.
// Returns true on success.
func CPageInit(data []byte, pageType uint8) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_page_init((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint8_t(pageType)) == 1
}

// CPageGetType returns the page type byte.
func CPageGetType(data []byte) uint8 {
	if len(data) == 0 {
		return 0
	}
	return uint8(C.svdb_page_get_type((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// CPageGetNumCells returns the cell count from the page header.
func CPageGetNumCells(data []byte) uint16 {
	if len(data) == 0 {
		return 0
	}
	return uint16(C.svdb_page_get_num_cells((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// CPageGetContentOffset returns the content area start offset.
func CPageGetContentOffset(data []byte) uint32 {
	if len(data) == 0 {
		return 0
	}
	return uint32(C.svdb_page_get_content_offset((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// CPageSetNumCells sets the cell count in the page header.
func CPageSetNumCells(data []byte, count uint16) {
	if len(data) == 0 {
		return
	}
	C.svdb_page_set_num_cells((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint16_t(count))
}

// CPageSetContentOffset sets the content area start offset.
func CPageSetContentOffset(data []byte, offset uint32) {
	if len(data) == 0 {
		return
	}
	C.svdb_page_set_content_offset((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.uint32_t(offset))
}

// CPageGetCellPointer returns the cell pointer at slot idx.
func CPageGetCellPointer(data []byte, idx int) uint16 {
	if len(data) == 0 {
		return 0
	}
	return uint16(C.svdb_page_get_cell_pointer((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.int(idx)))
}

// CPageSetCellPointer sets the cell pointer at slot idx.
func CPageSetCellPointer(data []byte, idx int, offset uint16) {
	if len(data) == 0 {
		return
	}
	C.svdb_page_set_cell_pointer((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.int(idx), C.uint16_t(offset))
}

// CPageInsertCellPointer inserts a cell pointer at slot idx, shifting existing pointers right.
// The caller must first call CPageSetNumCells with the incremented count. Returns true on success.
func CPageInsertCellPointer(data []byte, idx int, offset uint16) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_page_insert_cell_pointer((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.int(idx), C.uint16_t(offset)) == 1
}

// CPageRemoveCellPointer removes the cell pointer at slot idx, shifting later pointers left.
func CPageRemoveCellPointer(data []byte, idx int) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_page_remove_cell_pointer((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.int(idx)) == 1
}

// CPageIsOverfull returns true if the page is more than thresholdPct percent used.
func CPageIsOverfull(data []byte, thresholdPct int) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_page_is_overfull((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.int(thresholdPct)) == 1
}

// CPageIsUnderfull returns true if the page is less than thresholdPct percent used.
func CPageIsUnderfull(data []byte, thresholdPct int) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_page_is_underfull((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), C.int(thresholdPct)) == 1
}

// CPageUsedBytes returns the number of bytes currently used on the page.
func CPageUsedBytes(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	return int(C.svdb_page_used_bytes((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// CPageFreeBytes returns the number of free bytes available for new cell content.
func CPageFreeBytes(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	return int(C.svdb_page_free_bytes((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// CPageCompact defragments the page, packing all cell content toward the end.
// Returns true on success.
func CPageCompact(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_page_compact((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))) == 1
}
