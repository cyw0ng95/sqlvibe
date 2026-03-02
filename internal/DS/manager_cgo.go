package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "manager.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// ManagerMinPageSize is the minimum valid page size (512 bytes).
const ManagerMinPageSize = 512

// ManagerMaxPageSize is the maximum valid page size (65536 bytes).
const ManagerMaxPageSize = 65536

// ManagerHeaderSize is the SQLite-compatible database header size (100 bytes).
const ManagerHeaderSize = 100

// ManagerPageOffset returns the file byte offset for the given page number
// (1-based). Page 1 starts at offset 0. Returns -1 if pageNum == 0.
func ManagerPageOffset(pageNum uint32, pageSize uint32) int64 {
	return int64(C.svdb_manager_page_offset(C.uint32_t(pageNum), C.uint32_t(pageSize)))
}

// ManagerIsValidPageSize returns true if pageSize is a power of two
// in the range [512, 65536].
func ManagerIsValidPageSize(pageSize uint32) bool {
	return C.svdb_manager_is_valid_page_size(C.uint32_t(pageSize)) != 0
}

// ManagerHeaderMagicValid returns true if the first 16 bytes of data match
// the SQLite format magic string.
func ManagerHeaderMagicValid(data []byte) bool {
	if len(data) < 16 {
		return false
	}
	var pinner runtime.Pinner
	pinner.Pin(&data[0])
	defer pinner.Unpin()
	return C.svdb_manager_header_magic_valid(
		(*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))) != 0
}

// ManagerReadHeaderPageSize reads the page size from the database header.
// Returns the page size (65536 if the stored value is 1), or 0 on error.
func ManagerReadHeaderPageSize(data []byte) uint32 {
	if len(data) < 18 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&data[0])
	defer pinner.Unpin()
	return uint32(C.svdb_manager_read_header_page_size(
		(*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// ManagerWriteHeaderPageSize writes pageSize into the database header at data.
// Returns true on success, false if data is too short or pageSize is invalid.
func ManagerWriteHeaderPageSize(data []byte, pageSize uint32) bool {
	if len(data) < 18 {
		return false
	}
	var pinner runtime.Pinner
	pinner.Pin(&data[0])
	defer pinner.Unpin()
	return C.svdb_manager_write_header_page_size(
		(*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)),
		C.uint32_t(pageSize)) != 0
}

// ManagerReadHeaderNumPages reads the database size (number of pages) from the
// header. Returns 0 if data is shorter than 32 bytes.
func ManagerReadHeaderNumPages(data []byte) uint32 {
	if len(data) < 32 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&data[0])
	defer pinner.Unpin()
	return uint32(C.svdb_manager_read_header_num_pages(
		(*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}

// ManagerWriteHeaderNumPages writes numPages into the database header at data.
// Returns true on success, false if data is too short.
func ManagerWriteHeaderNumPages(data []byte, numPages uint32) bool {
	if len(data) < 32 {
		return false
	}
	var pinner runtime.Pinner
	pinner.Pin(&data[0])
	defer pinner.Unpin()
	return C.svdb_manager_write_header_num_pages(
		(*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)),
		C.uint32_t(numPages)) != 0
}
