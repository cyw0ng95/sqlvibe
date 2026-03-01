//go:build SVDB_ENABLE_CGO_DS
// +build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_ds
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "varint.h"
#include "cell.h"
#include "btree.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

// useCGODS controls whether to use CGO for DS operations
var useCGODS = true

// CGO-accelerated varint operations

// GetVarintCGO decodes a varint from buf using CGO
func GetVarintCGO(buf []byte) (int64, int, error) {
	if len(buf) == 0 {
		return 0, 0, errors.New("empty buffer")
	}

	var value C.int64_t
	var bytesRead C.int

	ret := C.svdb_get_varint((*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &value, &bytesRead)
	if ret == 0 {
		return 0, 0, errors.New("failed to decode varint")
	}

	return int64(value), int(bytesRead), nil
}

// PutVarintCGO encodes an int64 as a varint using CGO
func PutVarintCGO(buf []byte, v int64) (int, error) {
	if len(buf) == 0 {
		return 0, errors.New("empty buffer")
	}

	ret := C.svdb_put_varint((*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), C.int64_t(v))
	if ret == 0 {
		return 0, errors.New("buffer too small")
	}

	return int(ret), nil
}

// VarintLenCGO returns the number of bytes required to encode v as a varint
func VarintLenCGO(v int64) int {
	return int(C.svdb_varint_len(C.int64_t(v)))
}

// CGO-accelerated cell operations

// EncodeTableLeafCellCGO encodes a table leaf cell using CGO
func EncodeTableLeafCellCGO(rowid int64, payload []byte, overflowPage uint32) ([]byte, error) {
	if len(payload) == 0 || rowid <= 0 {
		return nil, errors.New("invalid parameters")
	}

	// Calculate required size
	size := C.svdb_table_leaf_cell_size(C.int64_t(rowid), C.size_t(len(payload)), C.uint32_t(overflowPage))
	buf := make([]byte, size)

	ret := C.svdb_encode_table_leaf_cell(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		C.int64_t(rowid),
		(*C.uint8_t)(unsafe.Pointer(&payload[0])),
		C.size_t(len(payload)),
		C.uint32_t(overflowPage),
	)

	if ret == 0 {
		return nil, errors.New("failed to encode cell")
	}

	return buf[:ret], nil
}

// DecodeTableLeafCellCGO decodes a table leaf cell using CGO
func DecodeTableLeafCellCGO(buf []byte) (rowid int64, payload []byte, overflowPage uint32, err error) {
	if len(buf) < 2 {
		return 0, nil, 0, errors.New("buffer too small")
	}

	var cell C.svdb_cell_data_t
	ret := C.svdb_decode_table_leaf_cell(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		&cell,
	)

	if ret == 0 {
		return 0, nil, 0, errors.New("failed to decode cell")
	}

	defer C.svdb_free_cell_data(&cell)

	rowid = int64(cell.rowid)
	overflowPage = uint32(cell.overflow_page)

	if cell.payload_len > 0 && cell.payload != nil {
		payload = make([]byte, cell.payload_len)
		copy(payload, C.GoBytes(unsafe.Pointer(cell.payload), C.int(cell.payload_len)))
	}

	return rowid, payload, overflowPage, nil
}

// BTree CGO wrapper

// BTreeCGO is a CGO-accelerated B-Tree
type BTreeCGO struct {
	handle *C.svdb_btree_t
	pm     *C.svdb_page_manager_t
	pmData *pageManagerData
}

type pageManagerData struct {
	goPM *PageManager
}

//export goReadPage
func goReadPage(userData unsafe.Pointer, pageNum C.uint32_t, pageData **C.uint8_t, pageSize *C.size_t) C.int {
	// This would need to call back into Go - simplified for now
	_ = userData
	_ = pageNum
	_ = pageData
	_ = pageSize
	return C.int(1) // Not implemented
}

// NewBTreeCGO creates a new CGO-accelerated B-Tree
func NewBTreeCGO(pm *PageManager, rootPage uint32, isTable bool) *BTreeCGO {
	config := C.svdb_btree_config_t{
		root_page:  C.uint32_t(rootPage),
		is_table:   cBool(isTable),
		page_size:  C.uint32_t(DefaultPageSize),
	}

	// Note: Full implementation would require proper page manager callbacks
	// For now, we use CGO for varint/cell operations only
	bt := C.svdb_btree_create(&config, nil)
	if bt == nil {
		return nil
	}

	return &BTreeCGO{
		handle: bt,
	}
}

// Close closes the B-Tree
func (bt *BTreeCGO) Close() {
	if bt.handle != nil {
		C.svdb_btree_destroy(bt.handle)
		bt.handle = nil
	}
}

// Search searches for a key in the B-Tree using CGO
func (bt *BTreeCGO) Search(key []byte) ([]byte, error) {
	if bt.handle == nil || len(key) == 0 {
		return nil, errors.New("invalid B-Tree or key")
	}

	var value *C.uint8_t
	var valueLen C.size_t

	ret := C.svdb_btree_search(
		bt.handle,
		(*C.uint8_t)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		&value,
		&valueLen,
	)

	if ret == 0 || value == nil {
		return nil, nil // Not found
	}

	defer C.free(unsafe.Pointer(value))

	result := make([]byte, valueLen)
	copy(result, C.GoBytes(unsafe.Pointer(value), C.int(valueLen)))
	return result, nil
}

// Helper function to convert Go bool to C int
func cBool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// BinarySearchCGO performs binary search in a page using CGO
func BinarySearchCGO(pageData []byte, key []byte, isTable bool) (int, error) {
	if len(pageData) < 8 || len(key) == 0 {
		return -1, errors.New("invalid parameters")
	}

	ret := C.svdb_btree_binary_search(
		(*C.uint8_t)(unsafe.Pointer(&pageData[0])),
		C.size_t(len(pageData)),
		(*C.uint8_t)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		cBool(isTable),
	)

	return int(ret), nil
}
