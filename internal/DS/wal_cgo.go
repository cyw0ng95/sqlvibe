package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "wal.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// WALOp constants match the C++ WAL operation types.
const (
	CWALInsert = 1
	CWALDelete = 2
	CWALUpdate = 3
)

// WALEntryTotalSize returns the encoded byte size for a WAL record whose JSON
// body is jsonLen bytes long (= 4 + jsonLen).
func WALEntryTotalSize(jsonLen int) int {
	return int(C.svdb_wal_entry_total_size(C.size_t(jsonLen)))
}

// WALEncodeEntry writes a length-prefixed WAL record into buf.
// jsonData is the raw JSON body.
// Returns the number of bytes written, or 0 on error (buf too small).
func WALEncodeEntry(buf []byte, jsonData []byte) int {
	if len(buf) == 0 || len(jsonData) == 0 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	pinner.Pin(&jsonData[0])
	defer pinner.Unpin()
	var written C.size_t
	ok := C.svdb_wal_encode_entry(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
		(*C.uint8_t)(unsafe.Pointer(&jsonData[0])), C.size_t(len(jsonData)),
		&written,
	)
	if ok == 0 {
		return 0
	}
	return int(written)
}

// WALDecodeEntryLength reads the 4-byte little-endian length prefix from buf.
// Returns the JSON body length, or 0 if buf is shorter than 4 bytes.
func WALDecodeEntryLength(buf []byte) uint32 {
	if len(buf) < 4 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	defer pinner.Unpin()
	return uint32(C.svdb_wal_decode_entry_length(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))))
}

// WALDecodeEntryBody returns the JSON body slice at buf[offset+4:].
// Returns nil if the buffer does not contain a complete record at offset.
func WALDecodeEntryBody(buf []byte, offset int) []byte {
	if len(buf) == 0 || offset < 0 || offset >= len(buf) {
		return nil
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	defer pinner.Unpin()
	var outBody *C.uint8_t
	var outLen C.size_t
	ok := C.svdb_wal_decode_entry_body(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
		C.size_t(offset), &outBody, &outLen,
	)
	if ok == 0 || outBody == nil || outLen == 0 {
		return nil
	}
	// Return a copy so the caller doesn't hold a pointer into C memory.
	src := unsafe.Slice((*byte)(unsafe.Pointer(outBody)), int(outLen))
	dst := make([]byte, int(outLen))
	copy(dst, src)
	return dst
}

// WALIsValidEntry returns true if buf contains a complete, valid WAL record.
func WALIsValidEntry(buf []byte) bool {
	if len(buf) == 0 {
		return false
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	defer pinner.Unpin()
	return C.svdb_wal_is_valid_entry(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) != 0
}

// WALCreateInsertRecord encodes an insert WAL record into buf.
// jsonVals is the JSON value array, e.g. [{"t":3,"s":"hello"},{"t":1,"i":42}].
// Returns the number of bytes written (4 + body_len), or 0 on error.
func WALCreateInsertRecord(buf []byte, jsonVals []byte) int {
	if len(buf) == 0 || len(jsonVals) == 0 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	pinner.Pin(&jsonVals[0])
	defer pinner.Unpin()
	ok := C.svdb_wal_create_insert_record(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
		(*C.uint8_t)(unsafe.Pointer(&jsonVals[0])), C.size_t(len(jsonVals)),
	)
	if ok == 0 {
		return 0
	}
	// The C function writes a length prefix; read it back to get total bytes.
	return int(WALDecodeEntryLength(buf)) + 4
}

// WALCreateDeleteRecord encodes a delete WAL record into buf.
// idx is the row index to delete.
// Returns the number of bytes written, or 0 on error.
func WALCreateDeleteRecord(buf []byte, idx int64) int {
	if len(buf) == 0 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	defer pinner.Unpin()
	ok := C.svdb_wal_create_delete_record(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
		C.int64_t(idx),
	)
	if ok == 0 {
		return 0
	}
	// {"op":2,"idx":<idx>} - estimate: total bytes written = WALEntryTotalSize(body_len)
	// We ask the C layer for the exact count by checking what was actually written.
	return int(WALDecodeEntryLength(buf)) + 4
}

// WALCreateUpdateRecord encodes an update WAL record into buf.
// idx is the row index to update; jsonVals is the new JSON value array.
// Returns the number of bytes written, or 0 on error.
func WALCreateUpdateRecord(buf []byte, idx int64, jsonVals []byte) int {
	if len(buf) == 0 || len(jsonVals) == 0 {
		return 0
	}
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	pinner.Pin(&jsonVals[0])
	defer pinner.Unpin()
	ok := C.svdb_wal_create_update_record(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
		C.int64_t(idx),
		(*C.uint8_t)(unsafe.Pointer(&jsonVals[0])), C.size_t(len(jsonVals)),
	)
	if ok == 0 {
		return 0
	}
	return int(WALDecodeEntryLength(buf)) + 4
}
