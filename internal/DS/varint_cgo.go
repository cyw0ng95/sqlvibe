package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "varint.h"
*/
import "C"
import "unsafe"

// CGetVarint decodes a varint from buf.
// Returns false if C returns 0 (buffer too small or invalid).
func CGetVarint(buf []byte) (value int64, bytesRead int, ok bool) {
	if len(buf) == 0 {
		return 0, 0, false
	}
	var cValue C.int64_t
	var cBytesRead C.int
	ret := C.svdb_get_varint((*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &cValue, &cBytesRead)
	if ret == 0 {
		return 0, 0, false
	}
	return int64(cValue), int(cBytesRead), true
}

// CPutVarint encodes value as a varint into buf.
// Returns the number of bytes written and true on success.
func CPutVarint(buf []byte, value int64) (bytesWritten int, ok bool) {
	if len(buf) == 0 {
		return 0, false
	}
	n := C.svdb_put_varint((*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), C.int64_t(value))
	if n == 0 {
		return 0, false
	}
	return int(n), true
}

// CVarintLen returns the number of bytes required to encode value as a varint.
func CVarintLen(value int64) int {
	return int(C.svdb_varint_len(C.int64_t(value)))
}

// CGetVarint32 decodes a varint that fits in 32 bits.
// Returns false if C returns 0.
func CGetVarint32(buf []byte) (value uint32, bytesRead int, ok bool) {
	if len(buf) == 0 {
		return 0, 0, false
	}
	var cValue C.uint32_t
	var cBytesRead C.int
	ret := C.svdb_get_varint32((*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &cValue, &cBytesRead)
	if ret == 0 {
		return 0, 0, false
	}
	return uint32(cValue), int(cBytesRead), true
}

// CPutVarint32 encodes a 32-bit value as a varint into buf.
// Returns the number of bytes written and true on success.
func CPutVarint32(buf []byte, value uint32) (bytesWritten int, ok bool) {
	if len(buf) == 0 {
		return 0, false
	}
	n := C.svdb_put_varint32((*C.uint8_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), C.uint32_t(value))
	if n == 0 {
		return 0, false
	}
	return int(n), true
}

// CBatchGetVarint decodes up to maxValues varints from buf.
// Returns the decoded values and total bytes consumed.
func CBatchGetVarint(buf []byte, maxValues int) (values []int64, totalBytes int) {
	if len(buf) == 0 || maxValues <= 0 {
		return nil, 0
	}
	out := make([]int64, maxValues)
	var cTotalBytes C.int
	n := C.svdb_batch_get_varint(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.int(maxValues),
		&cTotalBytes,
	)
	if n < 0 || int(n) > maxValues {
		n = C.int(maxValues)
	}
	return out[:int(n)], int(cTotalBytes)
}

// CBatchPutVarint encodes values as varints into buf.
// Returns the number of varints successfully encoded.
func CBatchPutVarint(buf []byte, values []int64) int {
	if len(values) == 0 {
		return 0
	}
	if len(buf) == 0 {
		return 0
	}
	n := C.svdb_batch_put_varint(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		(*C.int64_t)(unsafe.Pointer(&values[0])),
		C.int(len(values)),
	)
	return int(n)
}
