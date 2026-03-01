//go:build SVDB_ENABLE_CGO_DS
// +build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_ds
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "compression.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

var (
	errCompressionFailed    = errors.New("compression failed")
	errDecompressionFailed  = errors.New("decompression failed")
)

// CompressLZ4 compresses data using LZ4
func CompressLZ4(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Allocate output buffer
	maxSize := C.svdb_lz4_compress_bound(C.size_t(len(data)))
	output := make([]byte, maxSize)

	compressedSize := C.svdb_lz4_compress(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&output[0])),
		C.size_t(maxSize),
	)

	if compressedSize == 0 {
		return nil, errCompressionFailed
	}

	return output[:compressedSize], nil
}

// DecompressLZ4 decompresses LZ4 data
func DecompressLZ4(data []byte, maxSize int) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	output := make([]byte, maxSize)

	decompressedSize := C.svdb_lz4_decompress(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&output[0])),
		C.size_t(maxSize),
	)

	if decompressedSize == 0 {
		return nil, errDecompressionFailed
	}

	return output[:decompressedSize], nil
}

// CompressZSTD compresses data using ZSTD
func CompressZSTD(data []byte, level int) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Allocate output buffer (ZSTD compress bound)
	maxSize := len(data) + 1024
	output := make([]byte, maxSize)

	compressedSize := C.svdb_zstd_compress(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&output[0])),
		C.size_t(maxSize),
		C.int(level),
	)

	if compressedSize == 0 {
		return nil, errCompressionFailed
	}

	return output[:compressedSize], nil
}

// DecompressZSTD decompresses ZSTD data
func DecompressZSTD(data []byte, maxSize int) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	output := make([]byte, maxSize)

	decompressedSize := C.svdb_zstd_decompress(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&output[0])),
		C.size_t(maxSize),
	)

	if decompressedSize == 0 {
		return nil, errDecompressionFailed
	}

	return output[:decompressedSize], nil
}

// GetDefaultZSTDLevel returns the default ZSTD compression level
func GetDefaultZSTDLevel() int {
	return int(C.svdb_zstd_default_compression_level())
}
