
package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "hash.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

// Hash64 computes xxHash64 of data
func Hash64(data []byte, seed uint64) uint64 {
	if len(data) == 0 {
		return seed
	}
	return uint64(C.svdb_xxhash64(
		unsafe.Pointer(&data[0]),
		C.size_t(len(data)),
		C.uint64_t(seed),
	))
}

// Hash64Batch hashes multiple keys efficiently
func Hash64Batch(keys [][]byte, seed uint64) []uint64 {
	if len(keys) == 0 {
		return nil
	}

	// Prepare C arrays
	keyPtrs := make([]*C.uint8_t, len(keys))
	keyLens := make([]C.size_t, len(keys))
	hashes := make([]uint64, len(keys))

	for i, key := range keys {
		if len(key) > 0 {
			keyPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&key[0]))
		}
		keyLens[i] = C.size_t(len(key))
	}

	C.svdb_xxhash64_batch(
		&keyPtrs[0],
		&keyLens[0],
		(*C.uint64_t)(unsafe.Pointer(&hashes[0])),
		C.size_t(len(keys)),
		C.uint64_t(seed),
	)

	return hashes
}

// Hash32 computes xxHash32 of data
func Hash32(data []byte, seed uint32) uint32 {
	if len(data) == 0 {
		return seed
	}
	return uint32(C.svdb_xxhash32(
		unsafe.Pointer(&data[0]),
		C.size_t(len(data)),
		C.uint32_t(seed),
	))
}

// Hash32Batch hashes multiple keys with 32-bit hash
func Hash32Batch(keys [][]byte, seed uint32) []uint32 {
	if len(keys) == 0 {
		return nil
	}

	keyPtrs := make([]*C.uint8_t, len(keys))
	keyLens := make([]C.size_t, len(keys))
	hashes := make([]uint32, len(keys))

	for i, key := range keys {
		if len(key) > 0 {
			keyPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&key[0]))
		}
		keyLens[i] = C.size_t(len(key))
	}

	C.svdb_xxhash32_batch(
		&keyPtrs[0],
		&keyLens[0],
		(*C.uint32_t)(unsafe.Pointer(&hashes[0])),
		C.size_t(len(keys)),
		C.uint32_t(seed),
	)

	return hashes
}

// HashInt64 computes fast hash for int64
func HashInt64(value int64, seed uint64) uint64 {
	return uint64(C.svdb_hash_int64(C.int64_t(value), C.uint64_t(seed)))
}

// HashInt64Pair computes fast hash for two int64 values (composite key)
func HashInt64Pair(a, b int64, seed uint64) uint64 {
	return uint64(C.svdb_hash_int64_pair(C.int64_t(a), C.int64_t(b), C.uint64_t(seed)))
}
