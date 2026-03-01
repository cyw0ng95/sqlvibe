//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import (
	"hash/fnv"
)

// Hash64 computes FNV-1a 64-bit hash (pure Go fallback)
func Hash64(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	h.Write(toBytesWithSeed(seed))
	h.Write(data)
	return h.Sum64()
}

// Hash64Batch hashes multiple keys (pure Go fallback)
func Hash64Batch(keys [][]byte, seed uint64) []uint64 {
	hashes := make([]uint64, len(keys))
	for i, key := range keys {
		hashes[i] = Hash64(key, seed)
	}
	return hashes
}

// Hash32 computes FNV-1a 32-bit hash (pure Go fallback)
func Hash32(data []byte, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write(toBytesWithSeed32(seed))
	h.Write(data)
	return h.Sum32()
}

// Hash32Batch hashes multiple keys with 32-bit hash (pure Go fallback)
func Hash32Batch(keys [][]byte, seed uint32) []uint32 {
	hashes := make([]uint32, len(keys))
	for i, key := range keys {
		hashes[i] = Hash32(key, seed)
	}
	return hashes
}

// HashInt64 computes hash for int64 (pure Go fallback)
func HashInt64(value int64, seed uint64) uint64 {
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(value >> (uint(i) * 8))
	}
	return Hash64(buf[:], seed)
}

// HashInt64Pair computes hash for two int64 values (pure Go fallback)
func HashInt64Pair(a, b int64, seed uint64) uint64 {
	var buf [16]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(a >> (uint(i) * 8))
		buf[i+8] = byte(b >> (uint(i) * 8))
	}
	return Hash64(buf[:], seed)
}

func toBytesWithSeed(seed uint64) []byte {
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(seed >> (uint(i) * 8))
	}
	return buf[:]
}

func toBytesWithSeed32(seed uint32) []byte {
	var buf [4]byte
	for i := 0; i < 4; i++ {
		buf[i] = byte(seed >> (uint(i) * 8))
	}
	return buf[:]
}
