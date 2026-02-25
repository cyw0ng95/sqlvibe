package DS

import (
	"encoding/binary"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure used to test
// whether an element is a member of a set. False positives are possible but
// false negatives are not.
type BloomFilter struct {
	bits []uint64 // bit array (each element is 64 bits)
	k    int      // number of hash functions
	m    int      // total number of bits
}

// NewBloomFilter creates a new BloomFilter sized for expectedItems with the given
// false-positive probability (0 < falsePositiveRate < 1).
// Uses k=2 hash functions derived from FNV-1a with different seeds.
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}
	// m = -n * ln(p) / (ln(2)^2)
	ln2sq := math.Log(2) * math.Log(2)
	m := int(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / ln2sq))
	if m < 64 {
		m = 64
	}
	// Round up to next multiple of 64
	m = ((m + 63) / 64) * 64
	k := 2
	return &BloomFilter{
		bits: make([]uint64, m/64),
		k:    k,
		m:    m,
	}
}

// Add inserts key into the bloom filter.
func (bf *BloomFilter) Add(key interface{}) {
	h1, h2 := bloomHash(key)
	for i := 0; i < bf.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(bf.m)
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

// MightContain returns true if key might be in the filter.
// Returns false only if key is definitely not in the filter.
func (bf *BloomFilter) MightContain(key interface{}) bool {
	h1, h2 := bloomHash(key)
	for i := 0; i < bf.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(bf.m)
		if bf.bits[pos/64]>>(pos%64)&1 == 0 {
			return false
		}
	}
	return true
}

// bloomHash returns two independent 64-bit hashes for key using FNV-1a.
// The two hashes use different seeds to simulate independent hash functions.
func bloomHash(key interface{}) (h1, h2 uint64) {
	// FNV-1a offset basis with two seeds
	const (
		seed1 = 14695981039346656037 // FNV-1a offset basis
		seed2 = 0xcbf29ce484222325   // alternative seed
		prime = 1099511628211
	)

	var buf [16]byte
	n := bloomKeyBytes(key, buf[:])

	h1 = seed1
	h2 = seed2
	for _, b := range buf[:n] {
		h1 ^= uint64(b)
		h1 *= prime
		h2 ^= uint64(b)
		h2 *= prime
	}
	return h1, h2
}

// bloomKeyBytes serialises key into buf and returns the number of bytes written.
func bloomKeyBytes(key interface{}, buf []byte) int {
	switch v := key.(type) {
	case int64:
		binary.LittleEndian.PutUint64(buf[:8], uint64(v))
		return 8
	case float64:
		binary.LittleEndian.PutUint64(buf[:8], math.Float64bits(v))
		return 8
	case string:
		n := copy(buf, v)
		return n
	case bool:
		if v {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
		return 1
	default:
		return 0
	}
}
