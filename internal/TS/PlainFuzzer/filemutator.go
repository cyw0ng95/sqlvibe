package PlainFuzzer

import (
	"math/rand"
)

// FileMutator applies random byte-level mutations to a binary database file.
type FileMutator struct {
	rng *rand.Rand
}

// NewFileMutator creates a FileMutator seeded with the provided value.
func NewFileMutator(seed int64) *FileMutator {
	return &FileMutator{rng: rand.New(rand.NewSource(seed))} //nolint:gosec
}

// Mutate applies one randomly chosen mutation strategy to data and returns the
// mutated copy.  The original slice is never modified.
func (m *FileMutator) Mutate(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	strategy := m.rng.Intn(6)
	switch strategy {
	case 0:
		return m.mutateHeader(data)
	case 1:
		return m.mutateTruncate(data)
	case 2:
		return m.mutateByteFlip(data)
	case 3:
		return m.mutateStructure(data)
	case 4:
		return m.mutateFooter(data)
	default:
		return m.mutatePadding(data)
	}
}

// mutateHeader corrupts the first 16 bytes (magic / version / flags).
func (m *FileMutator) mutateHeader(data []byte) []byte {
	out := copyBytes(data)
	limit := 16
	if len(out) < limit {
		limit = len(out)
	}
	for i := 0; i < limit; i++ {
		out[i] = byte(m.rng.Intn(256))
	}
	return out
}

// mutateTruncate cuts the file at a random offset.
func (m *FileMutator) mutateTruncate(data []byte) []byte {
	if len(data) <= 1 {
		return []byte{}
	}
	cutAt := m.rng.Intn(len(data))
	out := make([]byte, cutAt)
	copy(out, data[:cutAt])
	return out
}

// mutateByteFlip flips random bits in one random byte.
func (m *FileMutator) mutateByteFlip(data []byte) []byte {
	out := copyBytes(data)
	pos := m.rng.Intn(len(out))
	out[pos] ^= byte(m.rng.Intn(256))
	return out
}

// mutateStructure corrupts a random range of bytes in the middle of the file.
func (m *FileMutator) mutateStructure(data []byte) []byte {
	out := copyBytes(data)
	if len(out) < 2 {
		return out
	}
	start := m.rng.Intn(len(out))
	length := m.rng.Intn(len(out)-start) + 1
	for i := start; i < start+length && i < len(out); i++ {
		out[i] = byte(m.rng.Intn(256))
	}
	return out
}

// mutateFooter zeroes or corrupts the last 32 bytes (footer / CRC region).
func (m *FileMutator) mutateFooter(data []byte) []byte {
	out := copyBytes(data)
	footerStart := len(out) - 32
	if footerStart < 0 {
		footerStart = 0
	}
	for i := footerStart; i < len(out); i++ {
		out[i] = 0
	}
	return out
}

// mutatePadding injects null bytes at a random offset inside the file.
func (m *FileMutator) mutatePadding(data []byte) []byte {
	insertAt := m.rng.Intn(len(data) + 1)
	padLen := m.rng.Intn(128) + 1
	out := make([]byte, len(data)+padLen)
	copy(out, data[:insertAt])
	// padLen zero bytes are already zero-initialized by make
	copy(out[insertAt+padLen:], data[insertAt:])
	return out
}

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
