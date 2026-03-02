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
	strategy := m.rng.Intn(12)
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
	case 5:
		return m.mutatePadding(data)
	case 6:
		return m.mutateSwapBytes(data)
	case 7:
		return m.mutateRepeatSection(data)
	case 8:
		return m.mutateShift(data)
	case 9:
		return m.mutateDuplicateSection(data)
	case 10:
		return m.mutateNegativeSize(data)
	default:
		return m.mutateFragment(data)
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
	maxLength := len(out) - start
	if maxLength <= 0 {
		return out
	}
	length := m.rng.Intn(maxLength) + 1
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

// mutateSwapBytes swaps two random bytes in the file
func (m *FileMutator) mutateSwapBytes(data []byte) []byte {
	if len(data) < 2 {
		return m.mutateByteFlip(data)
	}
	out := copyBytes(data)
	i := m.rng.Intn(len(out))
	j := m.rng.Intn(len(out))
	out[i], out[j] = out[j], out[i]
	return out
}

// mutateRepeatSection repeats a section of bytes multiple times
func (m *FileMutator) mutateRepeatSection(data []byte) []byte {
	if len(data) < 4 {
		return data
	}
	half := len(data) / 2
	if half < 1 {
		return data
	}
	start := m.rng.Intn(half)
	endRange := half - start
	if endRange <= 0 {
		endRange = 1
	}
	end := start + m.rng.Intn(endRange) + 1
	if end > len(data) {
		end = len(data)
	}
	section := data[start:end]
	repeat := m.rng.Intn(3) + 1 // repeat 1-3 times

	out := make([]byte, 0, start+len(section)*repeat+len(data)-end)
	out = append(out, data[:start]...)
	for i := 0; i < repeat; i++ {
		out = append(out, section...)
	}
	out = append(out, data[end:]...)
	return out
}

// mutateShift shifts all bytes by a random offset (rotates within the file)
func (m *FileMutator) mutateShift(data []byte) []byte {
	if len(data) < 2 {
		return data
	}
	shift := m.rng.Intn(len(data))
	out := make([]byte, len(data))
	for i := 0; i < len(data); i++ {
		out[(i+shift)%len(data)] = data[i]
	}
	return out
}

// mutateDuplicateSection duplicates a section of bytes and inserts it
func (m *FileMutator) mutateDuplicateSection(data []byte) []byte {
	if len(data) < 4 {
		return data
	}
	half := len(data) / 2
	if half < 1 {
		return data
	}
	start := m.rng.Intn(half)
	endRange := half - start
	if endRange <= 0 {
		endRange = 1
	}
	end := start + m.rng.Intn(endRange) + 1
	if end > len(data) {
		end = len(data)
	}
	section := make([]byte, end-start)
	copy(section, data[start:end])

	out := make([]byte, 0, len(data)+len(section))
	out = append(out, data[:start]...)
	out = append(out, section...)
	out = append(out, data[start:]...)
	return out
}

// mutateNegativeSize writes a negative size value at a random offset
func (m *FileMutator) mutateNegativeSize(data []byte) []byte {
	if len(data) < 4 {
		return m.mutateByteFlip(data)
	}
	out := copyBytes(data)
	maxPos := len(out) - 4
	if maxPos <= 0 {
		return m.mutateByteFlip(data)
	}
	pos := m.rng.Intn(maxPos)
	negativeValue := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	copy(out[pos:], negativeValue)
	return out
}

// mutateFragment replaces a section with random garbage
func (m *FileMutator) mutateFragment(data []byte) []byte {
	if len(data) < 8 {
		return m.mutateByteFlip(data)
	}
	out := copyBytes(data)
	maxStart := len(out) - 8
	if maxStart <= 0 {
		return m.mutateByteFlip(data)
	}
	start := m.rng.Intn(maxStart)
	length := m.rng.Intn(8) + 1
	for i := start; i < start+length && i < len(out); i++ {
		out[i] = byte(m.rng.Intn(256))
	}
	return out
}

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
