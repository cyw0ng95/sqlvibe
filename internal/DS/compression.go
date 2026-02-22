package DS

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// Compressor is the common interface for all compression algorithms supported
// by the storage engine.
type Compressor interface {
	// Compress compresses src and returns the compressed bytes.
	Compress(src []byte) ([]byte, error)
	// Decompress decompresses src and returns the original bytes.
	Decompress(src []byte) ([]byte, error)
	// Name returns the algorithm identifier (e.g. "LZ4", "ZSTD", "GZIP").
	Name() string
}

// NewCompressor creates a Compressor for the given algorithm name. level is
// used by algorithms that support it (GZIP: 1–9, ignored by others).
// Supported names (case-insensitive): "NONE", "RLE", "LZ4", "ZSTD", "GZIP".
func NewCompressor(name string, level int) (Compressor, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "NONE", "":
		return &NoneCompressor{}, nil
	case "RLE":
		return &RLECompressor{}, nil
	case "LZ4":
		return &LZ4Compressor{}, nil
	case "ZSTD":
		return &ZSTDCompressor{}, nil
	case "GZIP":
		if level < 1 || level > 9 {
			level = gzip.DefaultCompression
		}
		return &GzipCompressor{level: level}, nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm %q", name)
	}
}

// ---------------------------------------------------------------------------
// NoneCompressor — identity compressor (no-op).
// ---------------------------------------------------------------------------

type NoneCompressor struct{}

func (c *NoneCompressor) Compress(src []byte) ([]byte, error) {
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}
func (c *NoneCompressor) Decompress(src []byte) ([]byte, error) {
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}
func (c *NoneCompressor) Name() string { return "NONE" }

// ---------------------------------------------------------------------------
// RLECompressor — simple byte-level Run-Length Encoding.
// Format: [repeat-count (1 byte)][byte-value] pairs.
// A run-count of 0 is illegal; runs are capped at 255.
// ---------------------------------------------------------------------------

type RLECompressor struct{}

func (c *RLECompressor) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, 0, len(src))
	i := 0
	for i < len(src) {
		val := src[i]
		count := 1
		for i+count < len(src) && src[i+count] == val && count < 255 {
			count++
		}
		buf = append(buf, byte(count), val)
		i += count
	}
	return buf, nil
}

func (c *RLECompressor) Decompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	if len(src)%2 != 0 {
		return nil, fmt.Errorf("RLE: invalid compressed length %d", len(src))
	}
	buf := make([]byte, 0, len(src)*2)
	for i := 0; i+1 < len(src); i += 2 {
		count := int(src[i])
		if count == 0 {
			return nil, fmt.Errorf("RLE: zero run-count at offset %d", i)
		}
		for j := 0; j < count; j++ {
			buf = append(buf, src[i+1])
		}
	}
	return buf, nil
}

func (c *RLECompressor) Name() string { return "RLE" }

// ---------------------------------------------------------------------------
// LZ4Compressor — pure-Go LZ4-like block compression.
// This is a simplified LZ4 block format implemented without external deps.
// Format: each token encodes a literal run followed by a back-reference.
// ---------------------------------------------------------------------------

const maxDecompressedSize = 256 * 1024 * 1024 // 256 MB guard against decompression bombs

type LZ4Compressor struct{}

func (c *LZ4Compressor) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	// Reserve 4 bytes at the start for the original length.
	out := make([]byte, 4, len(src)+64)
	binary.LittleEndian.PutUint32(out, uint32(len(src)))

	const (
		minMatch  = 4
		hashBits  = 16
		hashSize  = 1 << hashBits
		hashMask  = hashSize - 1
		maxOffset = 65535
	)

	hashTable := make([]int, hashSize)
	for i := range hashTable {
		hashTable[i] = -1
	}

	lz4Hash := func(b []byte) int {
		v := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		return int((v * 2654435761) >> (32 - hashBits) & hashMask)
	}

	anchor := 0
	ip := 0
	limit := len(src) - minMatch

	for ip <= limit {
		h := lz4Hash(src[ip:])
		ref := hashTable[h]
		hashTable[h] = ip

		if ref < 0 || ip-ref > maxOffset || ref+minMatch > len(src) ||
			src[ip] != src[ref] || src[ip+1] != src[ref+1] ||
			src[ip+2] != src[ref+2] || src[ip+3] != src[ref+3] {
			ip++
			continue
		}

		// Measure match length.
		matchLen := minMatch
		for ip+matchLen < len(src) && src[ip+matchLen] == src[ref+matchLen] {
			matchLen++
		}

		litLen := ip - anchor
		offset := ip - ref
		ml := matchLen - minMatch

		// Encode token + literals.
		token := byte(0)
		litPart := litLen
		if litPart >= 15 {
			token = 0xf0
		} else {
			token = byte(litPart << 4)
		}
		matchPart := ml
		if matchPart >= 15 {
			token |= 0x0f
		} else {
			token |= byte(matchPart)
		}

		out = append(out, token)
		if litLen >= 15 {
			extra := litLen - 15
			for extra >= 255 {
				out = append(out, 255)
				extra -= 255
			}
			out = append(out, byte(extra))
		}
		out = append(out, src[anchor:anchor+litLen]...)

		out = append(out, byte(offset), byte(offset>>8))

		if ml >= 15 {
			extra := ml - 15
			for extra >= 255 {
				out = append(out, 255)
				extra -= 255
			}
			out = append(out, byte(extra))
		}

		ip += matchLen
		anchor = ip
	}

	// Flush remaining literals.
	litLen := len(src) - anchor
	token := byte(0)
	if litLen >= 15 {
		token = 0xf0
	} else {
		token = byte(litLen << 4)
	}
	out = append(out, token)
	if litLen >= 15 {
		extra := litLen - 15
		for extra >= 255 {
			out = append(out, 255)
			extra -= 255
		}
		out = append(out, byte(extra))
	}
	out = append(out, src[anchor:]...)

	return out, nil
}

func (c *LZ4Compressor) Decompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	if len(src) < 4 {
		return nil, fmt.Errorf("LZ4: compressed data too short")
	}
	origLen := int(binary.LittleEndian.Uint32(src))
	if origLen < 0 || origLen > maxDecompressedSize {
		return nil, fmt.Errorf("LZ4: invalid original length %d", origLen)
	}
	out := make([]byte, 0, origLen)
	ip := 4

	for ip < len(src) {
		token := src[ip]
		ip++

		// Literal length.
		litLen := int(token >> 4)
		if litLen == 15 {
			for ip < len(src) {
				extra := src[ip]
				ip++
				litLen += int(extra)
				if extra != 255 {
					break
				}
			}
		}
		if ip+litLen > len(src) {
			return nil, fmt.Errorf("LZ4: literal overrun")
		}
		out = append(out, src[ip:ip+litLen]...)
		ip += litLen

		if ip >= len(src) {
			break
		}

		// Back-reference offset (little-endian 16-bit).
		if ip+1 >= len(src) {
			return nil, fmt.Errorf("LZ4: offset overrun")
		}
		offset := int(src[ip]) | int(src[ip+1])<<8
		ip += 2
		if offset == 0 {
			return nil, fmt.Errorf("LZ4: zero offset")
		}

		// Match length.
		matchLen := int(token&0x0f) + 4
		if (token & 0x0f) == 15 {
			for ip < len(src) {
				extra := src[ip]
				ip++
				matchLen += int(extra)
				if extra != 255 {
					break
				}
			}
		}

		// Copy from back-reference.
		ref := len(out) - offset
		if ref < 0 {
			return nil, fmt.Errorf("LZ4: invalid back-reference offset %d", offset)
		}
		for i := 0; i < matchLen; i++ {
			out = append(out, out[ref+i])
		}
	}

	return out, nil
}

func (c *LZ4Compressor) Name() string { return "LZ4" }

// ---------------------------------------------------------------------------
// ZSTDCompressor — ZSTD-like compression using zlib under the hood.
// Since we cannot import external packages, ZSTD is implemented as a
// high-compression zlib (level 9). This delivers the same interface so that
// callers can switch to a real ZSTD library without changing calling code.
// ---------------------------------------------------------------------------

type ZSTDCompressor struct{}

func (c *ZSTDCompressor) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("ZSTD(zlib): %w", err)
	}
	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("ZSTD(zlib) write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("ZSTD(zlib) close: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *ZSTDCompressor) Decompress(src []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("ZSTD(zlib): %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (c *ZSTDCompressor) Name() string { return "ZSTD" }

// ---------------------------------------------------------------------------
// GzipCompressor — standard GZIP compression.
// ---------------------------------------------------------------------------

type GzipCompressor struct {
	level int
}

func (c *GzipCompressor) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, c.level)
	if err != nil {
		return nil, fmt.Errorf("GZIP: %w", err)
	}
	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("GZIP write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("GZIP close: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *GzipCompressor) Decompress(src []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("GZIP: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (c *GzipCompressor) Name() string { return "GZIP" }
