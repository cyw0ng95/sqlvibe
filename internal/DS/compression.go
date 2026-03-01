package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "compression.h"
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unsafe"
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
// LZ4Compressor — C++ LZ4 compression.
// ---------------------------------------------------------------------------

const maxDecompressedSize = 256 * 1024 * 1024 // 256 MB guard against decompression bombs

type LZ4Compressor struct{}

func (c *LZ4Compressor) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	
	// Get max compressed size from C++
	maxSize := C.svdb_lz4_compress_bound(C.size_t(len(src)))
	out := make([]byte, maxSize)
	
	result := C.svdb_lz4_compress(
		(*C.uint8_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(maxSize),
	)
	
	if result == 0 {
		return nil, fmt.Errorf("LZ4 compression failed")
	}
	
	return out[:result], nil
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
	
	out := make([]byte, origLen)
	result := C.svdb_lz4_decompress(
		(*C.uint8_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(origLen),
	)
	
	if result == 0 {
		return nil, fmt.Errorf("LZ4 decompression failed")
	}
	
	return out[:result], nil
}

func (c *LZ4Compressor) Name() string { return "LZ4" }

// ---------------------------------------------------------------------------
// ZSTDCompressor — C++ ZSTD compression.
// ---------------------------------------------------------------------------

type ZSTDCompressor struct{}

func (c *ZSTDCompressor) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	
	level := C.svdb_zstd_default_compression_level()
	// Estimate compressed size (typically 2-10x compression)
	maxSize := len(src)
	if maxSize < 1024 {
		maxSize = 1024
	}
	out := make([]byte, maxSize)
	
	result := C.svdb_zstd_compress(
		(*C.uint8_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(maxSize),
		level,
	)
	
	if result == 0 {
		// Try with larger buffer
		maxSize = len(src) * 2
		out = make([]byte, maxSize)
		result = C.svdb_zstd_compress(
			(*C.uint8_t)(unsafe.Pointer(&src[0])),
			C.size_t(len(src)),
			(*C.uint8_t)(unsafe.Pointer(&out[0])),
			C.size_t(maxSize),
			level,
		)
		if result == 0 {
			return nil, fmt.Errorf("ZSTD compression failed")
		}
	}
	
	return out[:result], nil
}

func (c *ZSTDCompressor) Decompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	
	// Start with estimated output size
	outSize := len(src) * 10
	if outSize < 1024 {
		outSize = 1024
	}
	if outSize > maxDecompressedSize {
		outSize = maxDecompressedSize
	}
	
	out := make([]byte, outSize)
	result := C.svdb_zstd_decompress(
		(*C.uint8_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(outSize),
	)
	
	if result == 0 {
		return nil, fmt.Errorf("ZSTD decompression failed")
	}
	
	return out[:result], nil
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
