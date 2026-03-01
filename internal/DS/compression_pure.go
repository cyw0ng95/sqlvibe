//go:build !SVDB_ENABLE_CGO_DS
// +build !SVDB_ENABLE_CGO_DS

package DS

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
)

// CompressLZ4 compresses data using flate (pure Go fallback for LZ4)
func CompressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressLZ4 decompresses flate data (pure Go fallback)
func DecompressLZ4(data []byte, maxSize int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	return io.ReadAll(r)
}

// CompressZSTD compresses data using gzip (pure Go fallback for ZSTD)
func CompressZSTD(data []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressZSTD decompresses gzip data (pure Go fallback)
func DecompressZSTD(data []byte, maxSize int) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// GetDefaultZSTDLevel returns default compression level
func GetDefaultZSTDLevel() int {
	return flate.DefaultCompression
}
