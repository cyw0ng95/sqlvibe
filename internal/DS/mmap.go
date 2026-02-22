package DS

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"os"
	"syscall"
)

// MmapFile provides memory-mapped read access to a SQLVIBE database file.
// On supported platforms the entire file is mapped into virtual memory with
// MAP_SHARED | PROT_READ so that the OS page cache handles buffering and
// individual column reads are zero-copy.
//
// The type implements io.Closer; callers must call Close to release the mapping.
type MmapFile struct {
	data []byte
	file *os.File
}

// OpenMmap opens path and maps its contents into memory.
// Returns an error if the file cannot be opened or mapped.
func OpenMmap(path string) (*MmapFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mmap open: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap stat: %w", err)
	}
	size := fi.Size()
	if size == 0 {
		// Empty file: no data to map.  Return a valid but zero-size reader.
		return &MmapFile{data: nil, file: f}, nil
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return &MmapFile{data: data, file: f}, nil
}

// Close unmaps the file and closes the underlying file descriptor.
func (m *MmapFile) Close() error {
	var munmapErr error
	if m.data != nil {
		munmapErr = syscall.Munmap(m.data)
		m.data = nil
	}
	fileErr := m.file.Close()
	if munmapErr != nil {
		return munmapErr
	}
	return fileErr
}

// Len returns the total mapped length in bytes.
func (m *MmapFile) Len() int { return len(m.data) }

// ReadBytes returns a slice of the mapped region [offset, offset+length).
// The returned slice is valid until Close is called.
func (m *MmapFile) ReadBytes(offset, length int) ([]byte, error) {
	if offset < 0 || length < 0 || offset+length > len(m.data) {
		return nil, fmt.Errorf("mmap ReadBytes: range [%d, %d) out of bounds (file size %d)", offset, offset+length, len(m.data))
	}
	return m.data[offset : offset+length], nil
}

// ReadDatabaseMmap reads a SQLVIBE v1.0.0 database file using memory mapping
// for zero-copy column access on large files.  It validates checksums exactly
// as ReadDatabase does, and returns the same types.
//
// On platforms where mmap is unavailable the function transparently falls back
// to ReadDatabase.
func ReadDatabaseMmap(path string) (*HybridStore, map[string]interface{}, error) {
	mf, err := OpenMmap(path)
	if err != nil {
		// Fall back to regular read if mmap fails.
		return ReadDatabase(path)
	}
	defer mf.Close()

	data := mf.data
	if len(data) < HeaderSize+FooterSize {
		return nil, nil, fmt.Errorf("file too small: %d bytes", len(data))
	}

	// Validate footer.
	footerStart := len(data) - FooterSize
	ft, err := unmarshalFooter(data[footerStart:])
	if err != nil {
		return nil, nil, fmt.Errorf("parse footer: %w", err)
	}
	if string(ft.Magic[:]) != FooterMagic {
		return nil, nil, fmt.Errorf("invalid footer magic")
	}
	wantFileCRC := crc64.Checksum(data[:footerStart], crc64Table)
	if ft.FileCRC != wantFileCRC {
		return nil, nil, fmt.Errorf("file CRC mismatch: got %x, want %x", ft.FileCRC, wantFileCRC)
	}

	// Parse and validate header.
	hdr, err := unmarshalHeader(data[:HeaderSize])
	if err != nil {
		return nil, nil, fmt.Errorf("parse header: %w", err)
	}
	if string(hdr.Magic[:]) != MagicBytes {
		return nil, nil, fmt.Errorf("invalid magic bytes")
	}
	wantHdrCRC := crc64.Checksum(data[:248], crc64Table)
	if hdr.HeaderCRC64 != wantHdrCRC {
		return nil, nil, fmt.Errorf("header CRC mismatch: got %x, want %x", hdr.HeaderCRC64, wantHdrCRC)
	}

	// Read schema JSON.
	schemaEnd := int(hdr.SchemaOffset) + int(hdr.SchemaLength)
	if schemaEnd > footerStart {
		return nil, nil, fmt.Errorf("schema extends beyond data section")
	}
	schemaJSON := data[hdr.SchemaOffset:schemaEnd]
	var schema map[string]interface{}
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return nil, nil, fmt.Errorf("unmarshal schema: %w", err)
	}

	// Extract column metadata.
	colNamesRaw, _ := schema["column_names"].([]interface{})
	columns := make([]string, len(colNamesRaw))
	for i, n := range colNamesRaw {
		columns[i] = fmt.Sprintf("%v", n)
	}
	colTypes := extractColTypes(schema, columns)
	rowCount := int(hdr.RowCount)
	compressionType := hdr.CompressionType

	// Decode column data sequentially using the mapped bytes (zero-copy for no-compression path).
	pos := schemaEnd
	colVectors := make([]*ColumnVector, len(columns))
	for ci, col := range columns {
		var rawData []byte
		if compressionType != CompressionNone {
			if pos+8 > footerStart {
				return nil, nil, fmt.Errorf("column %d: size header extends beyond data", ci)
			}
			rawSize := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			compressedSize := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
			pos += 8
			if pos+compressedSize > footerStart {
				return nil, nil, fmt.Errorf("column %d: compressed data extends beyond data section", ci)
			}
			var decErr error
			rawData, decErr = decompressColumnData(data[pos:pos+compressedSize], compressionType)
			if decErr != nil {
				return nil, nil, fmt.Errorf("decompress column %d (%s): %w", ci, col, decErr)
			}
			if rawSize > 0 && len(rawData) != rawSize {
				return nil, nil, fmt.Errorf("column %d (%s): decompressed size %d != expected %d", ci, col, len(rawData), rawSize)
			}
			pos += compressedSize
		} else {
			// Zero-copy: slice directly from the mmap'd region.
			size := columnDataSize(data[pos:], colTypes[ci], rowCount)
			rawData = data[pos : pos+size]
			pos += size
		}
		cv, err := decodeColumnData(rawData, col, colTypes[ci], rowCount)
		if err != nil {
			return nil, nil, fmt.Errorf("decode column %d (%s): %w", ci, col, err)
		}
		colVectors[ci] = cv
	}

	// Build HybridStore and insert rows.
	hs := NewHybridStore(columns, colTypes)
	for i := 0; i < rowCount; i++ {
		vals := make([]Value, len(columns))
		for ci := range columns {
			vals[ci] = colVectors[ci].Get(i)
		}
		hs.Insert(vals)
	}

	return hs, schema, nil
}
