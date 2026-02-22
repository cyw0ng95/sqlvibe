package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"math"
	"os"
	"time"
)

// File format constants.
const (
	MagicBytes          = "SQLVIBE\x01"
	FooterMagic         = "SQLVIB\xFE\x01"
	FormatVersionMajor  = uint32(1)
	FormatVersionMinor  = uint32(0)
	FormatVersionPatch  = uint32(0)
	HeaderSize          = 256
	FooterSize          = 32
)

// crc64Table is the shared CRC64 table (ECMA polynomial).
var crc64Table = crc64.MakeTable(crc64.ECMA)

// FileHeader is the fixed 256-byte file header.
//
// Binary layout (all fields little-endian):
//   Offset  Size  Field
//   0       8     Magic
//   8       4     VersionMajor
//   12      4     VersionMinor
//   16      4     VersionPatch
//   20      4     Flags
//   24      4     SchemaOffset (always 256)
//   28      4     SchemaLength
//   32      4     ColumnCount
//   36      4     RowCount
//   40      4     IndexCount
//   44      4     CreatedAt (unix seconds)
//   48      4     ModifiedAt (unix seconds)
//   52      4     CompressionType (0 = none)
//   56      4     PageSize (0 = n/a)
//   60      188   Reserved
//   248     8     HeaderCRC64 (CRC64 of bytes 0..247)
type FileHeader struct {
	Magic           [8]byte
	VersionMajor    uint32
	VersionMinor    uint32
	VersionPatch    uint32
	Flags           uint32
	SchemaOffset    uint32
	SchemaLength    uint32
	ColumnCount     uint32
	RowCount        uint32
	IndexCount      uint32
	CreatedAt       uint32
	ModifiedAt      uint32
	CompressionType uint32
	PageSize        uint32
	Reserved        [188]byte
	HeaderCRC64     uint64
}

// Footer is the fixed 32-byte file footer.
//
// Binary layout:
//   Offset  Size  Field
//   0       8     Magic ("SQLVIB\xFE\x01")
//   8       8     FileCRC (CRC64 of all bytes before the footer)
//   16      4     RowCount
//   20      4     ColumnCount
//   24      8     Reserved
type Footer struct {
	Magic       [8]byte
	FileCRC     uint64
	RowCount    uint32
	ColumnCount uint32
	Reserved    [8]byte
}

// ColumnHeader describes serialized column metadata (used internally).
type ColumnHeader struct {
	Name       string
	Type       ValueType
	DataOffset uint64
	DataLength uint64
	RowCount   uint32
}

// marshalHeader serialises hdr into exactly HeaderSize bytes (little-endian).
func marshalHeader(hdr *FileHeader) []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:8], hdr.Magic[:])
	binary.LittleEndian.PutUint32(buf[8:], hdr.VersionMajor)
	binary.LittleEndian.PutUint32(buf[12:], hdr.VersionMinor)
	binary.LittleEndian.PutUint32(buf[16:], hdr.VersionPatch)
	binary.LittleEndian.PutUint32(buf[20:], hdr.Flags)
	binary.LittleEndian.PutUint32(buf[24:], hdr.SchemaOffset)
	binary.LittleEndian.PutUint32(buf[28:], hdr.SchemaLength)
	binary.LittleEndian.PutUint32(buf[32:], hdr.ColumnCount)
	binary.LittleEndian.PutUint32(buf[36:], hdr.RowCount)
	binary.LittleEndian.PutUint32(buf[40:], hdr.IndexCount)
	binary.LittleEndian.PutUint32(buf[44:], hdr.CreatedAt)
	binary.LittleEndian.PutUint32(buf[48:], hdr.ModifiedAt)
	binary.LittleEndian.PutUint32(buf[52:], hdr.CompressionType)
	binary.LittleEndian.PutUint32(buf[56:], hdr.PageSize)
	copy(buf[60:248], hdr.Reserved[:])
	binary.LittleEndian.PutUint64(buf[248:], hdr.HeaderCRC64)
	return buf
}

// unmarshalHeader parses a FileHeader from exactly HeaderSize bytes.
func unmarshalHeader(buf []byte) (*FileHeader, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("header too short: %d bytes", len(buf))
	}
	hdr := &FileHeader{}
	copy(hdr.Magic[:], buf[0:8])
	hdr.VersionMajor = binary.LittleEndian.Uint32(buf[8:])
	hdr.VersionMinor = binary.LittleEndian.Uint32(buf[12:])
	hdr.VersionPatch = binary.LittleEndian.Uint32(buf[16:])
	hdr.Flags = binary.LittleEndian.Uint32(buf[20:])
	hdr.SchemaOffset = binary.LittleEndian.Uint32(buf[24:])
	hdr.SchemaLength = binary.LittleEndian.Uint32(buf[28:])
	hdr.ColumnCount = binary.LittleEndian.Uint32(buf[32:])
	hdr.RowCount = binary.LittleEndian.Uint32(buf[36:])
	hdr.IndexCount = binary.LittleEndian.Uint32(buf[40:])
	hdr.CreatedAt = binary.LittleEndian.Uint32(buf[44:])
	hdr.ModifiedAt = binary.LittleEndian.Uint32(buf[48:])
	hdr.CompressionType = binary.LittleEndian.Uint32(buf[52:])
	hdr.PageSize = binary.LittleEndian.Uint32(buf[56:])
	copy(hdr.Reserved[:], buf[60:248])
	hdr.HeaderCRC64 = binary.LittleEndian.Uint64(buf[248:])
	return hdr, nil
}

// marshalFooter serialises a Footer into exactly FooterSize bytes.
func marshalFooter(ft *Footer) []byte {
	buf := make([]byte, FooterSize)
	copy(buf[0:8], ft.Magic[:])
	binary.LittleEndian.PutUint64(buf[8:], ft.FileCRC)
	binary.LittleEndian.PutUint32(buf[16:], ft.RowCount)
	binary.LittleEndian.PutUint32(buf[20:], ft.ColumnCount)
	copy(buf[24:], ft.Reserved[:])
	return buf
}

// unmarshalFooter parses a Footer from exactly FooterSize bytes.
func unmarshalFooter(buf []byte) (*Footer, error) {
	if len(buf) < FooterSize {
		return nil, fmt.Errorf("footer too short: %d bytes", len(buf))
	}
	ft := &Footer{}
	copy(ft.Magic[:], buf[0:8])
	ft.FileCRC = binary.LittleEndian.Uint64(buf[8:])
	ft.RowCount = binary.LittleEndian.Uint32(buf[16:])
	ft.ColumnCount = binary.LittleEndian.Uint32(buf[20:])
	copy(ft.Reserved[:], buf[24:])
	return ft, nil
}

// nullBitmapSize returns the number of bytes needed for a null bitmap of n rows.
func nullBitmapSize(n int) int {
	return (n + 7) / 8
}

// encodeColumnData encodes the null bitmap followed by all values for one column.
// rows is the full set of live rows; ci is the column index.
func encodeColumnData(rows [][]Value, ci int, typ ValueType, rowCount int) []byte {
	var buf bytes.Buffer

	// null bitmap
	bitmapBytes := nullBitmapSize(rowCount)
	bitmap := make([]byte, bitmapBytes)
	for i := 0; i < rowCount; i++ {
		var v Value
		if i < len(rows) && ci < len(rows[i]) {
			v = rows[i][ci]
		}
		if v.IsNull() {
			bitmap[i/8] |= 1 << uint(i%8)
		}
	}
	buf.Write(bitmap)

	// values (write slot for every row, zero/empty for nulls)
	for i := 0; i < rowCount; i++ {
		var v Value
		if i < len(rows) && ci < len(rows[i]) {
			v = rows[i][ci]
		}
		buf.Write(encodeValue(v, typ))
	}
	return buf.Bytes()
}

// encodeValue encodes a single value according to its storage type.
func encodeValue(v Value, typ ValueType) []byte {
	switch typ {
	case TypeInt, TypeBool:
		b := make([]byte, 8)
		if !v.IsNull() {
			binary.LittleEndian.PutUint64(b, uint64(v.Int))
		}
		return b
	case TypeFloat:
		b := make([]byte, 8)
		if !v.IsNull() {
			binary.LittleEndian.PutUint64(b, math.Float64bits(v.Float))
		}
		return b
	case TypeString:
		if v.IsNull() {
			return []byte{0, 0, 0, 0}
		}
		s := []byte(v.Str)
		b := make([]byte, 4+len(s))
		binary.LittleEndian.PutUint32(b[0:4], uint32(len(s)))
		copy(b[4:], s)
		return b
	case TypeBytes:
		if v.IsNull() {
			return []byte{0, 0, 0, 0}
		}
		b := make([]byte, 4+len(v.Bytes))
		binary.LittleEndian.PutUint32(b[0:4], uint32(len(v.Bytes)))
		copy(b[4:], v.Bytes)
		return b
	}
	return nil
}

// decodeColumnData decodes the null bitmap and value data for a column,
// returning a ColumnVector of rowCount elements.
func decodeColumnData(data []byte, name string, typ ValueType, rowCount int) (*ColumnVector, error) {
	cv := NewColumnVector(name, typ)
	bitmapBytes := nullBitmapSize(rowCount)
	if len(data) < bitmapBytes {
		return nil, fmt.Errorf("column %s: data too short for null bitmap", name)
	}
	bitmap := data[:bitmapBytes]
	pos := bitmapBytes

	for i := 0; i < rowCount; i++ {
		isNull := bitmap[i/8]>>uint(i%8)&1 == 1
		if isNull {
			// Advance past the null slot then append null
			skip, err := valueSize(data[pos:], typ)
			if err != nil {
				return nil, fmt.Errorf("column %s row %d: %w", name, i, err)
			}
			pos += skip
			cv.AppendNull()
			continue
		}
		v, n, err := decodeValue(data[pos:], typ)
		if err != nil {
			return nil, fmt.Errorf("column %s row %d: %w", name, i, err)
		}
		pos += n
		cv.Append(v)
	}
	return cv, nil
}

// valueSize returns how many bytes a single encoded value occupies.
func valueSize(data []byte, typ ValueType) (int, error) {
	switch typ {
	case TypeInt, TypeFloat, TypeBool:
		return 8, nil
	case TypeString, TypeBytes:
		if len(data) < 4 {
			return 0, fmt.Errorf("too short for length prefix")
		}
		l := int(binary.LittleEndian.Uint32(data[0:4]))
		return 4 + l, nil
	}
	return 0, fmt.Errorf("unknown type %d", typ)
}

// decodeValue decodes one value from data and returns (value, bytesConsumed, error).
func decodeValue(data []byte, typ ValueType) (Value, int, error) {
	switch typ {
	case TypeInt:
		if len(data) < 8 {
			return NullValue(), 0, fmt.Errorf("too short for int64")
		}
		return IntValue(int64(binary.LittleEndian.Uint64(data[0:8]))), 8, nil
	case TypeBool:
		if len(data) < 8 {
			return NullValue(), 0, fmt.Errorf("too short for bool")
		}
		return BoolValue(binary.LittleEndian.Uint64(data[0:8]) != 0), 8, nil
	case TypeFloat:
		if len(data) < 8 {
			return NullValue(), 0, fmt.Errorf("too short for float64")
		}
		bits := binary.LittleEndian.Uint64(data[0:8])
		return FloatValue(math.Float64frombits(bits)), 8, nil
	case TypeString:
		if len(data) < 4 {
			return NullValue(), 0, fmt.Errorf("too short for string length")
		}
		l := int(binary.LittleEndian.Uint32(data[0:4]))
		if len(data) < 4+l {
			return NullValue(), 0, fmt.Errorf("too short for string body")
		}
		return StringValue(string(data[4 : 4+l])), 4 + l, nil
	case TypeBytes:
		if len(data) < 4 {
			return NullValue(), 0, fmt.Errorf("too short for bytes length")
		}
		l := int(binary.LittleEndian.Uint32(data[0:4]))
		if len(data) < 4+l {
			return NullValue(), 0, fmt.Errorf("too short for bytes body")
		}
		b := make([]byte, l)
		copy(b, data[4:4+l])
		return BytesValue(b), 4 + l, nil
	}
	return NullValue(), 0, fmt.Errorf("unknown type %d", typ)
}

// extractColTypes reads column types from the schema map.
// Falls back to TypeString for missing/unknown entries.
func extractColTypes(schema map[string]interface{}, columns []string) []ValueType {
	colTypes := make([]ValueType, len(columns))
	for i := range colTypes {
		colTypes[i] = TypeString
	}
	raw, ok := schema["column_types"]
	if !ok {
		return colTypes
	}
	switch v := raw.(type) {
	case []ValueType:
		for i, t := range v {
			if i < len(colTypes) {
				colTypes[i] = t
			}
		}
	case []int:
		for i, t := range v {
			if i < len(colTypes) {
				colTypes[i] = ValueType(t)
			}
		}
	case []interface{}:
		for i, t := range v {
			if i >= len(colTypes) {
				break
			}
			switch n := t.(type) {
			case float64:
				colTypes[i] = ValueType(int(n))
			case int:
				colTypes[i] = ValueType(n)
			case ValueType:
				colTypes[i] = n
			}
		}
	}
	return colTypes
}

// WriteDatabase writes a SQLVIBE v1.0.0 binary database file.
//
// File layout:
//   Header (256 bytes) | Schema JSON | Column data... | Footer (32 bytes)
//
// The schema parameter should include "column_names" ([]string) and
// "column_types" ([]int or []ValueType). Additional fields are preserved.
func WriteDatabase(path string, hs *HybridStore, schema map[string]interface{}) error {
	columns := hs.Columns()
	colTypes := extractColTypes(schema, columns)
	rows := hs.Scan()
	rowCount := len(rows)

	// Encode each column to its binary representation.
	colData := make([][]byte, len(columns))
	for ci := range columns {
		colData[ci] = encodeColumnData(rows, ci, colTypes[ci], rowCount)
	}

	// Build the schema JSON (always with canonical column metadata).
	writeSchema := make(map[string]interface{})
	for k, v := range schema {
		writeSchema[k] = v
	}
	typesAsInts := make([]int, len(colTypes))
	for i, t := range colTypes {
		typesAsInts[i] = int(t)
	}
	writeSchema["column_names"] = columns
	writeSchema["column_types"] = typesAsInts

	schemaJSON, err := json.Marshal(writeSchema)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}

	// Build header.
	now := uint32(time.Now().Unix())
	hdr := &FileHeader{
		VersionMajor: FormatVersionMajor,
		VersionMinor: FormatVersionMinor,
		VersionPatch: FormatVersionPatch,
		SchemaOffset: HeaderSize,
		SchemaLength: uint32(len(schemaJSON)),
		ColumnCount:  uint32(len(columns)),
		RowCount:     uint32(rowCount),
		CreatedAt:    now,
		ModifiedAt:   now,
	}
	copy(hdr.Magic[:], MagicBytes)

	// Compute header CRC over first 248 bytes.
	hdrBytes := marshalHeader(hdr)
	hdr.HeaderCRC64 = crc64.Checksum(hdrBytes[:248], crc64Table)
	hdrBytes = marshalHeader(hdr)

	// Assemble the file body (everything before the footer).
	var body bytes.Buffer
	body.Write(hdrBytes)
	body.Write(schemaJSON)
	for _, cd := range colData {
		body.Write(cd)
	}

	// Build footer with CRC of the body.
	fileCRC := crc64.Checksum(body.Bytes(), crc64Table)
	ft := &Footer{
		FileCRC:     fileCRC,
		RowCount:    uint32(rowCount),
		ColumnCount: uint32(len(columns)),
	}
	copy(ft.Magic[:], FooterMagic)

	// Write to file.
	var file bytes.Buffer
	file.Write(body.Bytes())
	file.Write(marshalFooter(ft))

	return os.WriteFile(path, file.Bytes(), 0644)
}

// ReadDatabase reads a SQLVIBE v1.0.0 database file, validates checksums and
// returns a populated HybridStore together with the original schema map.
func ReadDatabase(path string) (*HybridStore, map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read file: %w", err)
	}
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

	// Decode column data sequentially.
	pos := schemaEnd
	colVectors := make([]*ColumnVector, len(columns))
	for ci, col := range columns {
		cv, err := decodeColumnData(data[pos:], col, colTypes[ci], rowCount)
		if err != nil {
			return nil, nil, fmt.Errorf("decode column %d (%s): %w", ci, col, err)
		}
		colVectors[ci] = cv
		pos += columnDataSize(data[pos:], colTypes[ci], rowCount)
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

// columnDataSize returns the byte length of one column's encoded data section.
func columnDataSize(data []byte, typ ValueType, rowCount int) int {
	bitmapBytes := nullBitmapSize(rowCount)
	if len(data) < bitmapBytes {
		return len(data)
	}
	bitmap := data[:bitmapBytes]
	pos := bitmapBytes

	for i := 0; i < rowCount; i++ {
		isNull := bitmap[i/8]>>uint(i%8)&1 == 1
		n, err := valueSize(data[pos:], typ)
		if err != nil || isNull {
			// For fixed-size types we still know the size even for nulls.
			switch typ {
			case TypeInt, TypeFloat, TypeBool:
				pos += 8
			default:
				if err == nil {
					pos += n
				}
			}
			continue
		}
		pos += n
	}
	return pos
}
