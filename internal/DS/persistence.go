package DS

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"time"
)

// Compression type constants stored in the file header CompressionType field.
const (
	CompressionNone  = uint32(0) // no compression
	CompressionRLE   = uint32(1) // run-length encoding
	CompressionGzip  = uint32(2) // gzip (deflate)
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
		buf.Write(encodeStorageValue(v, typ))
	}
	return buf.Bytes()
}

// encodeValue encodes a single value according to its storage type.
func encodeStorageValue(v Value, typ ValueType) []byte {
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
		v, n, err := decodeStorageValue(data[pos:], typ)
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
func decodeStorageValue(data []byte, typ ValueType) (Value, int, error) {
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

// encodeRLE applies byte-level run-length encoding.
// Each run is encoded as: [value byte] [count-1 byte] where count is in [1, 256].
// Input bytes are encoded one run at a time; the output is always an even number of bytes.
func encodeRLE(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, 0, len(data))
	i := 0
	for i < len(data) {
		b := data[i]
		count := 1
		for i+count < len(data) && data[i+count] == b && count < 256 {
			count++
		}
		out = append(out, b, byte(count-1))
		i += count
	}
	return out
}

// decodeRLE reverses byte-level run-length encoding produced by encodeRLE.
func decodeRLE(data []byte) ([]byte, error) {
	if len(data)%2 != 0 {
		return nil, fmt.Errorf("RLE data has odd length %d", len(data))
	}
	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); i += 2 {
		b := data[i]
		count := int(data[i+1]) + 1
		for j := 0; j < count; j++ {
			out = append(out, b)
		}
	}
	return out, nil
}

// compressGzip compresses data using gzip (default compression level).
func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompressGzip decompresses gzip-compressed data.
func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// compressColumnData encodes and optionally compresses column data.
// compressionType: CompressionNone, CompressionRLE, or CompressionGzip.
// Returns (payload, actualCompressionType, error).
func compressColumnData(raw []byte, compressionType uint32) ([]byte, uint32, error) {
	switch compressionType {
	case CompressionRLE:
		return encodeRLE(raw), CompressionRLE, nil
	case CompressionGzip:
		compressed, err := compressGzip(raw)
		if err != nil {
			return raw, CompressionNone, err
		}
		return compressed, CompressionGzip, nil
	default:
		return raw, CompressionNone, nil
	}
}

// decompressColumnData reverses compressColumnData.
func decompressColumnData(data []byte, compressionType uint32) ([]byte, error) {
	switch compressionType {
	case CompressionRLE:
		return decodeRLE(data)
	case CompressionGzip:
		return decompressGzip(data)
	default:
		return data, nil
	}
}

// ─── Index serialization ─────────────────────────────────────────────────────
//
// BitmapIndexes are serialized column-by-column.  For each indexed column the
// layout is:
//
//   [colNameLen uint16] [colName bytes]
//   [valueCount uint32]
//   For each unique value:
//     [keyLen uint16] [key bytes]
//     [bitCount uint32]  -- number of set bits in the bitmap
//     [bit0 uint32] [bit1 uint32] ...
//
// SkipList indexes are serialized as sorted key→rowIdx pairs:
//
//   [colNameLen uint16] [colName bytes]
//   [pairCount uint32]
//   For each (key, rowIdx) pair:
//     [valueType uint8] [encoded value] [rowIdx uint32]
//
// Both sections are prefixed with a count:
//   [bitmapColCount uint32] ... [skipListColCount uint32] ...

// SerializeIndexes serializes all bitmap and skip-list indexes from ie into a
// compact binary representation.
func SerializeIndexes(ie *IndexEngine) []byte {
	var buf bytes.Buffer

	// --- Bitmap indexes ---
	bitmapCols := ie.BitmapColumns()
	binary.Write(&buf, binary.LittleEndian, uint32(len(bitmapCols)))
	for _, col := range bitmapCols {
		bm := ie.BitmapMap(col) // map[string]*RoaringBitmap
		writeString16(&buf, col)
		binary.Write(&buf, binary.LittleEndian, uint32(len(bm)))
		for key, rb := range bm {
			writeString16(&buf, key)
			idxs := rb.ToSlice()
			binary.Write(&buf, binary.LittleEndian, uint32(len(idxs)))
			for _, idx := range idxs {
				binary.Write(&buf, binary.LittleEndian, idx)
			}
		}
	}

	// --- SkipList indexes ---
	skipCols := ie.SkipListColumns()
	binary.Write(&buf, binary.LittleEndian, uint32(len(skipCols)))
	for _, col := range skipCols {
		sl := ie.SkipList(col)
		writeString16(&buf, col)
		pairs := sl.Pairs() // []SkipPair
		binary.Write(&buf, binary.LittleEndian, uint32(len(pairs)))
		for _, p := range pairs {
			binary.Write(&buf, binary.LittleEndian, uint8(p.Key.Type))
			buf.Write(encodeStorageValue(p.Key, p.Key.Type))
			binary.Write(&buf, binary.LittleEndian, p.RowIdx)
		}
	}

	return buf.Bytes()
}

// DeserializeIndexes restores bitmap and skip-list indexes into ie from data
// produced by SerializeIndexes.
func DeserializeIndexes(data []byte, ie *IndexEngine) error {
	if len(data) == 0 {
		return nil
	}
	r := bytes.NewReader(data)

	// --- Bitmap indexes ---
	var bitmapColCount uint32
	if err := binary.Read(r, binary.LittleEndian, &bitmapColCount); err != nil {
		return fmt.Errorf("read bitmap col count: %w", err)
	}
	for ci := uint32(0); ci < bitmapColCount; ci++ {
		col, err := readString16(r)
		if err != nil {
			return fmt.Errorf("read bitmap col name: %w", err)
		}
		ie.AddBitmapIndex(col)
		var valueCount uint32
		if err := binary.Read(r, binary.LittleEndian, &valueCount); err != nil {
			return fmt.Errorf("read bitmap value count: %w", err)
		}
		for vi := uint32(0); vi < valueCount; vi++ {
			key, err := readString16(r)
			if err != nil {
				return fmt.Errorf("read bitmap key: %w", err)
			}
			var bitCount uint32
			if err := binary.Read(r, binary.LittleEndian, &bitCount); err != nil {
				return fmt.Errorf("read bit count: %w", err)
			}
			rb := NewRoaringBitmap()
			for bi := uint32(0); bi < bitCount; bi++ {
				var idx uint32
				if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
					return fmt.Errorf("read bitmap idx: %w", err)
				}
				rb.Add(idx)
			}
			ie.SetBitmap(col, key, rb)
		}
	}

	// --- SkipList indexes ---
	var skipColCount uint32
	if err := binary.Read(r, binary.LittleEndian, &skipColCount); err != nil {
		return fmt.Errorf("read skiplist col count: %w", err)
	}
	for ci := uint32(0); ci < skipColCount; ci++ {
		col, err := readString16(r)
		if err != nil {
			return fmt.Errorf("read skiplist col name: %w", err)
		}
		ie.AddSkipListIndex(col)
		var pairCount uint32
		if err := binary.Read(r, binary.LittleEndian, &pairCount); err != nil {
			return fmt.Errorf("read pair count: %w", err)
		}
		for pi := uint32(0); pi < pairCount; pi++ {
			var vt uint8
			if err := binary.Read(r, binary.LittleEndian, &vt); err != nil {
				return fmt.Errorf("read value type: %w", err)
			}
			v, err := readValueFromReader(r, ValueType(vt))
			if err != nil {
				return fmt.Errorf("decode skip value: %w", err)
			}
			var rowIdx uint32
			if err := binary.Read(r, binary.LittleEndian, &rowIdx); err != nil {
				return fmt.Errorf("read skip rowIdx: %w", err)
			}
			ie.SkipList(col).Insert(v, rowIdx)
		}
	}
	return nil
}

func writeString16(buf *bytes.Buffer, s string) {
	b := []byte(s)
	binary.Write(buf, binary.LittleEndian, uint16(len(b)))
	buf.Write(b)
}

func readString16(r *bytes.Reader) (string, error) {
	var l uint16
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return "", err
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// readValueFromReader reads one encoded value of type vt from r.
func readValueFromReader(r *bytes.Reader, vt ValueType) (Value, error) {
	switch vt {
	case TypeInt, TypeFloat, TypeBool:
		b := make([]byte, 8)
		if _, err := io.ReadFull(r, b); err != nil {
			return NullValue(), err
		}
		v, _, err := decodeStorageValue(b, vt)
		return v, err
	case TypeString, TypeBytes:
		var l uint32
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return NullValue(), err
		}
		body := make([]byte, l)
		if _, err := io.ReadFull(r, body); err != nil {
			return NullValue(), err
		}
		if vt == TypeString {
			return StringValue(string(body)), nil
		}
		return BytesValue(body), nil
	default:
		return NullValue(), fmt.Errorf("unknown value type %d", vt)
	}
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

// WriteDatabase writes a SQLVIBE v1.0.0 binary database file with no compression.
//
// File layout:
//   Header (256 bytes) | Schema JSON | Column data... | Footer (32 bytes)
//
// The schema parameter should include "column_names" ([]string) and
// "column_types" ([]int or []ValueType). Additional fields are preserved.
func WriteDatabase(path string, hs *HybridStore, schema map[string]interface{}) error {
	return WriteDatabaseOpts(path, hs, schema, CompressionNone)
}

// WriteDatabaseOpts writes a SQLVIBE database file with the specified column-level
// compression.  compressionType must be one of CompressionNone, CompressionRLE,
// or CompressionGzip.
func WriteDatabaseOpts(path string, hs *HybridStore, schema map[string]interface{}, compressionType uint32) error {
	columns := hs.Columns()
	colTypes := extractColTypes(schema, columns)
	rows := hs.Scan()
	rowCount := len(rows)

	// Encode each column to its binary representation, optionally compressed.
	colData := make([][]byte, len(columns))
	for ci := range columns {
		raw := encodeColumnData(rows, ci, colTypes[ci], rowCount)
		if compressionType != CompressionNone {
			compressed, _, err := compressColumnData(raw, compressionType)
			if err != nil {
				compressed = raw
			}
			// Prefix: [rawSize uint32][compressedSize uint32][compressed data]
			hdrBuf := make([]byte, 8)
			binary.LittleEndian.PutUint32(hdrBuf[0:4], uint32(len(raw)))
			binary.LittleEndian.PutUint32(hdrBuf[4:8], uint32(len(compressed)))
			colData[ci] = append(hdrBuf, compressed...)
		} else {
			colData[ci] = raw
		}
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
		VersionMajor:    FormatVersionMajor,
		VersionMinor:    FormatVersionMinor,
		VersionPatch:    FormatVersionPatch,
		SchemaOffset:    HeaderSize,
		SchemaLength:    uint32(len(schemaJSON)),
		ColumnCount:     uint32(len(columns)),
		RowCount:        uint32(rowCount),
		CreatedAt:       now,
		ModifiedAt:      now,
		CompressionType: compressionType,
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
	compressionType := hdr.CompressionType
	for ci, col := range columns {
		var rawData []byte
		if compressionType != CompressionNone {
			// Compressed columns are prefixed with [rawSize uint32][compressedSize uint32].
			// rawSize holds the expected uncompressed byte count and is used to pre-allocate
			// the decompression buffer; decompressColumnData validates the actual length.
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
			// Uncompressed: consume exactly columnDataSize bytes.
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

// BenchEncodeRLE is a public wrapper around encodeRLE for benchmark access.
func BenchEncodeRLE(data []byte) []byte { return encodeRLE(data) }
