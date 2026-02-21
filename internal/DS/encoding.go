package DS

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/sqlvibe/sqlvibe/internal/SF/util"
)

// Varint encoding/decoding following SQLite format
// SQLite varints are 1-9 bytes for 64-bit values
// First 7 bits of each byte are data, MSB indicates if more bytes follow

var (
	ErrVarintOverflow = errors.New("varint overflow")
	ErrVarintTrunc    = errors.New("varint truncated")
)

// PutVarint encodes an int64 as a varint and returns the number of bytes written
// Maximum 9 bytes (8 bytes with 7 bits + 1 byte with 8 bits)
func PutVarint(buf []byte, v int64) int {
	util.AssertNotNil(buf, "buf")
	requiredLen := VarintLen(v)
	util.Assert(len(buf) >= requiredLen, "buf too small for varint: %d bytes, need at least %d", len(buf), requiredLen)

	uv := uint64(v)

	// Handle single byte case (0-127)
	if uv < 0x80 {
		buf[0] = byte(uv)
		return 1
	}

	// Multi-byte encoding
	n := 0
	for n < 8 && uv >= 0x80 {
		buf[n] = byte(uv) | 0x80
		uv >>= 7
		n++
	}

	// Last byte doesn't have continuation bit
	buf[n] = byte(uv)
	return n + 1
}

// GetVarint decodes a varint from buf and returns (value, bytes_read)
func GetVarint(buf []byte) (int64, int) {
	util.AssertNotNil(buf, "buf")

	if len(buf) == 0 {
		return 0, 0
	}

	// Fast path for single byte
	if buf[0] < 0x80 {
		return int64(buf[0]), 1
	}

	// Multi-byte decode
	var v uint64
	var shift uint
	n := 0

	for n < 8 && n < len(buf) {
		b := buf[n]
		v |= uint64(b&0x7F) << shift
		n++

		if b < 0x80 {
			return int64(v), n
		}
		shift += 7
	}

	// 9th byte uses all 8 bits
	if n < len(buf) {
		v |= uint64(buf[n]) << shift
		n++
	}

	return int64(v), n
}

// VarintLen returns the length of the varint encoding of v
func VarintLen(v int64) int {
	uv := uint64(v)

	if uv < 0x80 {
		return 1
	}

	// Use bit manipulation for faster computation
	// Check how many bits are needed (each byte provides 7 bits)
	// 0x80 = 128 = 2^7, so values < 128 need 1 byte
	// 0x4000 = 16384 = 2^14, so values < 16384 need 2 bytes
	// etc.
	if uv < 0x4000 { // 14 bits (2 bytes max)
		return 2
	}
	if uv < 0x200000 { // 21 bits (3 bytes max)
		return 3
	}
	if uv < 0x10000000 { // 28 bits (4 bytes max)
		return 4
	}
	if uv < 0x800000000 { // 35 bits (5 bytes max)
		return 5
	}
	if uv < 0x40000000000 { // 42 bits (6 bytes max)
		return 6
	}
	if uv < 0x2000000000000 { // 49 bits (7 bytes max)
		return 7
	}
	if uv < 0x100000000000000 { // 56 bits (8 bytes max)
		return 8
	}
	return 9 // 63 bits (9 bytes max)
}

// Serial type codes for record format
// Used in record header to describe column types
const (
	SerialTypeNull       = 0
	SerialTypeInt8       = 1
	SerialTypeInt16      = 2
	SerialTypeInt24      = 3
	SerialTypeInt32      = 4
	SerialTypeInt48      = 5
	SerialTypeInt64      = 6
	SerialTypeFloat64    = 7
	SerialTypeZero       = 8  // Integer constant 0 (schema format 4+)
	SerialTypeOne        = 9  // Integer constant 1 (schema format 4+)
	SerialTypeReserved10 = 10 // Reserved
	SerialTypeReserved11 = 11 // Reserved
	// N >= 12 and even: BLOB with (N-12)/2 bytes
	// N >= 13 and odd: TEXT with (N-13)/2 bytes
)

// GetSerialType returns the serial type code for a value
func GetSerialType(v interface{}) int {
	if v == nil {
		return SerialTypeNull
	}

	switch val := v.(type) {
	case int:
		return getIntSerialType(int64(val))
	case int64:
		return getIntSerialType(val)
	case int32:
		return getIntSerialType(int64(val))
	case int16:
		return getIntSerialType(int64(val))
	case int8:
		return getIntSerialType(int64(val))
	case float64:
		return SerialTypeFloat64
	case float32:
		return SerialTypeFloat64
	case string:
		n := len(val)
		return 13 + 2*n
	case []byte:
		n := len(val)
		return 12 + 2*n
	default:
		// Default to NULL for unknown types
		return SerialTypeNull
	}
}

func getIntSerialType(v int64) int {
	if v == 0 {
		return SerialTypeZero
	}
	if v == 1 {
		return SerialTypeOne
	}

	// Determine minimum bytes needed (SQLite uses Int8, Int16, Int32, Int48, Int64)
	if v >= -128 && v <= 127 {
		return SerialTypeInt8
	}
	if v >= -32768 && v <= 32767 {
		return SerialTypeInt16
	}
	// SQLite uses Int32 (not Int24) for 4-byte integers
	if v >= -2147483648 && v <= 2147483647 {
		return SerialTypeInt32
	}
	if v >= -140737488355328 && v <= 140737488355327 {
		return SerialTypeInt48
	}
	return SerialTypeInt64
}

// SerialTypeLen returns the payload length for a serial type
func SerialTypeLen(serialType int) int {
	switch serialType {
	case SerialTypeNull, SerialTypeZero, SerialTypeOne:
		return 0
	case SerialTypeInt8:
		return 1
	case SerialTypeInt16:
		return 2
	case SerialTypeInt24:
		return 3
	case SerialTypeInt32:
		return 4
	case SerialTypeInt48:
		return 6
	case SerialTypeInt64:
		return 8
	case SerialTypeFloat64:
		return 8
	default:
		if serialType >= 12 {
			if serialType%2 == 0 {
				// BLOB
				return (serialType - 12) / 2
			}
			// TEXT
			return (serialType - 13) / 2
		}
		return 0
	}
}

// EncodeRecord encodes a record (row) in SQLite format
// Returns: header + data bytes
func EncodeRecord(values []interface{}) []byte {
	// Calculate header size
	serialTypes := make([]int, len(values))
	headerSize := 1 // At least 1 byte for header size varint

	for i, v := range values {
		st := GetSerialType(v)
		serialTypes[i] = st
		headerSize += VarintLen(int64(st))
	}

	// Calculate total data size
	dataSize := 0
	for _, st := range serialTypes {
		dataSize += SerialTypeLen(st)
	}

	// Allocate buffer
	totalSize := headerSize + dataSize
	buf := make([]byte, totalSize)

	// Write header size
	pos := PutVarint(buf, int64(headerSize))

	// Write serial types
	for _, st := range serialTypes {
		pos += PutVarint(buf[pos:], int64(st))
	}

	// Write data
	for i, v := range values {
		st := serialTypes[i]
		pos += encodeValue(buf[pos:], v, st)
	}

	return buf
}

func encodeValue(buf []byte, v interface{}, serialType int) int {
	switch serialType {
	case SerialTypeNull, SerialTypeZero, SerialTypeOne:
		return 0

	case SerialTypeInt8:
		buf[0] = byte(v.(int64))
		return 1

	case SerialTypeInt16:
		binary.BigEndian.PutUint16(buf, uint16(v.(int64)))
		return 2

	case SerialTypeInt24:
		val := uint32(v.(int64))
		buf[0] = byte(val >> 16)
		buf[1] = byte(val >> 8)
		buf[2] = byte(val)
		return 3

	case SerialTypeInt32:
		binary.BigEndian.PutUint32(buf, uint32(v.(int64)))
		return 4

	case SerialTypeInt48:
		val := uint64(v.(int64))
		buf[0] = byte(val >> 40)
		buf[1] = byte(val >> 32)
		buf[2] = byte(val >> 24)
		buf[3] = byte(val >> 16)
		buf[4] = byte(val >> 8)
		buf[5] = byte(val)
		return 6

	case SerialTypeInt64:
		binary.BigEndian.PutUint64(buf, uint64(v.(int64)))
		return 8

	case SerialTypeFloat64:
		binary.BigEndian.PutUint64(buf, math.Float64bits(v.(float64)))
		return 8

	default:
		if serialType >= 12 {
			length := SerialTypeLen(serialType)
			if serialType%2 == 0 {
				// BLOB
				copy(buf, v.([]byte))
			} else {
				// TEXT
				copy(buf, []byte(v.(string)))
			}
			return length
		}
		return 0
	}
}

// DecodeRecord decodes a record from bytes
// Returns values and bytes consumed
func DecodeRecord(buf []byte) ([]interface{}, int, error) {
	if len(buf) == 0 {
		return nil, 0, errors.New("empty buffer")
	}

	// Read header size
	headerSize, n := GetVarint(buf)
	if n == 0 {
		return nil, 0, ErrVarintTrunc
	}

	// Read serial types from header
	var serialTypes []int
	pos := n
	for pos < int(headerSize) {
		st, bytesRead := GetVarint(buf[pos:])
		if bytesRead == 0 {
			return nil, 0, ErrVarintTrunc
		}
		serialTypes = append(serialTypes, int(st))
		pos += bytesRead
	}

	// Decode values
	values := make([]interface{}, len(serialTypes))
	for i, st := range serialTypes {
		val, bytesRead := decodeValue(buf[pos:], st)
		values[i] = val
		pos += bytesRead
	}

	return values, pos, nil
}

func decodeValue(buf []byte, serialType int) (interface{}, int) {
	switch serialType {
	case SerialTypeNull:
		return nil, 0

	case SerialTypeZero:
		return int64(0), 0

	case SerialTypeOne:
		return int64(1), 0

	case SerialTypeInt8:
		return int64(int8(buf[0])), 1

	case SerialTypeInt16:
		return int64(int16(binary.BigEndian.Uint16(buf))), 2

	case SerialTypeInt24:
		val := uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])
		// Sign extend
		if val&0x800000 != 0 {
			val |= 0xFF000000
		}
		return int64(int32(val)), 3

	case SerialTypeInt32:
		return int64(int32(binary.BigEndian.Uint32(buf))), 4

	case SerialTypeInt48:
		val := uint64(buf[0])<<40 | uint64(buf[1])<<32 | uint64(buf[2])<<24 |
			uint64(buf[3])<<16 | uint64(buf[4])<<8 | uint64(buf[5])
		// Sign extend
		if val&0x800000000000 != 0 {
			val |= 0xFFFF000000000000
		}
		return int64(val), 6

	case SerialTypeInt64:
		return int64(binary.BigEndian.Uint64(buf)), 8

	case SerialTypeFloat64:
		bits := binary.BigEndian.Uint64(buf)
		return math.Float64frombits(bits), 8

	default:
		if serialType >= 12 {
			length := SerialTypeLen(serialType)
			if serialType%2 == 0 {
				// BLOB
				data := make([]byte, length)
				copy(data, buf[:length])
				return data, length
			}
			// TEXT
			return string(buf[:length]), length
		}
		return nil, 0
	}
}
