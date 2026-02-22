package DS

import (
	"encoding/binary"
	"errors"

	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// Cell format encoding/decoding for all 4 BTree page types
// Following SQLite BTree cell format specifications

var (
	ErrInvalidCellFormat = errors.New("invalid cell format")
	ErrCellTooLarge      = errors.New("cell too large")
)

// CellType represents the type of BTree cell
type CellType int

const (
	CellTypeTableLeaf CellType = iota
	CellTypeTableInterior
	CellTypeIndexLeaf
	CellTypeIndexInterior
)

// CellData represents a decoded BTree cell
type CellData struct {
	Type         CellType
	LeftChild    uint32 // For interior cells only
	Rowid        int64  // For table cells only
	Key          []byte // For index cells only
	Payload      []byte // Cell payload
	OverflowPage uint32 // Overflow page number (0 if no overflow)
	LocalSize    int    // Size of local payload
}

// EncodeTableLeafCell encodes a table leaf cell
// Format: payload_size (varint) + rowid (varint) + payload + [overflow_page (4 bytes)]
func EncodeTableLeafCell(rowid int64, payload []byte, overflowPage uint32) []byte {
	util.AssertNotNil(payload, "payload")
	util.Assert(rowid > 0, "rowid must be positive: %d", rowid)

	payloadSize := len(payload)

	// Calculate size
	size := VarintLen(int64(payloadSize)) + VarintLen(rowid) + payloadSize
	if overflowPage > 0 {
		size += 4
	}

	buf := make([]byte, size)
	pos := 0

	// Write payload size
	pos += PutVarint(buf[pos:], int64(payloadSize))

	// Write rowid
	pos += PutVarint(buf[pos:], rowid)

	// Write payload
	copy(buf[pos:], payload)
	pos += payloadSize

	// Write overflow page if needed
	if overflowPage > 0 {
		binary.BigEndian.PutUint32(buf[pos:], overflowPage)
	}

	return buf
}

// DecodeTableLeafCell decodes a table leaf cell
func DecodeTableLeafCell(buf []byte) (*CellData, error) {
	util.AssertNotNil(buf, "buf")

	if len(buf) < 2 {
		return nil, ErrInvalidCellFormat
	}

	cell := &CellData{Type: CellTypeTableLeaf}
	pos := 0

	// Read payload size
	payloadSize, n := GetVarint(buf[pos:])
	if n == 0 {
		return nil, ErrInvalidCellFormat
	}
	pos += n

	// Read rowid
	rowid, n := GetVarint(buf[pos:])
	if n == 0 {
		return nil, ErrInvalidCellFormat
	}
	cell.Rowid = rowid
	pos += n

	// Determine if there's an overflow page
	// If buffer has exactly 4 more bytes after payload, it's overflow page
	remainingBytes := len(buf) - pos
	hasOverflow := false
	localSize := int(payloadSize)

	// Check if we have overflow page indicator (4 bytes after payload)
	if remainingBytes >= localSize+4 {
		// Could have overflow page - check if we have exactly payload + 4 bytes
		// or if there's more data, only read what the payload size indicates
		hasOverflow = true
	} else if remainingBytes < localSize {
		// Buffer too small - invalid
		return nil, ErrInvalidCellFormat
	}

	// Read only the payload bytes indicated by payloadSize
	cell.Payload = make([]byte, localSize)
	copy(cell.Payload, buf[pos:pos+localSize])
	cell.LocalSize = localSize
	pos += localSize

	// Read overflow page if present
	if hasOverflow && pos+4 <= len(buf) {
		cell.OverflowPage = binary.BigEndian.Uint32(buf[pos:])
	}

	return cell, nil
}

// EncodeTableInteriorCell encodes a table interior cell
// Format: left_child (4 bytes) + rowid (varint)
func EncodeTableInteriorCell(leftChild uint32, rowid int64) []byte {
	size := 4 + VarintLen(rowid)
	buf := make([]byte, size)

	// Write left child page number
	binary.BigEndian.PutUint32(buf[0:4], leftChild)

	// Write rowid
	PutVarint(buf[4:], rowid)

	return buf
}

// DecodeTableInteriorCell decodes a table interior cell
func DecodeTableInteriorCell(buf []byte) (*CellData, error) {
	if len(buf) < 5 {
		return nil, ErrInvalidCellFormat
	}

	cell := &CellData{Type: CellTypeTableInterior}

	// Read left child page number
	cell.LeftChild = binary.BigEndian.Uint32(buf[0:4])

	// Read rowid
	rowid, n := GetVarint(buf[4:])
	if n == 0 {
		return nil, ErrInvalidCellFormat
	}
	cell.Rowid = rowid

	return cell, nil
}

// EncodeIndexLeafCell encodes an index leaf cell
// Format: payload_size (varint) + payload + [overflow_page (4 bytes)]
func EncodeIndexLeafCell(key []byte, overflowPage uint32) []byte {
	payloadSize := len(key)

	size := VarintLen(int64(payloadSize)) + payloadSize
	if overflowPage > 0 {
		size += 4
	}

	buf := make([]byte, size)
	pos := 0

	// Write payload size
	pos += PutVarint(buf[pos:], int64(payloadSize))

	// Write payload (key)
	copy(buf[pos:], key)
	pos += payloadSize

	// Write overflow page if needed
	if overflowPage > 0 {
		binary.BigEndian.PutUint32(buf[pos:], overflowPage)
	}

	return buf
}

// DecodeIndexLeafCell decodes an index leaf cell
func DecodeIndexLeafCell(buf []byte) (*CellData, error) {
	if len(buf) < 1 {
		return nil, ErrInvalidCellFormat
	}

	cell := &CellData{Type: CellTypeIndexLeaf}
	pos := 0

	// Read payload size
	payloadSize, n := GetVarint(buf[pos:])
	if n == 0 {
		return nil, ErrInvalidCellFormat
	}
	pos += n

	// Determine if there's an overflow page
	remainingBytes := len(buf) - pos
	hasOverflow := false
	localSize := int(payloadSize)

	if remainingBytes == localSize+4 {
		hasOverflow = true
	} else if remainingBytes < localSize {
		return nil, ErrInvalidCellFormat
	} else {
		// localSize already set to payloadSize
	}

	cell.Key = make([]byte, localSize)
	copy(cell.Key, buf[pos:pos+localSize])
	cell.LocalSize = localSize
	pos += localSize

	// Read overflow page if present
	if hasOverflow && pos+4 <= len(buf) {
		cell.OverflowPage = binary.BigEndian.Uint32(buf[pos:])
	}

	return cell, nil
}

// EncodeIndexInteriorCell encodes an index interior cell
// Format: left_child (4 bytes) + payload_size (varint) + payload + [overflow_page (4 bytes)]
func EncodeIndexInteriorCell(leftChild uint32, key []byte, overflowPage uint32) []byte {
	payloadSize := len(key)

	size := 4 + VarintLen(int64(payloadSize)) + payloadSize
	if overflowPage > 0 {
		size += 4
	}

	buf := make([]byte, size)
	pos := 0

	// Write left child page number
	binary.BigEndian.PutUint32(buf[pos:pos+4], leftChild)
	pos += 4

	// Write payload size
	pos += PutVarint(buf[pos:], int64(payloadSize))

	// Write payload (key)
	copy(buf[pos:], key)
	pos += payloadSize

	// Write overflow page if needed
	if overflowPage > 0 {
		binary.BigEndian.PutUint32(buf[pos:], overflowPage)
	}

	return buf
}

// DecodeIndexInteriorCell decodes an index interior cell
func DecodeIndexInteriorCell(buf []byte) (*CellData, error) {
	if len(buf) < 5 {
		return nil, ErrInvalidCellFormat
	}

	cell := &CellData{Type: CellTypeIndexInterior}
	pos := 0

	// Read left child page number
	cell.LeftChild = binary.BigEndian.Uint32(buf[pos : pos+4])
	pos += 4

	// Read payload size
	payloadSize, n := GetVarint(buf[pos:])
	if n == 0 {
		return nil, ErrInvalidCellFormat
	}
	pos += n

	// Determine if there's an overflow page
	remainingBytes := len(buf) - pos
	hasOverflow := false
	localSize := int(payloadSize)

	if remainingBytes == localSize+4 {
		hasOverflow = true
	} else if remainingBytes < localSize {
		return nil, ErrInvalidCellFormat
	} else {
		// localSize already set to payloadSize
	}

	cell.Key = make([]byte, localSize)
	copy(cell.Key, buf[pos:pos+localSize])
	cell.LocalSize = localSize
	pos += localSize

	// Read overflow page if present
	if hasOverflow && pos+4 <= len(buf) {
		cell.OverflowPage = binary.BigEndian.Uint32(buf[pos:])
	}

	return cell, nil
}

// CalculateLocalPayloadSize calculates how much payload fits on the page
// Following SQLite's local payload calculation
func CalculateLocalPayloadSize(usableSize int, payloadSize int, isLeaf bool) int {
	U := usableSize
	P := payloadSize

	// M = ((U-12)*32/255)-23 (min local)
	M := ((U - 12) * 32 / 255) - 23

	if isLeaf {
		// X = U-35 (max local for leaf)
		X := U - 35

		if P <= X {
			return P
		}

		// K = M+((P-M)%(U-4))
		K := M + ((P - M) % (U - 4))
		return K
	}

	// Interior page
	// X = ((U-12)*64/255)-23 (max local for interior)
	X := ((U - 12) * 64 / 255) - 23

	if P <= X {
		return P
	}

	K := M + ((P - M) % (U - 4))
	return K
}

// CellSize returns the size of an encoded cell
func CellSize(cell *CellData) int {
	switch cell.Type {
	case CellTypeTableLeaf:
		size := VarintLen(int64(len(cell.Payload))) + VarintLen(cell.Rowid) + len(cell.Payload)
		if cell.OverflowPage > 0 {
			size += 4
		}
		return size

	case CellTypeTableInterior:
		return 4 + VarintLen(cell.Rowid)

	case CellTypeIndexLeaf:
		size := VarintLen(int64(len(cell.Key))) + len(cell.Key)
		if cell.OverflowPage > 0 {
			size += 4
		}
		return size

	case CellTypeIndexInterior:
		size := 4 + VarintLen(int64(len(cell.Key))) + len(cell.Key)
		if cell.OverflowPage > 0 {
			size += 4
		}
		return size
	}

	return 0
}
