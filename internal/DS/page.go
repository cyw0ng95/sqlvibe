package DS

import (
	"encoding/binary"
	"errors"

	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

const (
	DefaultPageSize = 4096
	MaxPageSize     = 65536
	MinPageSize     = 512
)

const (
	PageTypeLockByte    = 0xff
	PageTypeFreelist    = 0xfe
	PageTypePointerMap  = 0xfd
	PageTypeInteriorIdx = 0x02
	PageTypeInteriorTbl = 0x05
	PageTypeLeafIdx     = 0x0a
	PageTypeLeafTbl     = 0x0d
)

const (
	SQLiteMagic = "SQLite format 3\x00"
)

var (
	ErrInvalidPageSize = errors.New("invalid page size")
	ErrInvalidHeader   = errors.New("invalid database header")
	ErrInvalidPage     = errors.New("invalid page")
)

type PageType uint8

func (pt PageType) String() string {
	switch pt {
	case PageTypeLockByte:
		return "lock-byte"
	case PageTypeFreelist:
		return "freelist"
	case PageTypePointerMap:
		return "pointer-map"
	case PageTypeInteriorIdx:
		return "interior-index"
	case PageTypeInteriorTbl:
		return "interior-table"
	case PageTypeLeafIdx:
		return "leaf-index"
	case PageTypeLeafTbl:
		return "leaf-table"
	default:
		return "unknown"
	}
}

type Page struct {
	Num      uint32
	Type     PageType
	Data     []byte
	IsDirty  bool
	RefCount int
}

func NewPage(num uint32, size int) *Page {
	// Note: Allow sizes smaller than MinPageSize for unit testing purposes
	// but validate that power-of-2 sizes are used for production databases
	util.Assert(size > 0, "page size %d must be positive", size)
	if size >= MinPageSize {
		util.Assert(IsValidPageSize(size), "page size %d must be power of 2 for sizes >= %d", size, MinPageSize)
	}

	return &Page{
		Num:      num,
		Type:     0,
		Data:     make([]byte, size),
		IsDirty:  false,
		RefCount: 0,
	}
}

func (p *Page) SetType(t PageType) {
	p.Type = t
	p.IsDirty = true
}

func (p *Page) SetData(data []byte) {
	util.AssertNotNil(data, "data")
	util.Assert(len(data) <= len(p.Data), "data size %d exceeds page capacity %d", len(data), len(p.Data))

	copy(p.Data, data)
	p.IsDirty = true
}

type DatabaseHeader struct {
	Magic               [16]byte
	PageSize            uint16
	WriteVersion        uint8
	ReadVersion         uint8
	ReservedSpace       uint8
	MaxEmbeddedPayload  uint8
	MinEmbeddedPayload  uint8
	LeafPayloadFraction uint8
	FileChangeCounter   uint32
	DatabaseSize        uint32
	FirstFreelistTrunk  uint32
	TotalFreelistPages  uint32
	SchemaCookie        uint32
	SchemaFormat        uint32
	DefaultCacheSize    uint32
	LargestRootBTree    uint32
	TextEncoding        uint32
	UserVersion         uint32
	IncrementalVacuum   uint32
	ApplicationID       uint32
	VersionValidFor     uint32
	SQLiteVersion       uint32
}

func (h *DatabaseHeader) GetTextEncoding() string {
	switch h.TextEncoding {
	case 1:
		return "UTF-8"
	case 2:
		return "UTF-16le"
	case 3:
		return "UTF-16be"
	default:
		return "unknown"
	}
}

func ParseHeader(data []byte) (*DatabaseHeader, error) {
	util.AssertNotNil(data, "data")

	if len(data) < 100 {
		return nil, ErrInvalidHeader
	}

	h := &DatabaseHeader{}
	copy(h.Magic[:], data[0:16])
	h.PageSize = binary.BigEndian.Uint16(data[16:18])
	h.WriteVersion = data[18]
	h.ReadVersion = data[19]
	h.ReservedSpace = data[20]
	h.MaxEmbeddedPayload = data[21]
	h.MinEmbeddedPayload = data[22]
	h.LeafPayloadFraction = data[23]
	h.FileChangeCounter = binary.BigEndian.Uint32(data[24:28])
	h.DatabaseSize = binary.BigEndian.Uint32(data[28:32])
	h.FirstFreelistTrunk = binary.BigEndian.Uint32(data[32:36])
	h.TotalFreelistPages = binary.BigEndian.Uint32(data[36:40])
	h.SchemaCookie = binary.BigEndian.Uint32(data[40:44])
	h.SchemaFormat = binary.BigEndian.Uint32(data[44:48])
	h.DefaultCacheSize = binary.BigEndian.Uint32(data[48:52])
	h.LargestRootBTree = binary.BigEndian.Uint32(data[52:56])
	h.TextEncoding = binary.BigEndian.Uint32(data[56:60])
	h.UserVersion = binary.BigEndian.Uint32(data[60:64])
	h.IncrementalVacuum = binary.BigEndian.Uint32(data[64:68])
	h.ApplicationID = binary.BigEndian.Uint32(data[68:72])
	h.VersionValidFor = binary.BigEndian.Uint32(data[92:96])
	h.SQLiteVersion = binary.BigEndian.Uint32(data[96:100])

	if string(h.Magic[:]) != SQLiteMagic {
		return nil, ErrInvalidHeader
	}

	return h, nil
}

func (h *DatabaseHeader) WriteTo(data []byte) error {
	util.AssertNotNil(data, "data")

	if len(data) < 100 {
		return ErrInvalidHeader
	}

	copy(data[0:16], h.Magic[:])
	binary.BigEndian.PutUint16(data[16:18], h.PageSize)
	data[18] = h.WriteVersion
	data[19] = h.ReadVersion
	data[20] = h.ReservedSpace
	data[21] = h.MaxEmbeddedPayload
	data[22] = h.MinEmbeddedPayload
	data[23] = h.LeafPayloadFraction
	binary.BigEndian.PutUint32(data[24:28], h.FileChangeCounter)
	binary.BigEndian.PutUint32(data[28:32], h.DatabaseSize)
	binary.BigEndian.PutUint32(data[32:36], h.FirstFreelistTrunk)
	binary.BigEndian.PutUint32(data[36:40], h.TotalFreelistPages)
	binary.BigEndian.PutUint32(data[40:44], h.SchemaCookie)
	binary.BigEndian.PutUint32(data[44:48], h.SchemaFormat)
	binary.BigEndian.PutUint32(data[48:52], h.DefaultCacheSize)
	binary.BigEndian.PutUint32(data[52:56], h.LargestRootBTree)
	binary.BigEndian.PutUint32(data[56:60], h.TextEncoding)
	binary.BigEndian.PutUint32(data[60:64], h.UserVersion)
	binary.BigEndian.PutUint32(data[64:68], h.IncrementalVacuum)
	binary.BigEndian.PutUint32(data[68:72], h.ApplicationID)
	binary.BigEndian.PutUint32(data[92:96], h.VersionValidFor)
	binary.BigEndian.PutUint32(data[96:100], h.SQLiteVersion)

	return nil
}

func NewDatabaseHeader(pageSize uint16) *DatabaseHeader {
	util.Assert(IsValidPageSize(int(pageSize)), "invalid page size: %d", pageSize)

	h := &DatabaseHeader{
		PageSize:            pageSize,
		WriteVersion:        1,
		ReadVersion:         1,
		ReservedSpace:       0,
		MaxEmbeddedPayload:  64,
		MinEmbeddedPayload:  32,
		LeafPayloadFraction: 32,
		FileChangeCounter:   1,
		DatabaseSize:        0,
		FirstFreelistTrunk:  0,
		TotalFreelistPages:  0,
		SchemaCookie:        1,
		SchemaFormat:        4,
		DefaultCacheSize:    ^uint32(1999),
		LargestRootBTree:    0,
		TextEncoding:        1,
		UserVersion:         0,
		IncrementalVacuum:   0,
		ApplicationID:       0,
		VersionValidFor:     1,
		SQLiteVersion:       3024000,
	}
	copy(h.Magic[:], SQLiteMagic)
	return h
}

func IsValidPageSize(size int) bool {
	if size < MinPageSize || size > MaxPageSize {
		return false
	}
	if size&(size-1) != 0 {
		return false
	}
	return true
}

func GetDefaultPageSize() int {
	return DefaultPageSize
}
