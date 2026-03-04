// Package DS - minimal Page type for C++ wrapper compatibility
package DS

// PageType represents the type of a database page.
type PageType int

const (
	PageTypeInteriorIndex PageType = 0x02
	PageTypeInteriorTable PageType = 0x05
	PageTypeLeafIndex     PageType = 0x0a
	PageTypeLeafTable     PageType = 0x0d
)

// Page represents a database page.
// This is a minimal type definition for C++ wrapper compatibility.
// The actual page handling is done in C++.
type Page struct {
	Data    []byte
	Num     uint32
	Type    PageType
	IsDirty bool
}

// NewPage creates a new page with the given number and size.
func NewPage(pageNum uint32, pageSize int) *Page {
	return &Page{
		Num:  pageNum,
		Data: make([]byte, pageSize),
	}
}

// ParseHeader parses a database header from data.
func ParseHeader(data []byte) (*DatabaseHeader, error) {
	h := &DatabaseHeader{}
	if len(data) >= len(h.Data) {
		copy(h.Data[:], data[:len(h.Data)])
	}
	return h, nil
}
