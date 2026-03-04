// Package DS - minimal DatabaseHeader type for C++ wrapper compatibility
package DS

// DatabaseHeader represents the database file header.
// This is a minimal type definition for C++ wrapper compatibility.
// The actual header handling is done in C++.
type DatabaseHeader struct {
	Data [100]byte
}

// NewDatabaseHeader creates a new database header.
func NewDatabaseHeader(pageSize uint16) *DatabaseHeader {
	h := &DatabaseHeader{}
	h.Data[0] = byte(pageSize >> 8)
	h.Data[1] = byte(pageSize)
	return h
}
